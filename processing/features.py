from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from math import asin, cos, radians, sin, sqrt
from typing import Dict, Iterable, List, Optional

import pandas as pd

from .coordinates import CAPITAL_COORDINATES, TEAM_COORDINATES


def normalize_team_name(name: str) -> str:
    return (
        name.replace("-", " ")
        .replace(".", "")
        .replace("FC", "")
        .replace("F.C.", "")
        .strip()
        .lower()
    )


def haversine_distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two points on Earth."""
    r = 6371.0  # kilometers
    d_lat = radians(lat2 - lat1)
    d_lon = radians(lon2 - lon1)
    lat1_rad = radians(lat1)
    lat2_rad = radians(lat2)

    a = sin(d_lat / 2) ** 2 + cos(lat1_rad) * cos(lat2_rad) * sin(d_lon / 2) ** 2
    c = 2 * asin(sqrt(a))
    return round(r * c, 2)


def _resolve_team_coords(
    team: str, teams_info: Optional[pd.DataFrame]
) -> Optional[tuple[float, float]]:
    if teams_info is not None and not teams_info.empty:
        row = teams_info.loc[teams_info["nome_time"].str.lower() == team.lower()]
        if not row.empty:
            return float(row.iloc[0]["latitude"]), float(row.iloc[0]["longitude"])

    if team in TEAM_COORDINATES:
        coords = TEAM_COORDINATES[team]
        return coords["lat"], coords["lon"]

    return None


def _resolve_city_coords(city: str, capital_hint: Optional[str]) -> Optional[tuple[float, float]]:
    if not capital_hint:
        return None
    state = capital_hint.upper()
    if state in CAPITAL_COORDINATES:
        coords = CAPITAL_COORDINATES[state]
        return coords["lat"], coords["lon"]
    return None


def compute_travel_distance(
    matches_df: pd.DataFrame,
    teams_info_df: Optional[pd.DataFrame] = None,
    city_state_map: Optional[Dict[str, str]] = None,
) -> pd.DataFrame:
    """
    Add travel distance columns (km) for home and away teams.

    - Uses team coordinates from teams_info_df, otherwise TEAM_COORDINATES.
    - Uses CAPITAL_COORDINATES as a fallback for the match city if no direct coordinate is known.
    - `cidade_jogo` may be in the form "Cidade-XX" to hint the state; city_state_map can override.
    """
    df = matches_df.copy()
    df["travel_km_home"] = None
    df["travel_km_away"] = None

    for idx, row in df.iterrows():
        city_field = str(row.get("cidade_jogo") or "")
        city_hint, state_hint = None, None
        if "-" in city_field:
            parts = city_field.rsplit("-", 1)
            city_hint = parts[0].strip()
            state_hint = parts[1].strip()
        elif city_state_map and city_field in city_state_map:
            state_hint = city_state_map[city_field]

        match_coords = _resolve_city_coords(city_hint or city_field, state_hint)

        for col, team in (("travel_km_home", row["time_casa"]), ("travel_km_away", row["time_fora"])):
            base_coords = _resolve_team_coords(team, teams_info_df)
            if not base_coords or not match_coords:
                df.at[idx, col] = None
                continue
            if city_hint and normalize_team_name(team) in normalize_team_name(city_hint):
                df.at[idx, col] = 0.0
                continue
            df.at[idx, col] = haversine_distance_km(
                base_coords[0], base_coords[1], match_coords[0], match_coords[1]
            )

    return df


def compute_rest_days(matches_df: pd.DataFrame) -> pd.DataFrame:
    """Compute rest days between matches for home and away teams."""
    df = matches_df.copy()
    df["data"] = pd.to_datetime(df["data"])
    df.sort_values("data", inplace=True)

    df["rest_days_home"] = None
    df["rest_days_away"] = None
    last_played: dict[str, pd.Timestamp] = {}

    for idx, row in df.iterrows():
        match_date = row["data"]
        home = row["time_casa"]
        away = row["time_fora"]

        df.at[idx, "rest_days_home"] = (
            (match_date - last_played[home]).days if home in last_played else None
        )
        df.at[idx, "rest_days_away"] = (
            (match_date - last_played[away]).days if away in last_played else None
        )

        last_played[home] = match_date
        last_played[away] = match_date

    return df


def _rolling_mean_shifted(series: pd.Series, window: int) -> pd.Series:
    return series.shift(1).rolling(window=window, min_periods=1).mean()


def compute_rolling_stats(matches_df: pd.DataFrame, window: int = 5) -> pd.DataFrame:
    """
    Compute rolling averages (last N matches) for goals and xG.
    Adds columns: gf_lastN_home, ga_lastN_home, xg_lastN_home, etc.
    """
    df = matches_df.copy()
    df["data"] = pd.to_datetime(df["data"])

    long_rows: List[dict] = []
    for _, row in df.iterrows():
        long_rows.append(
            {
                "id_partida": row["id_partida"],
                "team": row["time_casa"],
                "goals_for": row.get("gols_casa"),
                "goals_against": row.get("gols_fora"),
                "xg_for": row.get("xG_casa"),
                "xg_against": row.get("xG_fora"),
                "data": row["data"],
                "side": "home",
            }
        )
        long_rows.append(
            {
                "id_partida": row["id_partida"],
                "team": row["time_fora"],
                "goals_for": row.get("gols_fora"),
                "goals_against": row.get("gols_casa"),
                "xg_for": row.get("xG_fora"),
                "xg_against": row.get("xG_casa"),
                "data": row["data"],
                "side": "away",
            }
        )

    long_df = pd.DataFrame(long_rows).sort_values(["team", "data"])

    for metric in ["goals_for", "goals_against", "xg_for", "xg_against"]:
        long_df[f"{metric}_roll"] = (
            long_df.groupby("team")[metric].transform(lambda s: _rolling_mean_shifted(s, window))
        )

    home_stats = (
        long_df[long_df["side"] == "home"]
        .set_index("id_partida")[
            ["goals_for_roll", "goals_against_roll", "xg_for_roll", "xg_against_roll"]
        ]
        .add_suffix(f"_last{window}_home")
    )
    away_stats = (
        long_df[long_df["side"] == "away"]
        .set_index("id_partida")[
            ["goals_for_roll", "goals_against_roll", "xg_for_roll", "xg_against_roll"]
        ]
        .add_suffix(f"_last{window}_away")
    )

    merged = (
        df.set_index("id_partida")
        .join(home_stats)
        .join(away_stats)
        .reset_index()
    )
    return merged


def compute_key_players(
    matches_df: pd.DataFrame,
    player_stats_df: pd.DataFrame,
    window: int = 5,
    top_n: int = 3,
) -> pd.DataFrame:
    """
    Compute key players based on goals + assists in last N matches.
    Requires player_stats_df with columns: id_partida, id_jogador, time, gols, assistencias.
    """
    df = matches_df.copy()
    df["data"] = pd.to_datetime(df["data"])
    player_stats_df = player_stats_df.copy()
    player_stats_df["time"] = player_stats_df["time"].astype(str)

    match_date_map = df.set_index("id_partida")["data"].to_dict()
    team_matches: Dict[str, List[int]] = defaultdict(list)

    for _, row in df.sort_values("data").iterrows():
        team_matches[row["time_casa"]].append(row["id_partida"])
        team_matches[row["time_fora"]].append(row["id_partida"])

    key_home, key_away = {}, {}

    for _, match in df.iterrows():
        match_id = match["id_partida"]
        match_date = match_date_map[match_id]
        for side, team, target in (
            ("home", match["time_casa"], key_home),
            ("away", match["time_fora"], key_away),
        ):
            history_ids = [
                mid
                for mid in team_matches.get(team, [])
                if match_date_map[mid] < match_date
            ]
            recent_ids = history_ids[-window:]
            mask = (player_stats_df["time"] == str(team)) & (
                player_stats_df["id_partida"].isin(recent_ids)
            )
            recent = player_stats_df.loc[mask]
            if recent.empty:
                target[match_id] = []
                continue
            grouped = (
                recent.groupby("id_jogador")[["gols", "assistencias"]]
                .sum()
                .reset_index()
            )
            grouped["score"] = grouped["gols"] + grouped["assistencias"]
            top_players = grouped.sort_values(
                ["score", "gols"], ascending=False
            ).head(top_n)
            target[match_id] = [
                {
                    "id_jogador": int(row["id_jogador"]),
                    "gols": int(row["gols"]),
                    "assistencias": int(row["assistencias"]),
                    "score": int(row["score"]),
                }
                for _, row in top_players.iterrows()
            ]

    df["key_players_home"] = df["id_partida"].map(key_home)
    df["key_players_away"] = df["id_partida"].map(key_away)
    return df


@dataclass
class ImportanceParams:
    high_round_threshold: int = 28
    g4_cutoff: int = 4
    z4_cutoff: int = 17


def compute_importance_score(
    matches_df: pd.DataFrame,
    standings_df: Optional[pd.DataFrame],
    params: ImportanceParams = ImportanceParams(),
) -> pd.DataFrame:
    """
    Calculate importance based on round and proximity to G4/Z4.
    standings_df must contain columns: rodada, time, posicao, pontos.
    """
    df = matches_df.copy()
    if standings_df is None or standings_df.empty:
        df["importance_home"] = None
        df["importance_away"] = None
        return df

    standings_df = standings_df.copy()
    standings_df["rodada"] = standings_df["rodada"].astype(int)

    def team_score(team: str, rodada: Optional[int | str]) -> Optional[float]:
        if rodada is None:
            return None
        if isinstance(rodada, str):
            digits = "".join(ch for ch in rodada if ch.isdigit())
            rodada = int(digits) if digits else None
        table = standings_df[standings_df["rodada"] == rodada]
        if table.empty:
            return None
        team_row = table.loc[table["time"].str.lower() == team.lower()]
        if team_row.empty:
            return None
        pontos = float(team_row.iloc[0]["pontos"])
        g4_row = table.loc[table["posicao"] == params.g4_cutoff]
        z4_row = table.loc[table["posicao"] == params.z4_cutoff]
        if g4_row.empty or z4_row.empty:
            return None
        gap_g4 = float(g4_row.iloc[0]["pontos"]) - pontos
        gap_z4 = pontos - float(z4_row.iloc[0]["pontos"])
        tension = 1 / (1 + max(gap_g4, 0) + max(gap_z4, 0))
        stage_factor = 1.0 if rodada >= params.high_round_threshold else 0.5
        return round(tension * stage_factor, 3)

    df["importance_home"] = df.apply(
        lambda row: team_score(row["time_casa"], row.get("rodada")), axis=1
    )
    df["importance_away"] = df.apply(
        lambda row: team_score(row["time_fora"], row.get("rodada")), axis=1
    )
    return df
