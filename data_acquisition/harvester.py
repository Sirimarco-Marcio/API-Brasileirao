from __future__ import annotations

import json
import os
import unicodedata
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import requests
from dotenv import load_dotenv

from data_acquisition.db import (
    get_connection,
    init_db,
    insert_matches,
    insert_player_stats,
    match_exists,
    player_stats_exists,
    seed_known_teams,
    upsert_team_info,
)
from processing.coordinates import TEAM_COORDINATES, TEAM_CITIES

BASE_URL = "https://v3.football.api-sports.io"


def _normalize(team_name: str) -> str:
    if not team_name:
        return ""
    normalized = unicodedata.normalize("NFKD", team_name)
    return (
        "".join(ch for ch in normalized if not unicodedata.combining(ch))
        .lower()
        .replace("-", "")
        .replace(" ", "")
    )


SERIE_A_TEAM_NAMES = {_normalize(name) for name in TEAM_COORDINATES.keys()}
TEAM_ALIASES = {
    "atleticopr": "athletico pr",
    "atleticoparanaense": "athletico pr",
    "atleticomg": "atletico mg",
    "americamg": "america mg",
    "gremio": "gremio",
    "internacional": "internacional",
    "bragantino": "red bull bragantino",
}


def is_serie_a_team(team_name: str) -> bool:
    normalized = _normalize(team_name)
    if normalized in SERIE_A_TEAM_NAMES:
        return True
    alias = TEAM_ALIASES.get(normalized)
    if alias and _normalize(alias) in SERIE_A_TEAM_NAMES:
        return True
    return False


@dataclass
class RequestQuota:
    log_path: Path
    daily_limit: int = 100

    def __post_init__(self) -> None:
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self._state = {"date": date.today().isoformat(), "count": 0}
        self._load()

    def _load(self) -> None:
        if self.log_path.exists():
            try:
                data = json.loads(self.log_path.read_text())
                if data.get("date") == date.today().isoformat():
                    self._state = data
                else:
                    self._state = {"date": date.today().isoformat(), "count": 0}
            except json.JSONDecodeError:
                self._state = {"date": date.today().isoformat(), "count": 0}
        self._save()

    def _save(self) -> None:
        self.log_path.write_text(json.dumps(self._state))

    @property
    def remaining(self) -> int:
        return max(self.daily_limit - int(self._state.get("count", 0)), 0)

    @property
    def used_today(self) -> int:
        return int(self._state.get("count", 0))

    def consume(self, amount: int = 1) -> bool:
        if self.remaining < amount:
            return False
        self._state["count"] = int(self._state.get("count", 0)) + amount
        self._save()
        return True


class APIFootballClient:
    def __init__(self, api_key: str, quota: RequestQuota):
        self.session = requests.Session()
        self.session.headers.update(
            {
                "x-rapidapi-key": api_key,
                "x-rapidapi-host": "v3.football.api-sports.io",
            }
        )
        self.quota = quota

    def get(self, path: str, params: Optional[dict] = None) -> dict:
        if not self.quota.consume():
            raise RuntimeError("Request limit reached for today")
        response = self.session.get(f"{BASE_URL}{path}", params=params or {}, timeout=30)
        response.raise_for_status()
        payload = response.json()
        return payload.get("response", payload)


LEAGUE_IDS = {
    "SERIE_A": int(os.getenv("LEAGUE_ID_SERIE_A", "71")),
    "COPA_DO_BRASIL": int(os.getenv("LEAGUE_ID_CDB", "75")),
    "LIBERTADORES": int(os.getenv("LEAGUE_ID_LIBERTA", "13")),
}

CUP_PHASE_ORDER = [
    "1st",
    "2nd",
    "3rd",
    "Round of 32",
    "Round of 16",
    "Quarter",
    "Semi",
    "Final",
]


def allowed_copa_round(round_str: str) -> bool:
    text = round_str.lower()
    for idx, label in enumerate(CUP_PHASE_ORDER):
        if label.lower() in text:
            return idx >= 2
    return False


def allowed_competition(campeonato: str, fixture: dict) -> bool:
    round_str = fixture.get("league", {}).get("round", "") or ""
    home_name = fixture.get("teams", {}).get("home", {}).get("name", "")
    away_name = fixture.get("teams", {}).get("away", {}).get("name", "")

    if campeonato == "Série A":
        return True
    if campeonato == "Copa do Brasil":
        if not allowed_copa_round(round_str):
            return False
        return is_serie_a_team(home_name) or is_serie_a_team(away_name)
    if campeonato == "Libertadores":
        return is_serie_a_team(home_name) or is_serie_a_team(away_name)
    return False


def fixture_to_row(fixture: dict, campeonato: str) -> tuple:
    fixture_info = fixture.get("fixture", {})
    league_info = fixture.get("league", {})
    teams_info = fixture.get("teams", {})
    goals_info = fixture.get("goals", {})

    return (
        int(fixture_info.get("id")),
        fixture_info.get("date"),
        campeonato,
        league_info.get("season"),
        league_info.get("round"),
        teams_info.get("home", {}).get("name"),
        teams_info.get("away", {}).get("name"),
        goals_info.get("home"),
        goals_info.get("away"),
        teams_info.get("home", {}).get("statistics", {}).get("xG")
        if isinstance(teams_info.get("home", {}).get("statistics"), dict)
        else None,
        teams_info.get("away", {}).get("statistics", {}).get("xG")
        if isinstance(teams_info.get("away", {}).get("statistics"), dict)
        else None,
        fixture_info.get("venue", {}).get("city"),
    )


def fetch_player_stats(client: APIFootballClient, fixture_id: int) -> List[tuple]:
    """
    Pull player stats for a fixture. API response shape:
    response: [{team: {...}, players: [{player: {...}, statistics: [{goals: {...}, passes: {...}}]}]}]
    """
    payload = client.get("/fixtures/players", params={"fixture": fixture_id})
    rows: List[tuple] = []
    for team_block in payload:
        team_name = team_block.get("team", {}).get("name")
        for player in team_block.get("players", []):
            player_id = player.get("player", {}).get("id")
            stats = (player.get("statistics") or [{}])[0]
            goals = stats.get("goals", {}) or {}
            passes = stats.get("passes", {}) or {}
            rows.append(
                (
                    fixture_id,
                    player_id,
                    team_name,
                    goals.get("total") or 0,
                    passes.get("goal_assist") or 0,
                )
            )
    return rows


def fetch_fixtures(client: APIFootballClient, league_id: int, season: int) -> list:
    params = {"league": league_id, "season": season}
    return client.get("/fixtures", params=params)


def harvest_single_season(
    client: APIFootballClient,
    conn,
    season: int,
    include_player_stats: bool = False,
) -> Dict[str, int]:
    inserted_matches = 0
    skipped_existing = 0
    saved_player_stats = 0
    league_breakdown: List[dict] = []

    competitions = [
        ("Série A", LEAGUE_IDS["SERIE_A"]),
        ("Copa do Brasil", LEAGUE_IDS["COPA_DO_BRASIL"]),
        ("Libertadores", LEAGUE_IDS["LIBERTADORES"]),
    ]

    for campeonato, league_id in competitions:
        fixtures = fetch_fixtures(client, league_id, season)
        total_fetched = len(fixtures)
        kept_after_filter = 0
        pending_rows: List[tuple] = []

        for fixture in fixtures:
            fixture_id = int(fixture.get("fixture", {}).get("id"))
            if not allowed_competition(campeonato, fixture):
                continue
            exists = match_exists(conn, fixture_id)
            stats_needed = include_player_stats and not player_stats_exists(
                conn, fixture_id
            )
            if exists and not stats_needed:
                skipped_existing += 1
                continue

            # Upsert known team info based on dictionary (city + coords)
            for side in ("home", "away"):
                team_name = fixture.get("teams", {}).get(side, {}).get("name")
                if team_name and team_name in TEAM_COORDINATES:
                    coords = TEAM_COORDINATES[team_name]
                    city = TEAM_CITIES.get(team_name)
                    upsert_team_info(
                        conn,
                        nome_time=team_name,
                        cidade_sede=city,
                        latitude=coords["lat"],
                        longitude=coords["lon"],
                    )

            kept_after_filter += 1
            if not exists:
                pending_rows.append(fixture_to_row(fixture, campeonato))

            if stats_needed and client.quota.remaining > 0:
                try:
                    stats_rows = fetch_player_stats(client, fixture_id)
                    if stats_rows:
                        insert_player_stats(conn, stats_rows)
                        saved_player_stats += len(stats_rows)
                except RuntimeError:
                    break
                except requests.HTTPError:
                    continue

        if pending_rows:
            insert_matches(conn, pending_rows)
            inserted_matches += len(pending_rows)
        league_breakdown.append(
            {
                "campeonato": campeonato,
                "league_id": league_id,
                "season": season,
                "fetched": total_fetched,
                "kept_after_filter": kept_after_filter,
                "inserted": len(pending_rows),
            }
        )

    return {
        "season": season,
        "inserted_matches": inserted_matches,
        "skipped_existing": skipped_existing,
        "saved_player_stats": saved_player_stats,
        "leagues": league_breakdown,
    }


def harvest_seasons(
    start_season: int = 2018,
    end_season: int = 2025,
    include_player_stats: bool = False,
) -> Dict[str, object]:
    load_dotenv()
    api_key = os.getenv("RAPIDAPI_KEY")
    if not api_key:
        raise RuntimeError("RAPIDAPI_KEY is not set in the environment")

    quota = RequestQuota(Path(__file__).resolve().parent / ".request_quota.json")
    client = APIFootballClient(api_key, quota)
    init_db()

    season_summaries = []
    with get_connection() as conn:
        # Seed known teams into teams_info for distance calculations.
        known = {
            name: (TEAM_CITIES.get(name), coords["lat"], coords["lon"])
            for name, coords in TEAM_COORDINATES.items()
        }
        seed_known_teams(conn, known)
        for season in range(start_season, end_season + 1):
            try:
                summary = harvest_single_season(
                    client, conn, season, include_player_stats=include_player_stats
                )
                season_summaries.append(summary)
            except RuntimeError as exc:
                break

    return {
        "seasons": season_summaries,
        "requests_used": quota.used_today,
        "requests_remaining": quota.remaining,
    }


if __name__ == "__main__":
    report = harvest_seasons()
    print(json.dumps(report, indent=2))
