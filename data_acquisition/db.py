from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable, Optional, Dict, Tuple

DB_PATH = Path(__file__).resolve().parents[1] / "futebol_data.db"


def get_connection() -> sqlite3.Connection:
    """Return a SQLite connection with row factory configured."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    """Create tables if they do not exist."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.executescript(
            """
            CREATE TABLE IF NOT EXISTS matches (
                id_partida INTEGER PRIMARY KEY,
                data TEXT NOT NULL,
                campeonato TEXT NOT NULL,
                temporada INTEGER,
                rodada TEXT,
                time_casa TEXT NOT NULL,
                time_fora TEXT NOT NULL,
                gols_casa INTEGER,
                gols_fora INTEGER,
                xG_casa REAL,
                xG_fora REAL,
                cidade_jogo TEXT
            );

            CREATE TABLE IF NOT EXISTS player_stats (
                id_partida INTEGER NOT NULL,
                id_jogador INTEGER NOT NULL,
                time TEXT,
                gols INTEGER DEFAULT 0,
                assistencias INTEGER DEFAULT 0,
                PRIMARY KEY (id_partida, id_jogador)
            );

            CREATE TABLE IF NOT EXISTS teams_info (
                nome_time TEXT PRIMARY KEY,
                cidade_sede TEXT,
                latitude REAL,
                longitude REAL
            );
            """
        )
        conn.commit()


def match_exists(conn: sqlite3.Connection, fixture_id: int) -> bool:
    cur = conn.execute(
        "SELECT 1 FROM matches WHERE id_partida = ? LIMIT 1", (fixture_id,)
    )
    return cur.fetchone() is not None


def player_stats_exists(conn: sqlite3.Connection, fixture_id: int) -> bool:
    cur = conn.execute(
        "SELECT 1 FROM player_stats WHERE id_partida = ? LIMIT 1", (fixture_id,)
    )
    return cur.fetchone() is not None


def insert_matches(conn: sqlite3.Connection, rows: Iterable[tuple]) -> None:
    conn.executemany(
        """
        INSERT OR IGNORE INTO matches (
            id_partida, data, campeonato, temporada, rodada,
            time_casa, time_fora, gols_casa, gols_fora, xG_casa, xG_fora, cidade_jogo
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()


def insert_player_stats(
    conn: sqlite3.Connection, rows: Iterable[tuple]
) -> None:
    conn.executemany(
        """
        INSERT OR REPLACE INTO player_stats (
            id_partida, id_jogador, time, gols, assistencias
        ) VALUES (?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()


def upsert_team_info(
    conn: sqlite3.Connection,
    nome_time: str,
    cidade_sede: Optional[str],
    latitude: Optional[float],
    longitude: Optional[float],
) -> None:
    conn.execute(
        """
        INSERT INTO teams_info (nome_time, cidade_sede, latitude, longitude)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(nome_time) DO UPDATE SET
            cidade_sede=excluded.cidade_sede,
            latitude=excluded.latitude,
            longitude=excluded.longitude
        """,
        (nome_time, cidade_sede, latitude, longitude),
    )
    conn.commit()


def seed_known_teams(
    conn: sqlite3.Connection, teams: Dict[str, Tuple[str, float, float]]
) -> None:
    """Prefill teams_info using provided mapping: name -> (city, lat, lon)."""
    rows = [
        (name, meta[0], meta[1], meta[2])
        for name, meta in teams.items()
    ]
    conn.executemany(
        """
        INSERT INTO teams_info (nome_time, cidade_sede, latitude, longitude)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(nome_time) DO UPDATE SET
            cidade_sede=excluded.cidade_sede,
            latitude=excluded.latitude,
            longitude=excluded.longitude
        """,
        rows,
    )
    conn.commit()


if __name__ == "__main__":
    init_db()
    print(f"SQLite schema initialized at {DB_PATH}")
