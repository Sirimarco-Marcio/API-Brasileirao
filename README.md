# API-Brasileirao – versão Flask

Infra de coleta e preparação de dados para previsões do Brasileirão, Copa do Brasil e Libertadores usando Flask, SQLite e API-Football (RapidAPI).

## Estrutura
- `api/`: app Flask (`create_app`) com endpoints `/health` e `/harvest`.
- `data_acquisition/`: schema SQLite (`futebol_data.db`), conexões e coletor `harvester.py`.
- `processing/`: engenharia de features (descanso, distância, rolling stats, key players, importância) e dicionários de coordenadas.
- `requirements.txt`: dependências Python.

## Setup rápido
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export RAPIDAPI_KEY="sua_chave"
# opcional: export LEAGUE_ID_SERIE_A, LEAGUE_ID_CDB, LEAGUE_ID_LIBERTA
python -m data_acquisition.db  # cria futebol_data.db e tabelas
python -m api.app  # sobe Flask em 0.0.0.0:5000
```

## Coleta (Plano Caracol)
- `python -m data_acquisition.harvester` coleta de 2018 a 2025 respeitando limite de 100 requisições/dia (log em `data_acquisition/.request_quota.json`).
- Filtragem:
  - Série A: todos os jogos.
  - Copa do Brasil: só a partir da 3ª fase e apenas se houver time da Série A.
  - Libertadores: só jogos com times brasileiros da Série A.
- Persistência evita duplicar `matches` e `player_stats`.
- Endpoint `/harvest` aceita `start_season`, `end_season`, `include_player_stats`.

## Features
Carregue dados em pandas e aplique:
```python
import pandas as pd
import sqlite3
from processing import (
    compute_importance_score,
    compute_key_players,
    compute_rest_days,
    compute_rolling_stats,
    compute_travel_distance,
)

conn = sqlite3.connect("futebol_data.db")
matches = pd.read_sql("SELECT * FROM matches", conn)
players = pd.read_sql("SELECT * FROM player_stats", conn)
matches = compute_rest_days(matches)
matches = compute_travel_distance(matches)
matches = compute_rolling_stats(matches)
matches = compute_key_players(matches, players)
# importance: fornecer standings_df com colunas rodada/time/posicao/pontos
```

## Notas
- Banco padrão: `futebol_data.db` na raiz. Esquema em `data_acquisition/db.py`.
- Coordenadas dos clubes e capitais estão em `processing/coordinates.py`.
- Commit sugeridos: `feat: setup python flask and sqlite schema`, `feat: implement snail-paced data harvester`, `feat: implement feature engineering logic (rest days, travel)`.
