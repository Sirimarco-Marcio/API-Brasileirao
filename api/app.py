import os
from flask import Flask, jsonify, request
from dotenv import load_dotenv

from data_acquisition.db import get_connection, init_db


def create_app() -> Flask:
    """Application factory for the Flask API."""
    load_dotenv()
    app = Flask(__name__)
    init_db()

    @app.route("/health", methods=["GET"])
    def health() -> tuple[dict, int]:
        return jsonify({"status": "ok"}), 200

    @app.route("/harvest", methods=["POST"])
    def harvest_endpoint() -> tuple[dict, int]:
        """Trigger the harvester for a season interval."""
        from data_acquisition.harvester import harvest_seasons

        payload = request.get_json(silent=True) or {}
        # Allow query params as a fallback to make manual testing easier.
        args = request.args
        start_season = int(payload.get("start_season") or args.get("start_season", 2018))
        end_season = int(payload.get("end_season") or args.get("end_season", start_season))
        include_player_stats = (
            bool(payload.get("include_player_stats"))
            if "include_player_stats" in payload
            else args.get("include_player_stats", "true").lower() == "true"
        )

        summary = harvest_seasons(
            start_season=start_season,
            end_season=end_season,
            include_player_stats=include_player_stats,
        )
        return jsonify(summary), 200

    @app.route("/data/matches", methods=["GET"])
    def list_matches() -> tuple[dict, int]:
        """Return a small sample of matches for quick inspection."""
        limit = int(request.args.get("limit", 20))
        season = request.args.get("season")
        query = "SELECT * FROM matches"
        params: list = []
        if season:
            query += " WHERE temporada = ?"
            params.append(season)
        query += " ORDER BY data DESC LIMIT ?"
        params.append(limit)
        with get_connection() as conn:
            rows = conn.execute(query, params).fetchall()
        data = [dict(r) for r in rows]
        return jsonify({"count": len(data), "matches": data}), 200

    @app.route("/", methods=["GET"])
    def index() -> tuple[str, int]:
        """Simple UI to exercise the API."""
        return (
            """
            <!doctype html>
            <html lang="pt-br">
            <head>
              <meta charset="UTF-8">
              <title>API-Brasileirão Dashboard</title>
              <style>
                body { font-family: Arial, sans-serif; margin: 24px; background: #f7f7f7; color: #222; }
                h1 { margin-bottom: 0.5rem; }
                .card { background: white; padding: 16px; margin-bottom: 16px; border-radius: 8px; box-shadow: 0 1px 4px rgba(0,0,0,0.1); }
                label { display: block; margin-top: 8px; }
                input, select { padding: 6px; margin-top: 4px; }
                button { padding: 8px 12px; margin-top: 12px; cursor: pointer; }
                pre { background: #111; color: #0f0; padding: 12px; border-radius: 6px; max-height: 320px; overflow: auto; }
              </style>
            </head>
            <body>
              <h1>API-Brasileirão</h1>

              <div class="card">
                <h2>Testar API</h2>
                <label>Método
                  <select id="method">
                    <option value="GET">GET</option>
                    <option value="POST">POST</option>
                  </select>
                </label>
                <label>Endpoint
                  <select id="endpoint">
                    <option value="/health">/health (GET)</option>
                    <option value="/harvest">/harvest (POST)</option>
                  </select>
                </label>
                <div id="post-params" style="display:none;">
                  <label>start_season <input id="start_season" type="number" value="2018"></label>
                  <label>end_season <input id="end_season" type="number" value="2018"></label>
                  <label>include_player_stats
                    <select id="include_player_stats">
                      <option value="true">true</option>
                      <option value="false">false</option>
                    </select>
                  </label>
                </div>
                <button onclick="runRequest()">Enviar</button>
                <pre id="response-box">{}</pre>
              </div>

              <div class="card">
                <h2>Dados carregados</h2>
                <label>Temporada (opcional) <input id="season_filter" type="number" placeholder="ex: 2023"></label>
                <label>Limite <input id="limit_filter" type="number" value="20"></label>
                <button onclick="loadMatches()">Listar partidas</button>
                <pre id="data-box">{}</pre>
              </div>

              <script>
                const methodSel = document.getElementById('method');
                const endpointSel = document.getElementById('endpoint');
                const postParams = document.getElementById('post-params');
                methodSel.addEventListener('change', () => {
                  postParams.style.display = methodSel.value === 'POST' ? 'block' : 'none';
                });
                endpointSel.addEventListener('change', () => {
                  if (endpointSel.value === '/harvest') {
                    methodSel.value = 'POST';
                    postParams.style.display = 'block';
                  }
                });

                async function runRequest() {
                  const method = methodSel.value;
                  const endpoint = endpointSel.value;
                  let options = { method };
                  if (method === 'POST') {
                    options.headers = { 'Content-Type': 'application/json' };
                    options.body = JSON.stringify({
                      start_season: Number(document.getElementById('start_season').value),
                      end_season: Number(document.getElementById('end_season').value),
                      include_player_stats: document.getElementById('include_player_stats').value === 'true'
                    });
                  }
                  try {
                    const res = await fetch(endpoint, options);
                    const text = await res.text();
                    document.getElementById('response-box').textContent = text;
                  } catch (err) {
                    document.getElementById('response-box').textContent = err.toString();
                  }
                }

                async function loadMatches() {
                  const season = document.getElementById('season_filter').value;
                  const limit = document.getElementById('limit_filter').value || 20;
                  const params = new URLSearchParams({ limit });
                  if (season) params.append('season', season);
                  try {
                    const res = await fetch(`/data/matches?${params.toString()}`);
                    const text = await res.text();
                    document.getElementById('data-box').textContent = text;
                  } catch (err) {
                    document.getElementById('data-box').textContent = err.toString();
                  }
                }
              </script>
            </body>
            </html>
            """,
            200,
        )

    return app


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    create_app().run(host="0.0.0.0", port=port)
