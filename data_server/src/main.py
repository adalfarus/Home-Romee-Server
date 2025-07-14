"""TBA"""
from flask import Flask, jsonify, g, Response, request, make_response
import requests
import sqlite3
import json
import os

import typing as _ty

DB_NAME = "data.db"
app = Flask(__name__)

def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DB_NAME)
        g.db.row_factory = sqlite3.Row
    return g.db

@app.teardown_appcontext
def close_db(error):
    db = g.pop("db", None)
    if db: db.close()

def create_db(players: list[tuple[str, str]] | None = None, games: list[tuple[int | None, ...]] | None = None, hand_scores: dict[int, int] | None = None):
    good_players: list[tuple[str, str]] = players or [("Alice", "player1"), ("Bob", "player2"), ("Cara", "player3")]
    good_games: list[tuple[int | None, ...]] = games or [
            (10, 0, 15),    # session 1
            (0, 8, 7),
            (1, 1, 1),      # session end
            (0, 1, 22),     # session 2
            (1, 1, 1),      # session end
        ]
    good_hands: dict[int, int] = hand_scores or {}

    # Transmigrate data: 1 -> NULL
    good_games = [tuple(x if x != 1 else None for x in row) for row in good_games]

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()

    c.execute("DROP TABLE IF EXISTS scores")
    c.execute("DROP TABLE IF EXISTS players")
    c.execute("DROP TABLE IF EXISTS hands")
    c.execute("PRAGMA foreign_keys = ON;")

    c.execute("""
        CREATE TABLE players (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            colname TEXT UNIQUE NOT NULL
        )
    """)
    c.executemany("INSERT INTO players (name, colname) VALUES (?, ?)", good_players)

    columns = ", ".join(f"{col} INTEGER DEFAULT NULL" for _, col in good_players)
    c.execute(f"CREATE TABLE scores (id INTEGER PRIMARY KEY AUTOINCREMENT, {columns})")
    c.execute("CREATE TABLE hands (scores_id INTEGER REFERENCES scores(id), flag INTEGER NOT NULL)")

    placeholders = ", ".join(["?"] * len(good_players))
    c.executemany(f"INSERT INTO scores ({', '.join([col for _, col in good_players])}) VALUES ({placeholders})", good_games)

    c.executemany("INSERT INTO hands (scores_id, flag) VALUES (?, ?)", tuple((k, v) for k, v in good_hands.items() if v != 0))
    conn.commit()
    conn.close()

@app.route("/get_data")
def get_data() -> Response:
    db = get_db()
    cursor = db.cursor()  # Get all table names
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
    tables: list[str] = [row["name"] for row in cursor.fetchall()]
    db_dict: dict[str, list[dict[str, _ty.Any]]] = {}  # Read each table into a dictionary
    for table in tables:
        cursor.execute(f"SELECT * FROM {table};")
        rows = cursor.fetchall()
        db_dict[table] = [dict(row) for row in rows]
    resp = jsonify(db_dict)
    resp.status_code = 200
    return resp


def query_ollama(model: str, prompt: str, stream: bool = False) -> str:
    """
    Query a local Ollama model with a given prompt.

    Parameters:
        model (str): Name of the model (e.g., 'llama3', 'mistral', etc.)
        prompt (str): The prompt to send.
        stream (bool): Whether to stream the response (optional).

    Returns:
        str: The model's response.
    """
    url = "http://localhost:11434/api/generate"
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": stream
    }
    print(payload)

    try:
        response = requests.post(url, json=payload, stream=stream)
        response.raise_for_status()

        if stream:
            # Streamed output (generates chunks)
            output = ""
            for line in response.iter_lines():
                if line:
                    chunk = line.decode("utf-8")
                    # The response comes as JSON lines
                    data = json.loads(chunk)
                    output += data.get("response", "")
            return output
        else:
            # Non-streamed full response
            return response.json().get("response", "")
    except requests.exceptions.HTTPError as e:
        print("Full response:", e.response.text)
    except requests.exceptions.RequestException as e:
        print(e.response.text)
        return f"Error contacting Ollama: {e}"

# [(1, 0), (2, 0), (3, 0), (4, 0), (5, 0), (6, 0), (7, 0), (9, 0), (10, 0), (11, 0), (12, 0), (13, 0), (15, 0), (16, 0), (17, 0), (18, 0), (19, 0), (20, 0), (21, 0), (22, 0), (24, 0), (25, 0), (26, 0), (27, 0), (28, 0), (29, 0), (30, 0), (31, 0), (32, 0), (33, 0), (34, 0), (35, 0), (36, 0), (38, 1), (39, 0), (40, 1), (41, 0), (43, 0), (44, 0), (45, 0), (46, 0), (47, 0), (49, 1), (50, 0), (51, 0), (53, 0), (54, 0), (55, 0), (56, 0), (58, 0), (59, 0), (60, 0), (61, 0), (62, 0), (63, 0), (65, 0), (66, 0), (67, 0), (68, 0), (69, 1), (70, 0), (71, 0), (72, 1), (73, 0), (74, 1)]
@app.route("/player_quote/<string:model>/<string:player_name>", methods=["GET", "OPTIONS"])
def player_quote(model: str, player_name: str) -> Response:
    # For preflight (OPTIONS) requests
    if request.method == "OPTIONS":
        resp = make_response()
        origin = request.headers.get("Origin")
        if origin:
            resp.headers["Access-Control-Allow-Origin"] = origin
            # resp.headers["Access-Control-Allow-Credentials"] = "true"
            resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
            resp.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
        return resp, 204
    db = get_db()
    cursor = db.cursor()
    try:
        cursor.execute("SELECT colname FROM players WHERE name = ?", (player_name,))
        player_column: str = cursor.fetchone()[0]
    except sqlite3.OperationalError:
        resp = jsonify({"error": "Bad playername"})
        resp.status_code = 308
        return resp
    cursor.execute(f"SELECT s.{player_column}, h.flag FROM scores s JOIN hands h ON s.id = h.scores_id WHERE 1 = 1")
    data: tuple[tuple[int, int], ...] = tuple((x[0], x[1]) for x in cursor.fetchall() if x[0] != 1)
    data_str: str = ",".join(f"{x[0]}{'f' if x[1] else ''}" for x in data)

    # Calculate win rate
    total_games = len(data)
    wins = sum(1 for score, _ in data if score == 0)
    win_rate = round((wins / total_games) * 100) if total_games > 0 else 0
    has_flags = any(flag for _, flag in data)  # Detect if any games are flagged

    model_request = (
        f"The player {player_name} (they) has played {total_games} games and won {wins}, which is a win rate of {win_rate}%. "
        "Scores are listed as 'score' or 'scoref' if flagged. Score 0 means a win, 2-8 are basically a win. Higher scores are worse. "
        "Flagged scores are doubled, except if the score is 0 (a flagged win means an especially strong win). "
        "Over 40 is a bad result. A win rate above 20% is considered good. "
        f"{'There are flagged scores in the data.' if has_flags else 'There are no flagged scores.'} "
        "Do not assume any data that is not shown. "
        "Respond with exactly one long, honest sentence summarizing the player's performance. "
        "Do not explain. Do not elaborate. Do not mention flags unless they exist. "
        "Integrate the name of the player in a funny way if possible. This is a card game not Poker data do not mention this fact. "
    )
    model_request += f"Here is the data: {data_str}"
    response: str = query_ollama(model, model_request, False).replace("soccer", "")
    resp = jsonify({"response": response})
    resp.status_code = 200

    origin = request.headers.get("Origin")
    if origin:
        resp.headers["Access-Control-Allow-Origin"] = origin
    #     resp.headers["Access-Control-Allow-Credentials"] = "true"
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
        resp.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"

    return resp

@app.route("/player_info/<string:model>/<string:player_name>/<string:question>", methods=["GET", "OPTIONS"])
def player_info(model: str, player_name: str, question: str) -> Response:
    # For preflight (OPTIONS) requests
    if request.method == "OPTIONS":
        resp = make_response()
        origin = request.headers.get("Origin")
        if origin:
            resp.headers["Access-Control-Allow-Origin"] = origin
            # resp.headers["Access-Control-Allow-Credentials"] = "true"
            resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
            resp.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
        return resp, 204
    db = get_db()
    cursor = db.cursor()
    try:
        cursor.execute("SELECT colname FROM players WHERE name = ?", (player_name,))
        player_column: str = cursor.fetchone()[0]
    except sqlite3.OperationalError:
        resp = jsonify({"error": "Bad playername"})
        resp.status_code = 308
        return resp
    cursor.execute(f"SELECT s.{player_column}, h.flag FROM scores s JOIN hands h ON s.id = h.scores_id WHERE 1 = 1")
    data: tuple[tuple[int, int], ...] = tuple((x[0], x[1]) for x in cursor.fetchall() if x[0] != 1)
    data_str: str = ",".join(f"{x[0]}{'f' if x[1] else ''}" for x in data)
    model_request = ("The data format is: score followed by 'f' if flagged. 0 = win, higher is worse. "
               "Flag doubles the score. Analyze the data and answer the following question. "
               "Respond clearly and concisely.")
    model_request = (
        "You are given a list of player scores. Format: score followed by 'f' if flagged. "
        "Score 0 means a win. Higher scores are worse. "
        "If a score is flagged (has 'f'), it must be doubled — except if it's 0 (then it stays 0, but is a very good win). "
        "You must find the highest final score after applying these rules. "
        "Respond only with the number — do not explain. "
        f"Data: {data_str}\n"
        f"Question: {question}"
    )
    model_request += f"Here is the data: {data_str} "
    model_request += f"Please answer following question: {question}"
    response: str = query_ollama(model, model_request, False)
    resp = jsonify({"response": response})
    resp.status_code = 200

    origin = request.headers.get("Origin")
    if origin:
        resp.headers["Access-Control-Allow-Origin"] = origin
    #     resp.headers["Access-Control-Allow-Credentials"] = "true"
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
        resp.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"

    return resp


if __name__ == "__main__":
    if not os.path.exists(DB_NAME):
        create_db()
    app.run(port=8080, host="0.0.0.0", debug=True)
