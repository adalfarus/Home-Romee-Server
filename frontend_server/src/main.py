"""TBA"""
import requests
from flask import Flask, render_template, request, g, abort, redirect, url_for, jsonify
import sqlite3
import os
from analyze import analyze_stats
from werkzeug.exceptions import HTTPException
import traceback
from dataclasses import dataclass

DB_NAME = "data.db"
app = Flask(__name__)

@dataclass
class Round:
    player_scores: dict[str, int]
    hand: bool
    def __getitem__(self, item: int):
        return list(self.player_scores.values())[item]
    def __hash__(self):
        return hash(f"{self.player_scores}/{self.hand}")
@dataclass
class Session:
    rounds: list[Round]
    def __getitem__(self, item: int):
        return self.rounds[item]
    def __hash__(self):
        return hash(f"{self.rounds}")

SESSIONS: list[Session] = []


def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DB_NAME)
        g.db.row_factory = sqlite3.Row
    return g.db

@app.teardown_appcontext
def close_db(error):
    db = g.pop("db", None)
    if db: db.close()

@app.errorhandler(Exception)
def handle_all_errors(e):
    # If it's an HTTP error (abort(404) etc), show standard error.html
    if isinstance(e, HTTPException):
        return render_template("error.html",
            code=e.code,
            name=e.name,
            description=e.description,
        ), e.code

    # For any other Python/Jinja error, show as 500
    tb = traceback.format_exc()
    return render_template("error.html",
        code=500,
        name="Internal Server Error",
        description="An internal error occurred. Please try again later.",
        details=tb  # Show stacktrace only if DEBUG (remove in prod)
    ), 500

def get_players():
    db = get_db()
    rows = db.execute("SELECT name, colname FROM players ORDER BY id").fetchall()
    return [(row["name"], row["colname"]) for row in rows]

def get_rounds() -> list[Round | None]:
    db = get_db()
    players = get_players()
    colnames = [col for _, col in players]

    query = f"""
        SELECT scores.id, {', '.join(colnames)}, hands.flag
        FROM scores
        LEFT JOIN hands ON scores.id = hands.scores_id
        ORDER BY scores.id
    """
    rows = db.execute(query).fetchall()

    rounds: list[Round | None] = []
    for row in rows:
        raw_scores = {name: row[col] for name, col in players}
        flag = row["flag"]

        # Determine if this is the session separator row (all None's)
        if all(score is None for score in raw_scores.values()):
            # Still append this as a Round (in case needed)
            rounds.append(None)
        else:
            # Double scores if flag is set (but only if not None)
            adjusted_scores = {
                name: 1 if score is None else score * 2 if flag else score
                for name, score in raw_scores.items()
            }
            rounds.append(Round(player_scores=adjusted_scores, hand=bool(flag)))

    return rounds

def parse_sessions(rounds: list[Round | None]) -> list[Session]:
    global SESSIONS
    if SESSIONS:
        return SESSIONS
    sessions: list[Session] = []
    current_session: list[Round] = []

    for round_ in rounds:
        # All player scores are NULL -> delimiter, replaced by None in previous step
        if round_ is None:
            if current_session:
                sessions.append(Session(rounds=current_session))
                current_session = []
        else:
            current_session.append(round_)

    # Final session if not already appended
    if current_session:
        sessions.append(Session(rounds=current_session))
    SESSIONS = sessions
    return sessions

def update_db_from_json(data: dict):
    db = get_db()
    cursor = db.cursor()

    for table, rows in data.items():
        if not rows:
            continue  # Skip empty lists

        # Clean 'scores' table by keeping only 'id' column
        if table == "scores":
            # Step 1: Create temp table with only 'id'
            cursor.execute("DROP TABLE IF EXISTS _scores_temp")
            cursor.execute("CREATE TABLE _scores_temp (id INTEGER PRIMARY KEY AUTOINCREMENT)")

            # Step 2: Copy over existing IDs
            cursor.execute("INSERT INTO _scores_temp (id) SELECT id FROM scores")

            # Step 3: Drop original 'scores' table
            cursor.execute("DROP TABLE scores")

            # Step 4: Recreate 'scores' table from temp
            cursor.execute("CREATE TABLE scores (id INTEGER PRIMARY KEY AUTOINCREMENT)")
            cursor.execute("INSERT INTO scores (id) SELECT id FROM _scores_temp")
            cursor.execute("DROP TABLE _scores_temp")

        # Get current column names in the table
        cursor.execute(f"PRAGMA table_info({table})")
        existing_columns = {row["name"] for row in cursor.fetchall()}

        # Check all rows for new columns (assumes all rows are dicts with same keys)
        all_columns_in_data = set()
        for row in rows:
            all_columns_in_data.update(row.keys())

        missing_columns = all_columns_in_data - existing_columns

        # Automatically add missing columns as INTEGER
        if table == "scores":  # Only do this for scores table as per your request
            for col in missing_columns:
                print(f"Adding missing column to '{table}': {col}")
                try:
                    cursor.execute(f"ALTER TABLE {table} ADD COLUMN {col} INTEGER")
                    existing_columns.add(col)
                except sqlite3.OperationalError as e:
                    print(f"Failed to add column '{col}': {e}")

        # Clear the table
        cursor.execute(f"DELETE FROM {table}")

        # Prepare and insert filtered rows
        for row in rows:
            filtered_row = {k: v for k, v in row.items() if k in existing_columns}
            if not filtered_row:
                continue

            columns = filtered_row.keys()
            col_str = ", ".join(columns)
            placeholders = ", ".join(["?"] * len(columns))
            values = tuple(filtered_row[col] for col in columns)

            insert_query = f"INSERT INTO {table} ({col_str}) VALUES ({placeholders})"
            cursor.execute(insert_query, values)

    db.commit()
    db.close()

@app.route("/init")
def init():
    create_db()
    return "Database created! <a href='/'>See stats</a>"

@app.route("/update")
def update():
    global SESSIONS
    try:
        response = requests.get("http://192.168.20.148:8080/get_data")
        response.raise_for_status()
        update_json = response.json()
    except requests.exceptions.RequestException as e:
        return f"Update failed, could not reach server: {e}", 500
    except ValueError:
        return "Update failed: invalid JSON received", 400
    try:
        update_db_from_json(update_json)
    except Exception as e:
        return f"Update failed while writing to database: {e}", 500
    SESSIONS = []
    return jsonify({"status": "success", "message": "Database updated successfully"})

@app.route("/")
def home():
    return render_template("home.html")

@app.route("/individual")
def individual_stats():
    db_players = get_players()
    if not db_players:
        abort(404, "No players found! Did you initialize the DB?")
    player = request.args.get("player") or db_players[0][0]
    players = db_players
    player_idx = [i for i, p in enumerate(players) if p[0] == player]
    if not player_idx:
        abort(404, f"Player '{player}' not found")
    idx = player_idx[0]
    sessions = parse_sessions(get_rounds())
    stats = analyze_stats(sessions, players, idx)
    return render_template("individual_stats.html",
        players=[p[0] for p in players],
        player=player,
        stats=stats,
    )

@app.route("/global")
def global_stats():
    db_players = get_players()
    if not db_players:
        abort(404, "No players found! Did you initialize the DB?")
    players = db_players
    sessions = parse_sessions(get_rounds())

    # Gather stats for each player, indexed by name
    all_stats = []
    for idx, player in enumerate(players):
        stat = analyze_stats(sessions, players, idx)
        stat["player"] = player[0]  # Use player name for easier Jinja
        all_stats.append(stat)

    # Build each table list, ranked
    table_games = sorted(all_stats, key=lambda s: -s["games"])
    table_wins = sorted(all_stats, key=lambda s: (-s["wins"], -s["win_rate"]))
    table_winrate = sorted(all_stats, key=lambda s: -s["win_rate"])
    table_avgpoints = sorted(all_stats, key=lambda s: s["avg_points_left"])
    table_maxpoints = sorted(all_stats, key=lambda s: -s["max_points"])
    table_per_session = sorted(all_stats, key=lambda s: -s["best_session_wins"])
    table_totalpoints0 = sorted(all_stats, key=lambda s: s["total_points_absence_zero"])
    table_totalpointsavg = sorted(all_stats, key=lambda s: s["total_points_absence_avg"])

    return render_template("global_stats.html",
        table_games=table_games,
        table_wins=table_wins,
        table_avgpoints=table_avgpoints,
        table_maxpoints=table_maxpoints,
        table_per_session=table_per_session,
        table_totalpoints0=table_totalpoints0,
        table_totalpointsavg=table_totalpointsavg,
    )

def create_db(players: list[tuple[str, str]] | None = None, games: list[tuple[int, ...]] | None = None):
    good_players: list[tuple[str, str]] = players or [("Alice", "player1"), ("Bob", "player2"), ("Cara", "player3")]
    good_games: list[tuple[int, ...]] = games or [
            (10, 0, 15),    # session 1
            (0, 8, 7),
            (1, 1, 1),      # session end
            (0, 1, 22),     # session 2
            (1, 1, 1),      # session end
        ]

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()

    c.execute("DROP TABLE IF EXISTS scores")
    c.execute("DROP TABLE IF EXISTS players")
    c.execute("DROP TABLE IF EXISTS hands")
    c.execute("PRAGMA foreign_keys = ON;")

    c.execute("""
        CREATE TABLE players (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            colname TEXT
        )
    """)
    c.executemany("INSERT INTO players (name, colname) VALUES (?, ?)", good_players)

    columns = ", ".join(f"{col} INTEGER" for _, col in good_players)
    c.execute(f"CREATE TABLE scores (id INTEGER PRIMARY KEY AUTOINCREMENT, {columns})")
    c.execute("CREATE TABLE hands (scores_id INTEGER REFERENCES scores(id), flag INTEGER)")

    placeholders = ", ".join(["?"] * len(good_players))
    c.executemany(f"INSERT INTO scores ({', '.join([col for _, col in good_players])}) VALUES ({placeholders})", good_games)
    c.executemany("INSERT INTO hands (scores_id, flag) VALUES (?, ?)", tuple((i, 0) for i in range(1, len(good_games) + 1)))  # Fix? (Session markers are taken too)
    conn.commit()
    conn.close()


if __name__ == "__main__":
    if not os.path.exists(DB_NAME):
        create_db()
    app.run(port=80, host="0.0.0.0", debug=True)
