import sqlite3


conn = sqlite3.connect("old_data.db")
cursor = conn.cursor()

cursor.execute("SELECT * FROM players")

players = [x[1:] for x in cursor.fetchall()]

cursor.execute("SELECT * FROM scores")

scores = [x[1:] for x in cursor.fetchall()]

cursor.execute("SELECT * FROM hands")

hands = {k: v for (k, v) in cursor.fetchall()}

from main import create_db

create_db(players, scores, hands)
