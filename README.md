git clone https://github.com/Pratham-dash/fault-tolerant-system.git
cd fault-tolerant-system
pip install -r requirements.txt
python3 -m venv venv
source venv/bin/activate
python init_db.py
python Input_JSON.py

Open http://localhost:5000

1. assumptions i made:
-every event has source field (client_id)
-Amount is always numeric (integer or float)
-Metric is one of: sales, orders, revenue, users, views
-Timestamp format is YYYY-MM-DD
-JSON is valid and parseable
-Every payload has amount, metric, timestamp
-SQLite is single-process local file
-No concurrent connections from multiple machines

2. prevention of double counting:
Three-Layer Deduplication Strategy:
Layer 1: SHA256 Fingerprinting (Hash.py)
Layer 2: Database Lookup (Dedup.py)
Layer 3: Append-Only Log (Events.py)

What Happens If Database Fails Mid-Request?
Scenario A: Database File Locked (Another Process Writing)
try:
    db.execute("INSERT INTO events ...")
    db.commit()
except sqlite3.OperationalError("database is locked"):
    # Returns 500 error to client
    # Event NOT saved
    # Client can RETRY (dedup will catch it)

Scenario B: Disk Full During Write
try:
    db.execute("INSERT ...")
    db.commit()  # ← FAILS here
except sqlite3.OperationalError("disk I/O error"):
    # Partial insert ROLLED BACK (SQLite magic)
    # Database consistent
    # Event NOT in DB
    # Client retries → Works

scenario C:
    connection = sqlite3.connect("events.db")
cursor.execute("INSERT INTO events ...")
    #Event saved despite network failure.

What Would Break First at Scale?
SQLite Write Lock Bottleneck:

1 user = 20ms response. Each additional user adds 10ms (queue waits). 100 concurrent users = 1000ms total. Browser timeout at 30 seconds breaks at ~1000 concurrent users.



