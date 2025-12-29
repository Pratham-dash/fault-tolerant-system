
import sqlite3
import json

DB_FILE = "fault_tolerant_system.db"


# FUNCTION 1: Initialize table
def init_events_table():
    """Create events_log table (append-only!)"""
    conn = sqlite3.connect(DB_FILE)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS events_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id TEXT NOT NULL,
            amount REAL,
            metric TEXT,
            timestamp TEXT,
            raw_payload TEXT,
            normalized_payload TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()
    print("Events table ready!")


# FUNCTION 2: Save event
def save_event(validated_event, raw_event):
    """Append event to log (NEVER update/delete!)"""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    c.execute("""
        INSERT INTO events_log 
        (client_id, amount, metric, timestamp, raw_payload, normalized_payload)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        validated_event["client_id"],
        validated_event["amount"],
        validated_event["metric"],
        validated_event["timestamp"],
        json.dumps(raw_event),
        json.dumps(validated_event)
    ))

    event_id = c.lastrowid
    conn.commit()
    conn.close()

    print(f"EVENT SAVED: ID={event_id}")
    return event_id


# Initialize on import (auto-create table)
init_events_table()
