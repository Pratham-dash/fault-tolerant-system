
import sqlite3

DB_FILE = "fault_tolerant_system.db"


# FUNCTION 1: Initialize table
def init_dedup_table():
    """Create dedup table if not exists"""
    conn = sqlite3.connect(DB_FILE)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dedup_table (
            client_id TEXT,
            fingerprint TEXT,
            status TEXT DEFAULT 'processing',
            event_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(client_id, fingerprint)
        )
    """)
    conn.commit()
    conn.close()
    print("Dedup table ready!")


# FUNCTION 2: Check dedup
def check_dedup(client_id, fingerprint):
    #Check if event already processed
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    c.execute(
        "SELECT status FROM dedup_table WHERE client_id=? AND fingerprint=?",
        (client_id, fingerprint)
    )
    result = c.fetchone()
    conn.close()

    if result:
        status = dict(result)["status"]
        print(f"DEDUP HIT: {status}")
        return status
    print("DEDUP MISS: New event!")
    return None


# FUNCTION 3: Mark processing
def mark_processing(client_id, fingerprint):
    """Mark as being processed"""
    conn = sqlite3.connect(DB_FILE)
    try:
        conn.execute(
            "INSERT INTO dedup_table (client_id, fingerprint, status) VALUES (?, ?, 'processing')",
            (client_id, fingerprint)
        )
        conn.commit()
        print("Marked as PROCESSING")
    except sqlite3.IntegrityError:
        print("Already processing (race condition)")
    finally:
        conn.close()



def mark_completed(client_id, fingerprint, event_id):
    #Mark as successfully completed
    conn = sqlite3.connect(DB_FILE)
    conn.execute(
        "UPDATE dedup_table SET status='completed', event_id=? WHERE client_id=? AND fingerprint=?",
        (event_id, client_id, fingerprint)
    )
    conn.commit()
    conn.close()
    print("Marked as COMPLETED")


# Initialize on import
init_dedup_table()
