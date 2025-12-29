
import sqlite3
from datetime import datetime

DB_FILE = "fault_tolerant_system.db"


def get_aggregations(client_id=None, start_date=None, end_date=None):
    """
    Get aggregated data with optional filtering
    Returns: list of aggregations by client_id
    """
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()


    query = """
        SELECT 
            client_id,
            COUNT(*) as total_events,
            SUM(amount) as total_amount,
            AVG(amount) as avg_amount,
            MIN(amount) as min_amount,
            MAX(amount) as max_amount
        FROM events_log
        WHERE 1=1
    """
    params = []

    # Filter by client_id
    if client_id:
        query += " AND client_id = ?"
        params.append(client_id)

    # Filter by start date
    if start_date:
        query += " AND DATE(timestamp) >= ?"
        params.append(start_date)

    # Filter by end date
    if end_date:
        query += " AND DATE(timestamp) <= ?"
        params.append(end_date)

    # Group by client
    query += " GROUP BY client_id ORDER BY total_amount DESC"

    print(f"QUERY: {query} | PARAMS: {params}")
    c.execute(query, tuple(params))

    results = []
    for row in c.fetchall():
        results.append({
            "client_id": row["client_id"],
            "total_events": row["total_events"],
            "total_amount": row["total_amount"],
            "avg_amount": round(row["avg_amount"], 2) if row["avg_amount"] else 0,
            "min_amount": row["min_amount"],
            "max_amount": row["max_amount"]
        })

    conn.close()
    return results


def get_aggregations_by_metric(start_date=None, end_date=None):

    #Get aggregated data grouped by metric (not client)

    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    query = """
        SELECT 
            metric,
            COUNT(*) as total_events,
            SUM(amount) as total_amount,
            AVG(amount) as avg_amount,
            MIN(amount) as min_amount,
            MAX(amount) as max_amount
        FROM events_log
        WHERE 1=1
    """
    params = []

    if start_date:
        query += " AND DATE(timestamp) >= ?"
        params.append(start_date)

    if end_date:
        query += " AND DATE(timestamp) <= ?"
        params.append(end_date)

    query += " GROUP BY metric ORDER BY total_amount DESC"

    c.execute(query, tuple(params))

    results = []
    for row in c.fetchall():
        results.append({
            "metric": row["metric"],
            "total_events": row["total_events"],
            "total_amount": row["total_amount"],
            "avg_amount": round(row["avg_amount"], 2) if row["avg_amount"] else 0,
            "min_amount": row["min_amount"],
            "max_amount": row["max_amount"]
        })

    conn.close()
    return results


def get_summary():
    #Get overall system summary
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    c.execute("SELECT COUNT(*) as total_events, SUM(amount) as total_amount FROM events_log")
    row = c.fetchone()

    c.execute("SELECT COUNT(DISTINCT client_id) as unique_clients FROM events_log")
    clients = c.fetchone()

    conn.close()

    return {
        "total_events": row[0],
        "total_amount": row[1],
        "unique_clients": clients[0],
        "timestamp": datetime.now().isoformat()
    }
