
from flask import Flask, request, jsonify, render_template
import json
from flask_cors import CORS



from Normalise import normalize
from Validate import validate
from Hash import create_hash
from Dedup import (check_dedup, mark_processing, mark_completed, init_dedup_table)
from Events import save_event, init_events_table
from Aggregations import (get_aggregations, get_aggregations_by_metric, get_summary)


app = Flask(__name__)
CORS(app)
DB_FILE = "fault_tolerant_system.db"

@app.route("/")
def home():
    return render_template("index.html")

init_dedup_table()
init_events_table()




@app.route("/api/events", methods=["POST"])
def process_event():

    print("=" * 60)
    print("=== FULL PIPELINE START ===")
    print("=" * 60)


    raw_event = request.get_json()
    if not raw_event:
        return jsonify({"error": "No JSON provided"}), 400

    print("✓ STEP 1: RAW INPUT RECEIVED")
    print(f"  Source: {raw_event.get('source')}")


    print("\n→ STEP 2: NORMALIZING")
    normalized = normalize(raw_event)


    print("\n→ STEP 3: VALIDATING")
    is_valid, error, validated_event = validate(normalized)
    if not is_valid:
        print(f"  ✗ VALIDATION FAILED: {error}")
        return jsonify({
            "status": "failed",
            "step": "validate",
            "error": error,
            "raw_event": raw_event
        }), 400

    print("✓ STEP 3: VALIDATED")


    print("\n→ STEP 4: CREATING HASH")
    fingerprint = create_hash(validated_event)
    print(f"  Fingerprint: {fingerprint[:16]}...")


    print("\n→ STEP 5: DEDUP CHECK")
    client_id = validated_event["client_id"]
    dedup_status = check_dedup(client_id, fingerprint)

    if dedup_status == "completed":
        print("  ✗ DUPLICATE DETECTED (already completed)")
        return jsonify({
            "status": "duplicate",
            "fingerprint": fingerprint,
            "message": "Event already processed"
        }), 200

    if dedup_status == "processing":
        print("  Event still being processed")
        return jsonify({
            "status": "processing",
            "fingerprint": fingerprint,
            "message": "Still processing"
        }), 202

    print("  ✓ NEW EVENT (not in dedup)")


    print("\n→ STEP 5B: MARKING AS PROCESSING")
    mark_processing(client_id, fingerprint)

    try:

        print("\n→ STEP 6: SAVING TO DATABASE")
        event_id = save_event(validated_event, raw_event)
        print(f"  ✓ Event ID: {event_id}")


        print("\n→ STEP 7: MARKING AS COMPLETED")
        mark_completed(client_id, fingerprint, event_id)

        print("\n" + "=" * 60)
        print("=== ✓ PIPELINE COMPLETE ===")
        print("=" * 60)

        return jsonify({
            "status": "success",
            "event_id": event_id,
            "fingerprint": fingerprint,
            "message": "Event processed successfully"
        }), 201

    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        return jsonify({
            "status": "error",
            "error": str(e)
        }), 500



@app.route("/api/events", methods=["GET"])
def list_events():

    import sqlite3
    try:
        conn = sqlite3.connect(DB_FILE)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()

        c.execute("""
            SELECT 
                id, client_id, amount, metric, timestamp, created_at
            FROM events_log 
            ORDER BY id DESC 
            LIMIT 10
        """)

        events = [dict(row) for row in c.fetchall()]
        conn.close()

        return jsonify({
            "status": "success",
            "count": len(events),
            "events": events
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": str(e)
        }), 500


@app.route("/api/aggregations", methods=["GET"])
def aggregations():

    try:
        client_id = request.args.get("client_id")
        start_date = request.args.get("start_date")
        end_date = request.args.get("end_date")

        print(f"\n AGGREGATION QUERY: client={client_id}, start={start_date}, end={end_date}")

        aggregations_data = get_aggregations(client_id, start_date, end_date)

        return jsonify({
            "status": "success",
            "count": len(aggregations_data),
            "filters": {
                "client_id": client_id,
                "start_date": start_date,
                "end_date": end_date
            },
            "aggregations": aggregations_data
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": str(e)
        }), 500


@app.route("/api/aggregations/by-metric", methods=["GET"])
def aggregations_by_metric():

    try:
        start_date = request.args.get("start_date")
        end_date = request.args.get("end_date")

        print(f"\n METRIC AGGREGATION QUERY: start={start_date}, end={end_date}")

        aggregations_data = get_aggregations_by_metric(start_date, end_date)

        return jsonify({
            "status": "success",
            "count": len(aggregations_data),
            "aggregations": aggregations_data
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": str(e)
        }), 500


@app.route("/api/summary", methods=["GET"])
def summary():

    try:
        print("\ SUMMARY QUERY")
        summary_data = get_summary()

        return jsonify({
            "status": "success",
            "summary": summary_data
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": str(e)
        }), 500


#  health check

@app.route("/api/health", methods=["GET"])
def health():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "service": "Fault-Tolerant Data Processing System",
        "version": "1.0"
    }), 200


#  start server

if __name__ == "__main__":
    print("=" * 60)
    print(" Fault-Tolerant Data Processing System Starting...")
    print("=" * 60)
    print("\n API Endpoints:")
    print("  POST /api/events              - Submit new event")
    print("  GET  /api/events              - List all events")
    print("  GET  /api/aggregations        - Aggregations by client")
    print("  GET  /api/aggregations/by-metric - Aggregations by metric")
    print("  GET  /api/summary             - System summary")
    print("  GET  /api/health              - Health check")
    print("\n" + "=" * 60)

    app.run(debug=True, port=5000)
