# Hash.py - Step 4: Create fingerprint of validated data
import hashlib
import json


def create_hash(validated_event):
    """
    Create SHA256 fingerprint of validated event
    Same data → same hash → duplicate detection!
    """
    print(f"HASHING: {json.dumps(validated_event, indent=2)}")

    # Hash ONLY business fields (client_id, amount, metric, timestamp)
    data_to_hash = {
        "client_id": validated_event["client_id"],
        "amount": validated_event["amount"],
        "metric": validated_event["metric"],
        "timestamp": validated_event["timestamp"]
    }

    # Sort keys + JSON string + SHA256
    hash_string = json.dumps(data_to_hash, sort_keys=True)
    fingerprint = hashlib.sha256(hash_string.encode()).hexdigest()

    print(f"FINGERPRINT: {fingerprint[:16]}...")  # First 16 chars
    return fingerprint


# Test standalone
if __name__ == "__main__":
    test_event = {
        "client_id": "client_A",
        "amount": 1200.0,
        "metric": "sales",
        "timestamp": "2024-01-01T00:00:00Z"
    }
    hash_value = create_hash(test_event)
    print(f"Full hash: {hash_value}")
