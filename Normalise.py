
import json


def normalize(raw_event):

    print(f"NORMALIZING: {json.dumps(raw_event, indent=2)}")

    client_id = raw_event.get("source", "unknown")
    payload = raw_event.get("payload", {})


    normalized = {
        "client_id": client_id
    }


    amount_str = payload.get("amount")
    if amount_str:
        try:
            normalized["amount"] = float(amount_str)
        except ValueError:
            normalized["amount"] = None
            print("WARNING: Invalid amount")
    else:
        normalized["amount"] = None


    normalized["metric"] = payload.get("metric")


    timestamp_str = payload.get("timestamp")
    if timestamp_str:

        normalized["timestamp"] = timestamp_str.replace("/", "-") + "T00:00:00Z"
    else:
        normalized["timestamp"] = None

    print(f"NORMALIZED: {json.dumps(normalized, indent=2)}")
    return normalized



if __name__ == "__main__":
    test_raw = {
        "source": "client_A",
        "payload": {
            "amount": "1200",
            "metric": "sales",
            "timestamp": "2024/01/01"
        }
    }
    result = normalize(test_raw)
    print(f"FINAL RESULT: {result}")
