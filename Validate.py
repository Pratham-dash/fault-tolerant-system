
import json


REQUIRED_FIELDS = ["client_id", "amount", "metric", "timestamp"]


def validate(normalized_event):

    print(f"VALIDATING: {json.dumps(normalized_event, indent=2)}")


    missing = []
    for field in REQUIRED_FIELDS:
        if normalized_event.get(field) is None:
            missing.append(field)

    if missing:
        error = f"Missing required fields: {', '.join(missing)}"
        print(f"VALIDATION FAILED: {error}")
        return False, error


    allowed_fields = REQUIRED_FIELDS.copy()
    extra_fields = []
    for key in normalized_event:
        if key not in allowed_fields:
            extra_fields.append(key)

    if extra_fields:
        print(f"WARNING: Ignoring extra fields: {extra_fields}")

        clean_event = {k: normalized_event[k] for k in allowed_fields if normalized_event[k] is not None}
        print(f"CLEANED (extra removed): {json.dumps(clean_event, indent=2)}")
        return True, None, clean_event
    else:
        print("VALIDATION PASSED ✓")
        return True, None, normalized_event



if __name__ == "__main__":

    bad_data = {"client_id": "A", "amount": 1200}
    print("TEST MISSING:")
    valid, error, cleaned = validate(bad_data)
    print(f"Result: {valid}, Error: {error}")


    extra_data = {
        "client_id": "A",
        "amount": 1200,
        "metric": "sales",
        "timestamp": "2024-01-01T00:00:00Z",
        "extra_field1": "ignore",
        "extra_field2": "ignore"
    }
    print("\nTEST EXTRA:")
    valid, error, cleaned = validate(extra_data)
    print(f"Result: {valid}, Cleaned: {json.dumps(cleaned, indent=2)}")
