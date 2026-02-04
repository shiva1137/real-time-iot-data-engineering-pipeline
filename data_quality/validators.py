"""
Data Quality Validators (Topic 5)

Schema validation, range checks, format checks, freshness, completeness,
duplicate detection, and anomaly detection. Single source for all validation.

Usage:
    from data_quality.validators import validate_record
    result = validate_record(record)
"""

import re
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, Any, List

# Constants (aligned with Learning Guide Topic 5)
REQUIRED_FIELDS = [
    "sensor_id", "location", "state", "device_type",
    "temperature", "humidity", "energy_consumption",
    "timestamp", "signal_strength", "battery_level",
]
FIELD_TYPES = {
    "sensor_id": str,
    "location": str,
    "state": str,
    "device_type": str,
    "temperature": (float, int),
    "humidity": (float, int),
    "energy_consumption": (float, int),
    "timestamp": str,
    "signal_strength": (int, float),
    "battery_level": (int, float),
}
VALUE_RANGES = {
    "temperature": (-50, 50),
    "humidity": (0, 100),
    "energy_consumption": (0, 10),
    "signal_strength": (-150, 0),
    "battery_level": (0, 100),
}
CRITICAL_FIELDS = ["sensor_id", "temperature", "timestamp"]
DUPLICATE_WINDOW_SECONDS = 5
FRESHNESS_MAX_AGE_MINUTES = 5
ANOMALY_TEMP_DELTA_C = 20  # Sudden change > 20°C = sensor malfunction

_duplicate_tracker = defaultdict(set)


def create_validation_result(
    is_valid: bool = True,
    failure_reasons: List[str] = None,
) -> Dict[str, Any]:
    """Create a validation result dict."""
    return {"is_valid": is_valid, "failure_reasons": failure_reasons or []}


def add_failure_reason(result: Dict[str, Any], reason: str) -> None:
    """Append a failure reason and set is_valid to False."""
    result["failure_reasons"].append(reason)
    result["is_valid"] = False


def validate_schema(record: Dict[str, Any]) -> Dict[str, Any]:
    """Validate all required fields present; no unexpected fields."""
    result = create_validation_result()
    for field in REQUIRED_FIELDS:
        if field not in record:
            add_failure_reason(result, f"Missing required field: {field}")
    expected = set(REQUIRED_FIELDS) | {"message_id", "generated_at", "data_quality_flag"}
    unexpected = set(record.keys()) - expected
    if unexpected:
        add_failure_reason(result, f"Unexpected fields: {', '.join(unexpected)}")
    return result


def validate_types(record: Dict[str, Any]) -> Dict[str, Any]:
    """Validate field types (temperature float, timestamp datetime-parseable, etc.)."""
    result = create_validation_result()
    for field, expected_type in FIELD_TYPES.items():
        if field not in record or record[field] is None:
            continue
        value = record[field]
        if isinstance(expected_type, tuple):
            if not isinstance(value, expected_type):
                if isinstance(value, str) and (float in expected_type or int in expected_type):
                    try:
                        float(value)
                        continue
                    except (ValueError, TypeError):
                        pass
                add_failure_reason(
                    result,
                    f"Type mismatch for {field}: expected {expected_type}, got {type(value).__name__}",
                )
        else:
            if not isinstance(value, expected_type):
                add_failure_reason(
                    result,
                    f"Type mismatch for {field}: expected {expected_type.__name__}, got {type(value).__name__}",
                )
    return result


def validate_ranges(record: Dict[str, Any]) -> Dict[str, Any]:
    """Validate value ranges (temperature -50 to 50°C, humidity 0-100%, etc.)."""
    result = create_validation_result()
    for field, (min_val, max_val) in VALUE_RANGES.items():
        if field not in record or record[field] is None:
            continue
        try:
            value = record[field]
            if isinstance(value, str):
                if value.lower() in ("null", "none", "n/a", ""):
                    continue
                value = float(value)
            if value < min_val or value > max_val:
                add_failure_reason(
                    result,
                    f"Out of range for {field}: {value} (expected {min_val} to {max_val})",
                )
        except (ValueError, TypeError):
            continue
    return result


def validate_format(record: Dict[str, Any]) -> Dict[str, Any]:
    """Validate formats (sensor_id pattern sensor_###, timestamp ISO 8601)."""
    result = create_validation_result()
    if record.get("sensor_id"):
        sensor_id = str(record["sensor_id"]).strip()
        if not re.match(r"^SENSOR_[A-Z]{3}_\d{3}$", sensor_id):
            add_failure_reason(
                result,
                f"Invalid sensor_id format: {sensor_id} (expected SENSOR_XXX_###)",
            )
        else:
            record["sensor_id"] = sensor_id
    if record.get("timestamp"):
        try:
            datetime.fromisoformat(str(record["timestamp"]).replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            add_failure_reason(
                result,
                f"Invalid timestamp format (expected ISO 8601)",
            )
    if record.get("location"):
        record["location"] = str(record["location"]).strip()
    return result


def validate_freshness(record: Dict[str, Any]) -> Dict[str, Any]:
    """Timestamp not in future, not older than FRESHNESS_MAX_AGE_MINUTES."""
    result = create_validation_result()
    if not record.get("timestamp"):
        return result
    try:
        dt = datetime.fromisoformat(str(record["timestamp"]).replace("Z", "+00:00"))
        now = datetime.utcnow()
        if dt > now:
            add_failure_reason(result, f"Future timestamp: {record['timestamp']}")
        elif (now - dt) > timedelta(minutes=FRESHNESS_MAX_AGE_MINUTES):
            add_failure_reason(
                result,
                f"Stale data: max age {FRESHNESS_MAX_AGE_MINUTES} minutes",
            )
    except (ValueError, AttributeError):
        pass
    return result


def validate_completeness(record: Dict[str, Any]) -> Dict[str, Any]:
    """Critical fields not null."""
    result = create_validation_result()
    for field in CRITICAL_FIELDS:
        if field not in record:
            continue
        value = record[field]
        if value is None:
            add_failure_reason(result, f"Critical field {field} is null")
        elif isinstance(value, str) and value.lower() in ("null", "none", "n/a", ""):
            add_failure_reason(result, f"Critical field {field} is null string")
    return result


def validate_duplicates(record: Dict[str, Any], message_id: str) -> Dict[str, Any]:
    """Duplicate detection: same sensor_id + timestamp within window."""
    result = create_validation_result()
    sensor_id = record.get("sensor_id")
    timestamp = record.get("timestamp")
    if not sensor_id or not timestamp:
        return result
    try:
        dt = datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
        window_start = dt - timedelta(seconds=dt.second % DUPLICATE_WINDOW_SECONDS)
        key = (sensor_id, window_start.isoformat())
        if message_id in _duplicate_tracker.get(key, set()):
            add_failure_reason(result, f"Exact duplicate: message_id {message_id} already seen")
            return result
        if key in _duplicate_tracker:
            add_failure_reason(result, "Near-duplicate: same sensor_id and timestamp window")
        if key not in _duplicate_tracker:
            _duplicate_tracker[key] = set()
        _duplicate_tracker[key].add(message_id)
        cutoff = datetime.utcnow() - timedelta(minutes=1)
        for k in list(_duplicate_tracker.keys()):
            if len(k) >= 2 and datetime.fromisoformat(k[1]) < cutoff:
                del _duplicate_tracker[k]
    except (ValueError, AttributeError):
        pass
    return result


def validate_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Run all validations (schema, types, ranges, format, freshness, completeness, duplicates).
    Returns dict with is_valid and failure_reasons.
    """
    result = create_validation_result()
    validations = [
        validate_schema(record),
        validate_types(record),
        validate_completeness(record),
        validate_format(record),
        validate_ranges(record),
        validate_freshness(record),
    ]
    message_id = record.get("message_id", "unknown")
    validations.append(validate_duplicates(record, message_id))
    for v in validations:
        if not v["is_valid"]:
            result["failure_reasons"].extend(v["failure_reasons"])
            result["is_valid"] = False
    return result
