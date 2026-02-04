"""Unit tests for data_quality.validators (Topic 5)."""
import pytest

try:
    from data_quality.validators import validate_record, validate_schema, validate_ranges
except ImportError:
    from validators import validate_record, validate_schema, validate_ranges


def test_validate_schema_valid():
    record = {
        "sensor_id": "SENSOR_MUM_001",
        "location": "Mumbai",
        "state": "Maharashtra",
        "device_type": "Temperature Sensor",
        "temperature": 28.5,
        "humidity": 65.0,
        "energy_consumption": 2.1,
        "timestamp": "2025-01-15T10:00:00+00:00",
        "signal_strength": -70,
        "battery_level": 90,
    }
    result = validate_schema(record)
    assert result["is_valid"] is True
    assert len(result["failure_reasons"]) == 0


def test_validate_schema_missing_field():
    record = {"sensor_id": "SENSOR_MUM_001"}
    result = validate_schema(record)
    assert result["is_valid"] is False
    assert any("Missing required" in r for r in result["failure_reasons"])


def test_validate_ranges_valid():
    record = {
        "temperature": 25.0,
        "humidity": 50.0,
        "energy_consumption": 1.5,
        "signal_strength": -80,
        "battery_level": 100,
    }
    result = validate_ranges(record)
    assert result["is_valid"] is True


def test_validate_ranges_out_of_range():
    record = {"temperature": 200.0}
    result = validate_ranges(record)
    assert result["is_valid"] is False
    assert any("Out of range" in r for r in result["failure_reasons"])


def test_validate_record_invalid_sensor_id():
    record = {
        "sensor_id": "invalid",
        "location": "Mumbai",
        "state": "MH",
        "device_type": "Temp",
        "temperature": 25.0,
        "humidity": 50.0,
        "energy_consumption": 1.0,
        "timestamp": "2025-01-15T10:00:00+00:00",
        "signal_strength": -70,
        "battery_level": 90,
    }
    result = validate_record(record)
    assert result["is_valid"] is False
