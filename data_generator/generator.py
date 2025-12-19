"""
IoT Data Generator - India-Based with Comprehensive Data Quality Issues

This module generates realistic IoT sensor data for 100 sensors deployed across
major Indian cities. It intentionally introduces various data quality issues to
simulate real-world scenarios and test validation logic.

All logic uses functions - no classes needed for data generation.

Data Quality Issues Introduced:
- Null/Missing Values (15% of records)
- Duplicate Records (5% of records)
- Out-of-Range Values (12% of records)
- Data Type Mismatches (5% of records)
- Schema Violations (4% of records)
- Formatting Issues (6% of records)
- Sensor Reading Spikes/Anomalies (8% of records)

Usage:
    python generator.py
"""

import os
import json
import time
import random
import uuid
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List
from faker import Faker
from dotenv import load_dotenv
from zoneinfo import ZoneInfo

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Faker with India locale
fake = Faker('en_IN')

# Constants
INDIAN_CITIES = [
    {"city": "Mumbai", "state": "Maharashtra", "code": "MUM"},
    {"city": "Delhi", "state": "Delhi", "code": "DEL"},
    {"city": "Bangalore", "state": "Karnataka", "code": "BLR"},
    {"city": "Chennai", "state": "Tamil Nadu", "code": "CHE"},
    {"city": "Kolkata", "state": "West Bengal", "code": "KOL"},
    {"city": "Hyderabad", "state": "Telangana", "code": "HYD"},
    {"city": "Pune", "state": "Maharashtra", "code": "PUN"},
    {"city": "Ahmedabad", "state": "Gujarat", "code": "AHM"},
    {"city": "Jaipur", "state": "Rajasthan", "code": "JAI"},
    {"city": "Surat", "state": "Gujarat", "code": "SUR"},
    {"city": "Lucknow", "state": "Uttar Pradesh", "code": "LUC"},
    {"city": "Kanpur", "state": "Uttar Pradesh", "code": "KAN"},
    {"city": "Nagpur", "state": "Maharashtra", "code": "NAG"},
    {"city": "Indore", "state": "Madhya Pradesh", "code": "IND"},
    {"city": "Thane", "state": "Maharashtra", "code": "THA"},
]

DEVICE_TYPES = [
    "Temperature Sensor",
    "Humidity Monitor",
    "Energy Meter",
    "Air Quality Monitor",
    "Motion Detector",
    "Pressure Sensor",
    "Light Sensor",
    "Vibration Sensor",
]

# Module-level state (for tracking statistics and duplicates)
_generated_records = {}  # {(sensor_id, timestamp): record}
_message_count = 0
_quality_issue_stats = {
    "null_values": 0,
    "duplicates": 0,
    "late_data": 0,
    "out_of_range": 0,
    "type_mismatch": 0,
    "schema_violation": 0,
    "formatting_issue": 0,
    "network_failure": 0,
    "sensor_spikes": 0,
}


# ============================================================================
# Helper Functions
# ============================================================================

def get_random_city() -> Dict[str, str]:
    """Get random Indian city."""
    return random.choice(INDIAN_CITIES)


def generate_sensor_id(city_code: str, index: int) -> str:
    """Generate sensor ID in format SENSOR_{CITY_CODE}_{3-digit-id}."""
    return f"SENSOR_{city_code}_{index:03d}"


# ============================================================================
# Core Functions
# ============================================================================

def initialize_sensors(num_sensors: int = 100) -> List[Dict[str, Any]]:
    """
    Initialize sensor configurations across Indian cities.
    
    Args:
        num_sensors: Number of sensors to create (default: 100)
        
    Returns:
        List of sensor configuration dictionaries
    """
    sensors = []
    sensor_id_counter = {}
    
    for i in range(num_sensors):
        # Distribute sensors across cities
        city_info = get_random_city()
        city_code = city_info["code"]
        
        # Track sensor count per city
        if city_code not in sensor_id_counter:
            sensor_id_counter[city_code] = 0
        sensor_id_counter[city_code] += 1
        
        sensor_id = generate_sensor_id(city_code, sensor_id_counter[city_code])
        
        sensors.append({
            "sensor_id": sensor_id,
            "location": city_info["city"],
            "state": city_info["state"],
            "device_type": random.choice(DEVICE_TYPES),
            "latitude": fake.latitude(),
            "longitude": fake.longitude(),
        })
    
    logger.info(f"Initialized {len(sensors)} sensors across {len(INDIAN_CITIES)} cities")
    return sensors


def generate_base_record(sensor: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generate a base valid record without quality issues.
    
    Args:
        sensor: Sensor configuration dictionary
        
    Returns:
        Base record dictionary with valid data
    """
    now = datetime.now(ZoneInfo('Asia/Kolkata'))
    
    return {
        "sensor_id": sensor["sensor_id"],
        "location": sensor["location"],
        "state": sensor["state"],
        "device_type": sensor["device_type"],
        "temperature": round(random.uniform(-5, 45), 2),  # Indian climate range
        "humidity": round(random.uniform(20, 80), 2),  # Typical Indian weather
        "energy_consumption": round(random.uniform(0.5, 5), 2),  # kWh
        "timestamp": now.isoformat(),
        "signal_strength": random.randint(-100, 0),  # dBm
        "battery_level": random.randint(0, 100),  # Percentage
    }


def introduce_null_values(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Introduce null/missing values (15% of records).
    
    Strategy:
    - Non-critical fields: 5% chance per field
    - Critical fields: 2% chance total
    
    Args:
        record: Record dictionary to modify
        
    Returns:
        Modified record dictionary
    """
    global _quality_issue_stats
    
    if random.random() < 0.15:  # 15% of records have nulls
        # Non-critical fields (5% chance each)
        if random.random() < 0.03:  # 3% chance
            record["signal_strength"] = None  # "Sensor offline"
            _quality_issue_stats["null_values"] += 1
            
        if random.random() < 0.02:  # 2% chance
            record["battery_level"] = None  # "Battery reading failed"
            _quality_issue_stats["null_values"] += 1
            
        if random.random() < 0.03:  # 3% chance
            record["humidity"] = None  # "Sensor malfunction"
            _quality_issue_stats["null_values"] += 1
            
        if random.random() < 0.02:  # 2% chance
            record["energy_consumption"] = None  # "Meter error"
            _quality_issue_stats["null_values"] += 1
        
        # Critical fields (2% chance total)
        if random.random() < 0.01:  # 1% chance
            record["temperature"] = None  # "Critical sensor failure"
            _quality_issue_stats["null_values"] += 1
            
        if random.random() < 0.005:  # 0.5% chance
            record["timestamp"] = None  # "Clock sync issue"
            _quality_issue_stats["null_values"] += 1
            
        if random.random() < 0.005:  # 0.5% chance
            record["sensor_id"] = None  # "Malformed message"
            _quality_issue_stats["null_values"] += 1
    
    return record


def introduce_duplicates(record: Dict[str, Any], sensors: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Introduce duplicate records (5% of records).
    
    Types:
    - Exact duplicates: Same sensor_id + timestamp + all values (5% chance)
    - Simulates network retries or message queue duplicates
    
    Args:
        record: Record dictionary to modify
        sensors: List of all sensor configurations (for validation)
        
    Returns:
        Modified record dictionary
    """
    global _quality_issue_stats, _generated_records
    
    if random.random() < 0.05:  # 5% of records are duplicates
        sensor_id = record.get("sensor_id")
        timestamp = record.get("timestamp")
        
        if not sensor_id or not timestamp:
            return record
        
        # Exact duplicates (simulates network retry)
        if (sensor_id, timestamp) in _generated_records:
            _quality_issue_stats["duplicates"] += 1
            return _generated_records[(sensor_id, timestamp)].copy()
    
    return record


def introduce_late_data(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Introduce late-arriving data (10% of records).
    
    Types:
    - 1-5 minutes in the past (5% chance)
    - 5-15 minutes in the past (3% chance)
    - 15-60 minutes in the past (2% chance)
    - Future timestamps (0.5% chance)
    
    Args:
        record: Record dictionary to modify
        
    Returns:
        Modified record dictionary
    """
    global _quality_issue_stats
    
    if random.random() < 0.10:  # 10% of records are late
        rand = random.random()
        
        if rand < 0.05:  # 5% - 1-5 minutes late
            record["timestamp"] = (datetime.now(ZoneInfo('Asia/Kolkata')) - timedelta(minutes=random.randint(1, 5))).isoformat()
            _quality_issue_stats["late_data"] += 1
            
        elif rand < 0.08:  # 3% - 5-15 minutes late
            record["timestamp"] = (datetime.now(ZoneInfo('Asia/Kolkata')) - timedelta(minutes=random.randint(5, 15))).isoformat()
            _quality_issue_stats["late_data"] += 1
            
        elif rand < 0.10:  # 2% - 15-60 minutes late
            record["timestamp"] = (datetime.now(ZoneInfo('Asia/Kolkata')) - timedelta(minutes=random.randint(15, 60))).isoformat()
            _quality_issue_stats["late_data"] += 1
            
    
    return record


def introduce_out_of_range_values(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Introduce out-of-range values (12% of records).
    
    Types:
    - Temperature: > 50°C or < -50°C (3% chance)
    - Humidity: > 100% or < 0% (2% chance)
    - Energy: Negative or > 10 kWh (2% chance)
    - Battery: > 100% or < 0% (3% chance)
    - Signal strength: > 0 dBm or < -150 dBm (2% chance)
    
    Args:
        record: Record dictionary to modify
        
    Returns:
        Modified record dictionary
    """
    global _quality_issue_stats
    
    if random.random() < 0.12:  # 12% of records have out-of-range values
        rand = random.random()
        
        if rand < 0.03:  # 3% - Temperature out of range
            if random.random() < 0.5:
                record["temperature"] = random.uniform(50, 100)  # Too high
            else:
                record["temperature"] = random.uniform(-100, -50)  # Too low
            _quality_issue_stats["out_of_range"] += 1
            
        elif rand < 0.05:  # 2% - Humidity out of range
            if random.random() < 0.5:
                record["humidity"] = random.uniform(100, 150)  # > 100%
            else:
                record["humidity"] = random.uniform(-50, 0)  # < 0%
            _quality_issue_stats["out_of_range"] += 1
            
        elif rand < 0.07:  # 2% - Energy out of range
            if random.random() < 0.5:
                record["energy_consumption"] = random.uniform(10, 50)  # Too high
            else:
                record["energy_consumption"] = random.uniform(-10, 0)  # Negative
            _quality_issue_stats["out_of_range"] += 1
            
        elif rand < 0.10:  # 3% - Battery out of range
            if random.random() < 0.5:
                record["battery_level"] = random.randint(101, 200)  # > 100%
            else:
                record["battery_level"] = random.randint(-50, -1)  # < 0%
            _quality_issue_stats["out_of_range"] += 1
            
        elif rand < 0.12:  # 2% - Signal strength out of range
            if random.random() < 0.5:
                record["signal_strength"] = random.randint(1, 50)  # > 0 dBm
            else:
                record["signal_strength"] = random.randint(-200, -151)  # < -150 dBm
            _quality_issue_stats["out_of_range"] += 1
    
    return record


def introduce_type_mismatches(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Introduce data type mismatches (5% of records).
    
    Types:
    - Temperature as string instead of float (2% chance)
    - Timestamp as string in wrong format (1% chance)
    - Numeric fields as null strings "null" or "N/A" (2% chance)
    
    Args:
        record: Record dictionary to modify
        
    Returns:
        Modified record dictionary
    """
    global _quality_issue_stats
    
    if random.random() < 0.05:  # 5% of records have type mismatches
        rand = random.random()
        
        if rand < 0.02:  # 2% - Temperature as string
            record["temperature"] = str(record["temperature"])  # "Serialization error"
            _quality_issue_stats["type_mismatch"] += 1
            
        elif rand < 0.03:  # 1% - Timestamp in wrong format
            record["timestamp"] = datetime.now(ZoneInfo('Asia/Kolkata')).strftime("%d/%m/%Y %H:%M:%S")  # Wrong format
            _quality_issue_stats["type_mismatch"] += 1
            
        elif rand < 0.05:  # 2% - Null strings
            field = random.choice(["humidity", "energy_consumption", "battery_level"])
            if field in record:
                record[field] = random.choice(["null", "N/A", "NULL", "None"])  # String instead of null
                _quality_issue_stats["type_mismatch"] += 1
    
    return record


def introduce_schema_violations(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Introduce schema violations (4% of records).
    
    Types:
    - Missing required fields (2% chance)
    - Extra unexpected fields (1% chance)
    - Incorrect field names (typos) (1% chance)
    
    Args:
        record: Record dictionary to modify
        
    Returns:
        Modified record dictionary
    """
    global _quality_issue_stats
    
    if random.random() < 0.04:  # 4% of records have schema violations
        rand = random.random()
        
        if rand < 0.02:  # 2% - Missing required fields
            if "device_type" in record:
                del record["device_type"]  # "Incomplete transmission"
            _quality_issue_stats["schema_violation"] += 1
            
        elif rand < 0.03:  # 1% - Extra unexpected fields
            record["unexpected_field"] = "random_value"  # "Schema evolution issue"
            _quality_issue_stats["schema_violation"] += 1
            
        elif rand < 0.04:  # 1% - Incorrect field names
            if "temperature" in record:
                record["temprature"] = record.pop("temperature")  # Typo
            _quality_issue_stats["schema_violation"] += 1
    
    return record


def introduce_formatting_issues(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Introduce formatting issues (6% of records).
    
    Types:
    - sensor_id with extra spaces/trailing chars (2% chance)
    - Location names in mixed case or abbreviations (2% chance)
    - Timestamp in different timezones (1% chance)
    - Unicode characters in location names (1% chance)
    
    Args:
        record: Record dictionary to modify
        
    Returns:
        Modified record dictionary
    """
    global _quality_issue_stats
    
    if random.random() < 0.06:  # 6% of records have formatting issues
        rand = random.random()
        
        if rand < 0.02:  # 2% - Extra spaces in sensor_id
            if "sensor_id" in record and record["sensor_id"]:
                record["sensor_id"] = f"  {record['sensor_id']}  "  # Extra spaces
            _quality_issue_stats["formatting_issue"] += 1
            
        elif rand < 0.04:  # 2% - Mixed case location
            if "location" in record:
                record["location"] = record["location"].swapcase()  # Mixed case
            _quality_issue_stats["formatting_issue"] += 1
            
        elif rand < 0.05:  # 1% - Timezone confusion
            if "timestamp" in record and record["timestamp"]:
                record["timestamp"] = record["timestamp"].replace("+00:00", "+05:30")  # IST
            _quality_issue_stats["formatting_issue"] += 1
            
        elif rand < 0.06:  # 1% - Unicode characters
            if "location" in record:
                record["location"] = f"{record['location']} (मुंबई)"  # Unicode
            _quality_issue_stats["formatting_issue"] += 1
    
    return record


def introduce_sensor_spikes(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Introduce sensor reading spikes/anomalies (8% of records).
    
    Common in modern IoT deployments due to:
    - Sensor interference or electromagnetic noise
    - Temporary sensor malfunctions
    - Environmental anomalies (e.g., sudden temperature spikes)
    - Sensor calibration drift
    - Power fluctuations affecting sensor readings
    
    Types:
    - Temperature spikes: Sudden jumps of ±15-30°C (3% chance)
    - Humidity spikes: Sudden changes of ±30-50% (2% chance)
    - Energy consumption spikes: Sudden jumps of 3-5x normal (2% chance)
    - Battery level anomalies: Sudden drops or jumps (1% chance)
    
    Args:
        record: Record dictionary to modify
        
    Returns:
        Modified record dictionary
    """
    global _quality_issue_stats
    
    if random.random() < 0.08:  # 8% of records have sensor spikes
        rand = random.random()
        
        if rand < 0.03:  # 3% - Temperature spikes
            if "temperature" in record and record["temperature"] is not None and isinstance(record["temperature"], (int, float)):
                base_temp = record["temperature"]
                # Sudden spike: jump by 15-30°C in either direction
                spike_amount = random.uniform(15, 30) * random.choice([-1, 1])
                record["temperature"] = round(base_temp + spike_amount, 2)
                _quality_issue_stats["sensor_spikes"] += 1
                
        elif rand < 0.05:  # 2% - Humidity spikes
            if "humidity" in record and record["humidity"] is not None and isinstance(record["humidity"], (int, float)):
                base_humidity = record["humidity"]
                # Sudden change: jump by 30-50% in either direction
                spike_amount = random.uniform(30, 50) * random.choice([-1, 1])
                record["humidity"] = round(base_humidity + spike_amount, 2)
                _quality_issue_stats["sensor_spikes"] += 1
                
        elif rand < 0.07:  # 2% - Energy consumption spikes
            if "energy_consumption" in record and record["energy_consumption"] is not None and isinstance(record["energy_consumption"], (int, float)):
                base_energy = record["energy_consumption"]
                # Sudden spike: multiply by 3-5x (simulates equipment startup or malfunction)
                multiplier = random.uniform(3.0, 5.0)
                record["energy_consumption"] = round(base_energy * multiplier, 2)
                _quality_issue_stats["sensor_spikes"] += 1
                
        elif rand < 0.08:  # 1% - Battery level anomalies
            if "battery_level" in record and record["battery_level"] is not None and isinstance(record["battery_level"], (int, float)):
                base_battery = record["battery_level"]
                # Sudden drop or jump: change by 20-40% (simulates battery issues)
                spike_amount = random.uniform(20, 40) * random.choice([-1, 1])
                record["battery_level"] = max(0, min(100, round(base_battery + spike_amount)))
                _quality_issue_stats["sensor_spikes"] += 1
    
    return record


def generate_record(sensor: Dict[str, Any], sensors: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Generate a single IoT sensor record with potential data quality issues.
    
    Args:
        sensor: Sensor configuration dictionary
        sensors: List of all sensor configurations (for duplicate detection)
        
    Returns:
        Record dictionary with potential quality issues
    """
    global _message_count, _generated_records
    
    # Generate base valid record
    record = generate_base_record(sensor)
    
    # Introduce data quality issues (each function checks its own probability)
    record = introduce_null_values(record)
    record = introduce_duplicates(record, sensors)
    record = introduce_out_of_range_values(record)
    record = introduce_type_mismatches(record)
    record = introduce_schema_violations(record)
    record = introduce_formatting_issues(record)
    record = introduce_sensor_spikes(record)
    
    # Store for duplicate detection
    sensor_id = record.get("sensor_id")
    timestamp = record.get("timestamp")
    if sensor_id and timestamp:
        _generated_records[(sensor_id, timestamp)] = record.copy()
    
    # Add metadata
    record["message_id"] = str(uuid.uuid4())
    record["generated_at"] = datetime.now(ZoneInfo('Asia/Kolkata')).isoformat()
    record["data_quality_flag"] = "unknown"  # Will be set by validator
    
    _message_count += 1
    return record


def simulate_network_failure() -> bool:
    """
    Simulate random network failures (5% chance).
    
    Returns:
        True if network failure occurred, False otherwise
    """
    global _quality_issue_stats
    
    if random.random() < 0.05:  # 5% chance of network failure
        _quality_issue_stats["network_failure"] += 1
        return True
    return False


def get_statistics() -> Dict[str, Any]:
    """
    Get data quality issue statistics.
    
    Returns:
        Dictionary with statistics
    """
    global _message_count, _quality_issue_stats
    
    total = _message_count
    if total == 0:
        return {
            "total_messages": 0,
            "quality_stats": _quality_issue_stats.copy()
        }
    
    return {
        "total_messages": total,
        "quality_stats": _quality_issue_stats.copy(),
        "percentages": {
            "null_values": (_quality_issue_stats["null_values"] / total * 100) if total > 0 else 0,
            "duplicates": (_quality_issue_stats["duplicates"] / total * 100) if total > 0 else 0,
            "late_data": (_quality_issue_stats["late_data"] / total * 100) if total > 0 else 0,
            "out_of_range": (_quality_issue_stats["out_of_range"] / total * 100) if total > 0 else 0,
            "type_mismatch": (_quality_issue_stats["type_mismatch"] / total * 100) if total > 0 else 0,
            "schema_violation": (_quality_issue_stats["schema_violation"] / total * 100) if total > 0 else 0,
            "formatting_issue": (_quality_issue_stats["formatting_issue"] / total * 100) if total > 0 else 0,
            "network_failure": (_quality_issue_stats["network_failure"] / total * 100) if total > 0 else 0,
            "sensor_spikes": (_quality_issue_stats["sensor_spikes"] / total * 100) if total > 0 else 0,
        }
    }


def print_statistics():
    """Print data quality issue statistics to logger."""
    stats = get_statistics()
    total = stats["total_messages"]
    
    if total == 0:
        return
    
    logger.info("=" * 60)
    logger.info("Data Quality Issue Statistics")
    logger.info("=" * 60)
    logger.info(f"Total messages generated: {total}")
    
    for issue_type, count in stats["quality_stats"].items():
        pct = stats["percentages"][issue_type]
        logger.info(f"{issue_type}: {count} ({pct:.1f}%)")
    
    logger.info("=" * 60)


# ============================================================================
# Main Function
# ============================================================================

def main():
    """
    Main function to run the data generator.
    
    Generates data for 100 sensors, sending one record per sensor every 10 seconds.
    """
    # Initialize sensors
    sensors = initialize_sensors(100)
    
    logger.info("Starting IoT Data Generator...")
    logger.info(f"Generating data for {len(sensors)} sensors")
    logger.info("Sending data every 10 seconds per sensor")
    logger.info("Press Ctrl+C to stop")
    
    try:
        while True:
            for sensor in sensors:
                # Generate record
                record = generate_record(sensor, sensors)
                
                # Simulate network failure
                if simulate_network_failure():
                    logger.warning(f"Network failure simulated for sensor {sensor['sensor_id']}")
                    continue
                
                # Print record (will be sent to Kafka by producer)
                logger.debug(f"Generated record: {json.dumps(record, indent=2)}")
                
                # Wait 10 seconds before next sensor (distributed across all sensors)
                time.sleep(10 / len(sensors))
            
            # Print statistics every 100 messages
            if _message_count % 100 == 0:
                print_statistics()
                
    except KeyboardInterrupt:
        logger.info("\nStopping data generator...")
        print_statistics()
        logger.info("Generator stopped.")


if __name__ == "__main__":
    main()
