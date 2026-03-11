"""
config/devices.py
-----------------
Central registry of all servers.
Edit this file to add / remove devices.
IPMI credentials should come from env vars in production.
"""

import os
import logging

log = logging.getLogger(__name__)

# ── Validation functions ───────────────────────────────────────────────────────

def _validate_customer_id(value: str, field_name: str) -> str:
    """Validate customer ID: must not be empty and minimum 10 characters."""
    if not value or not value.strip():
        raise ValueError(f"{field_name} cannot be empty")
    if len(value) < 10:
        raise ValueError(f"{field_name} must be at least 10 characters (got {len(value)}: '{value}')")
    return value


def _validate_device_config(device_id: str, config: dict):
    """Validate required fields for a device."""
    required_fields = [
        "server_name", "model", "processor_vendor", "server_generation",
        "location_id", "location_city", "location_state", "location_country",
        "location_name", "platform_customer_id", "application_customer_id",
    ]
    for field in required_fields:
        if field not in config or not config[field]:
            raise ValueError(f"Device {device_id}: missing required field '{field}'")
    
    # Validate customer IDs (non-empty and minimum 10 chars)
    _validate_customer_id(config["platform_customer_id"], "platform_customer_id")
    _validate_customer_id(config["application_customer_id"], "application_customer_id")


# ── Device registry ───────────────────────────────────────────────────────────

# Platform prefixes for organization:
# PLAT1 = Platform 1, PLAT2 = Platform 2, PLAT3 = Platform 3

DEVICES: dict[str, dict] = {
    # ── PLATFORM 1 (PLAT1) ───────────────────────────────────────────────
    "PLAT1-DEV-001": {
        "ipmi_host"    : os.getenv("IPMI_HOST_PLAT1_001", "192.168.1.11"),
        "ipmi_user"    : os.getenv("IPMI_USER_PLAT1_001", "admin"),
        "ipmi_password": os.getenv("IPMI_PASS_PLAT1_001", "admin"),
        "ipmi_port"    : 623,

        "server_name"            : "srv-austin-001",
        "model"                  : "ProLiant DL360 Gen11",
        "processor_vendor"       : "Intel",
        "server_generation"      : "Gen11",
        "location_id"            : "LOC-001",
        "location_city"          : "Austin",
        "location_state"         : "TX",
        "location_country"       : "US",
        "location_name"          : "DC-Austin-01",
        "platform_customer_id"   : "PLATCUST001",
        "application_customer_id": "APPCUST00001",
    },
    "PLAT1-DEV-002": {
        "ipmi_host"    : os.getenv("IPMI_HOST_PLAT1_002", "192.168.1.12"),
        "ipmi_user"    : os.getenv("IPMI_USER_PLAT1_002", "admin"),
        "ipmi_password": os.getenv("IPMI_PASS_PLAT1_002", "admin"),
        "ipmi_port"    : 623,

        "server_name"            : "srv-austin-002",
        "model"                  : "ProLiant DL380 Gen11",
        "processor_vendor"       : "AMD",
        "server_generation"      : "Gen11",
        "location_id"            : "LOC-001",
        "location_city"          : "Austin",
        "location_state"         : "TX",
        "location_country"       : "US",
        "location_name"          : "DC-Austin-01",
        "platform_customer_id"   : "PLATCUST001",
        "application_customer_id": "APPCUST00002",
    },

    # ── PLATFORM 2 (PLAT2) ───────────────────────────────────────────────
    "PLAT2-DEV-001": {
        "ipmi_host"    : os.getenv("IPMI_HOST_PLAT2_001", "192.168.2.11"),
        "ipmi_user"    : os.getenv("IPMI_USER_PLAT2_001", "admin"),
        "ipmi_password": os.getenv("IPMI_PASS_PLAT2_001", "admin"),
        "ipmi_port"    : 623,

        "server_name"            : "srv-denver-001",
        "model"                  : "PowerEdge R750",
        "processor_vendor"       : "Intel",
        "server_generation"      : "Gen15",
        "location_id"            : "LOC-002",
        "location_city"          : "Denver",
        "location_state"         : "CO",
        "location_country"       : "US",
        "location_name"          : "DC-Denver-01",
        "platform_customer_id"   : "PLATCUST002",
        "application_customer_id": "APPCUST10001",
    },
    "PLAT2-DEV-002": {
        "ipmi_host"    : os.getenv("IPMI_HOST_PLAT2_002", "192.168.2.12"),
        "ipmi_user"    : os.getenv("IPMI_USER_PLAT2_002", "admin"),
        "ipmi_password": os.getenv("IPMI_PASS_PLAT2_002", "admin"),
        "ipmi_port"    : 623,

        "server_name"            : "srv-denver-002",
        "model"                  : "PowerEdge R650",
        "processor_vendor"       : "AMD",
        "server_generation"      : "Gen15",
        "location_id"            : "LOC-002",
        "location_city"          : "Denver",
        "location_state"         : "CO",
        "location_country"       : "US",
        "location_name"          : "DC-Denver-01",
        "platform_customer_id"   : "PLATCUST002",
        "application_customer_id": "APPCUST10002",
    },

    # ── PLATFORM 3 (PLAT3) ───────────────────────────────────────────────
    "PLAT3-DEV-001": {
        "ipmi_host"    : os.getenv("IPMI_HOST_PLAT3_001", "192.168.3.11"),
        "ipmi_user"    : os.getenv("IPMI_USER_PLAT3_001", "admin"),
        "ipmi_password": os.getenv("IPMI_PASS_PLAT3_001", "admin"),
        "ipmi_port"    : 623,

        "server_name"            : "srv-phoenix-001",
        "model"                  : "UCS C220 M6",
        "processor_vendor"       : "Intel",
        "server_generation"      : "Gen5",
        "location_id"            : "LOC-003",
        "location_city"          : "Phoenix",
        "location_state"         : "AZ",
        "location_country"       : "US",
        "location_name"          : "DC-Phoenix-01",
        "platform_customer_id"   : "PLATCUST003",
        "application_customer_id": "APPCUST20001",
    },
    "PLAT3-DEV-002": {
        "ipmi_host"    : os.getenv("IPMI_HOST_PLAT3_002", "192.168.3.12"),
        "ipmi_user"    : os.getenv("IPMI_USER_PLAT3_002", "admin"),
        "ipmi_password": os.getenv("IPMI_PASS_PLAT3_002", "admin"),
        "ipmi_port"    : 623,

        "server_name"            : "srv-phoenix-002",
        "model"                  : "UCS C240 M6",
        "processor_vendor"       : "AMD",
        "server_generation"      : "Gen5",
        "location_id"            : "LOC-003",
        "location_city"          : "Phoenix",
        "location_state"         : "AZ",
        "location_country"       : "US",
        "location_name"          : "DC-Phoenix-01",
        "platform_customer_id"   : "PLATCUST0",
        "application_customer_id": "APPCUST20002",
    },
}

# ── Validate all devices on startup ───────────────────────────────────────────
# Skip invalid devices instead of crashing the entire app

_valid_devices = {}
for device_id, config in DEVICES.items():
    try:
        _validate_device_config(device_id, config)
        _valid_devices[device_id] = config
    except ValueError as e:
        log.error(f"Device config validation failed for {device_id}: {e}")
        log.warning(f"Skipping device {device_id} due to validation error")

# Replace DEVICES with only validated devices
DEVICES = _valid_devices
log.info(f"Validated {len(DEVICES)} device(s) from configuration")

# ── Redis config ───────────────────────────────────────────────────────────────
REDIS_HOST     = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT     = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB       = int(os.getenv("REDIS_DB",   0))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)

# ── MinIO config ──────────────────────────────────────────────────────────────
MINIO_HOST      = os.getenv("MINIO_HOST", "localhost")
MINIO_PORT      = int(os.getenv("MINIO_PORT", 9000))
MINIO_BUCKET    = os.getenv("MINIO_BUCKET", "power-readings")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE    = os.getenv("MINIO_SECURE", "false").lower() == "true"

# ── Poller config ──────────────────────────────────────────────────────────────
POLL_INTERVAL_SECONDS = 300          # 5 minutes
READINGS_PER_HOUR     = 12
TOTAL_READINGS        = 2016         # 12 × 24 × 7
FRESH_READINGS        = 12           # 1 hour of new data per response (Redis)
HISTORICAL_READINGS  = 1728         # 12 × 24 × 6 = 6 days of historical data (MinIO)
REDIS_READINGS        = 288          # 12 × 24 = 24 hours of recent data in Redis
TTL_SECONDS           = 24 * 3600    # Redis key expiry = 24 hours (not 7 days!)
MINIO_RETENTION_DAYS  = 6            # Keep 6 days of historical data in MinIO
