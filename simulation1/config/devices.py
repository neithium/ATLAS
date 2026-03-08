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

DEVICES: dict[str, dict] = {
    "DEV-SERVER-01": {
        # ── IPMI connection ────────────────────────────────────────────
        "ipmi_host"    : os.getenv("IPMI_HOST_01", "192.168.1.11"),
        "ipmi_user"    : os.getenv("IPMI_USER_01", "admin"),
        "ipmi_password": os.getenv("IPMI_PASS_01", "admin"),
        "ipmi_port"    : 623,

        # ── metadata (written into every JSON response) ────────────────
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
    "DEV-SERVER-02": {
        "ipmi_host"    : os.getenv("IPMI_HOST_02", "192.168.1.12"),
        "ipmi_user"    : os.getenv("IPMI_USER_02", "admin"),
        "ipmi_password": os.getenv("IPMI_PASS_02", "admin"),
        "ipmi_port"    : 623,

        # ── metadata (written into every JSON response) ────────────────
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
    "DEV-SERVER-03": {
        "ipmi_host"    : os.getenv("IPMI_HOST_03", "192.168.1.13"),
        "ipmi_user"    : os.getenv("IPMI_USER_03", "admin"),
        "ipmi_password": os.getenv("IPMI_PASS_03", "admin"),
        "ipmi_port"    : 623,

        # ── metadata (written into every JSON response) ────────────────
        "server_name"            : "srv-austin-003",
        "model"                  : "ProLiant DL560 Gen11",
        "processor_vendor"       : "Intel",
        "server_generation"      : "Gen11",
        "location_id"            : "LOC-001",
        "location_city"          : "Austin",
        "location_state"         : "TX",
        "location_country"       : "US",
        "location_name"          : "DC-Austin-01",
        "platform_customer_id"   : "PLATCUST001",
        "application_customer_id": "APPCUST00003",
    },
    "DEV-SERVER-04": {
        # ── IPMI connection ────────────────────────────────────────────
        "ipmi_host"    : os.getenv("IPMI_HOST_04", "192.168.1.14"),
        "ipmi_user"    : os.getenv("IPMI_USER_04", "admin"),
        "ipmi_password": os.getenv("IPMI_PASS_04", "admin"),
        "ipmi_port"    : 623,

        # ── metadata (written into every JSON response) ────────────────
        "server_name"            : "srv-austin-004",
        "model"                  : "ProLiant DL360 Gen11",
        "processor_vendor"       : "Intel",
        "server_generation"      : "Gen11",
        "location_id"            : "LOC-001",
        "location_city"          : "Austin",
        "location_state"         : "TX",
        "location_country"       : "US",
        "location_name"          : "DC-Austin-01",
        "platform_customer_id"   : "PLATCUST001",
        "application_customer_id": "APPCUST00004",
    },
}

# ── Validate all devices on startup ───────────────────────────────────────────

for device_id, config in DEVICES.items():
    try:
        _validate_device_config(device_id, config)
    except ValueError as e:
        log.error(f"Device config validation failed: {e}")
        raise

# ── Redis config ───────────────────────────────────────────────────────────────
REDIS_HOST     = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT     = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB       = int(os.getenv("REDIS_DB",   0))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)

# ── Poller config ──────────────────────────────────────────────────────────────
POLL_INTERVAL_SECONDS = 300          # 5 minutes
READINGS_PER_HOUR     = 12
TOTAL_READINGS        = 2016         # 12 × 24 × 7
FRESH_READINGS        = 12           # 1 hour of new data per response
TTL_SECONDS           = 7 * 24 * 3600   # Redis key expiry = 7 days

