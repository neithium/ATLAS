import json
import os
from pathlib import Path

# Base directory for the V2 ingestion
BASE_DIR = Path(__file__).resolve().parent.parent.parent

# Load devices from the original JSON config
DEVICE_CONFIG_PATH = os.getenv("DEVICE_CONFIG_PATH", str(BASE_DIR / "device_configs.json"))

def load_device_registry():
    """Load the 50,000+ device registry from the JSON config file."""
    if not os.path.exists(DEVICE_CONFIG_PATH):
        raise FileNotFoundError(f"Device configuration not found at: {DEVICE_CONFIG_PATH}")
    
    with open(DEVICE_CONFIG_PATH, 'r') as f:
        data = json.load(f)
    
    # Transform to the flat list format (tuple) used by our V2 executors
    # [ (device_id, pcid, acid, srv_name, model, vendor, gen, loc_id, city, state, country, loc_name), ... ]
    registry = []
    for did, meta in data.items():
        registry.append((
            did,
            meta.get("platform_customer_id", "PLATCUST001"),
            meta.get("application_customer_id", "APPCUST0001"),
            meta.get("server_name", "srv-austin-0001"),
            meta.get("model", "ProLiant DL360"),
            meta.get("processor_vendor", "AMD"),
            meta.get("server_generation", "Gen11"),
            meta.get("location_id", "LOC-1"),
            meta.get("location_city", "Austin"),
            meta.get("location_state", "TX"),
            meta.get("location_country", "US"),
            meta.get("location_name", "DC-Austin-01")
        ))
    registry = registry[:50000]
    return registry

if __name__ == "__main__":
    reg = load_device_registry()
    print(f"Loaded registry with {len(reg):,} devices.")
    if reg:
        print(f"Sample device: {reg[0][0]} for {reg[0][1]}")
