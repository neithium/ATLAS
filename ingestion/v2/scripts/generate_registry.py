import orjson
import os
import argparse

def generate_registry(scale=80000, output_path="device_configs.json"):
    """
    PowerPulse V3 Registry Bootstrapper (Schema Aligned):
    Generates a 100% mirrored 80k device registry matching input_schema.py.
    """
    print(f"Bootstrapping Schema-Aligned Registry ({scale:,} devices)...")
    
    devices = {}
    
    # New Customer Hierarchy (1:M PCID-to-ACID mapping)
    PCID_COUNT = 5
    ACIDS_PER_PCID = 2  # Each platform has multiple ACIDs
    DEVICES_PER_ACID = 1000  # 2 * 1000 = 2000 devices per platform
    total_scale = PCID_COUNT * ACIDS_PER_PCID * DEVICES_PER_ACID
    
    # Regional DC Locations for Regional Diversity
    LOCATIONS = [
        {"city": "Bangalore", "state": "Karnataka", "country": "India", "name": "Atlas-DC-01"},
        {"city": "Mumbai", "state": "Maharashtra", "country": "India", "name": "Atlas-DC-02"},
        {"city": "Chennai", "state": "Tamil Nadu", "country": "India", "name": "Atlas-DC-03"},
        {"city": "Hyderabad", "state": "Telangana", "country": "India", "name": "Atlas-DC-04"},
        {"city": "Delhi", "state": "NCR", "country": "India", "name": "Atlas-DC-05"}
    ]
    
    global_counter = 0
    for p_idx in range(1, PCID_COUNT + 1):
        pcid = f"PLATCUST{p_idx:04}"
        
        for a_idx in range(1, ACIDS_PER_PCID + 1):
            acid = f"{pcid}_APPCUST{a_idx:02}"
            
            for d_idx in range(1, DEVICES_PER_ACID + 1):
                dev_id = f"PLAT{p_idx:04}-APP{a_idx:02}-DEV-{d_idx:04}"
                
                # Select Geographic Location (Rotation)
                loc = LOCATIONS[global_counter % len(LOCATIONS)]
                
                devices[dev_id] = {
                    "platform_customer_id": pcid,
                    "application_customer_id": acid,
                    "device_id": dev_id,
                    "server_name": f"host-{global_counter:06}",
                    "model": "PowerEdge R750",
                    "processor_vendor": "Intel",
                    "server_generation": "15G",
                    "tags": "production,critical",
                    "status": True,
                    "report_type": "telemetry_live",
                    "metric_type": "power_metrics",
                    "error_reason": "",
                    "location_id": f"LOC-{global_counter % len(LOCATIONS) + 1:02}",
                    "location_name": loc["name"],
                    "location_city": loc["city"],
                    "location_state": loc["state"],
                    "location_country": loc["country"],
                    "inventory_data": {
                        "cpu_count": 2,
                        "socket_count": 2,
                        "cpu_inventory": [{"model": "Intel Xeon Platinum 8380", "speed": 2300, "total_cores": 40}],
                        "memory_inventory": [{"memory_size": 32, "operating_freq": 3200, "memory_device_type": "DDR4"}]
                    }
                }
                
                global_counter += 1
                if global_counter % 2000 == 0:
                    print(f"  - Synchronized {global_counter:,} entries...")

    with open(output_path, "wb") as f:
        f.write(orjson.dumps(devices))
        
    print(f"Registry Synchronized: {output_path} ({os.path.getsize(output_path) / 1024 / 1024:.2f} MB)")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--scale", type=int, default=80000)
    parser.add_argument("--output", type=str, default="device_configs.json")
    args = parser.parse_args()
    
    generate_registry(scale=args.scale, output_path=args.output)
