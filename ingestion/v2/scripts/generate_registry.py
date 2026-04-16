import orjson
import os
import argparse

def generate_registry(scale=80000, output_path="device_configs.json"):
    """
    PowerPulse V3 Registry Bootstrapper (Schema Aligned):
    Generates a 100% mirrored 80k device registry matching input_schema.py.
    """
    print(f"🛰️  Bootstrapping Schema-Aligned Registry ({scale:,} devices)...")
    
    devices = {}
    
    # Customer Hierarchy Constants
    PLATCUST_COUNT = 5 
    APPCUST_PER_PLAT = 10 
    
    # Calculate intervals to avoid division by zero
    plat_interval = max(1, scale // PLATCUST_COUNT)
    app_interval = max(1, scale // (PLATCUST_COUNT * APPCUST_PER_PLAT))
    
    # Regional DC Locations for Regional Diversity
    LOCATIONS = [
        {"city": "Bangalore", "state": "Karnataka", "country": "India", "name": "Atlas-DC-01"},
        {"city": "Mumbai", "state": "Maharashtra", "country": "India", "name": "Atlas-DC-02"},
        {"city": "Chennai", "state": "Tamil Nadu", "country": "India", "name": "Atlas-DC-03"},
        {"city": "Hyderabad", "state": "Telangana", "country": "India", "name": "Atlas-DC-04"},
        {"city": "Delhi", "state": "NCR", "country": "India", "name": "Atlas-DC-05"}
    ]
    
    for i in range(scale):
        dev_id = f"PLAT1-DEV-{i//1000:04}-{i%1000:03}"
        
        # Calculate Hierarchical PCID/ACID
        p_idx = (i // plat_interval) + 1
        a_idx = (i // app_interval) % APPCUST_PER_PLAT + 1
        
        pcid = f"PLATCUST{p_idx:03}"
        acid = f"APPCUST{a_idx:04}"
        
        # Select Geographic Location (Rotation)
        loc = LOCATIONS[i % len(LOCATIONS)]
        
        devices[dev_id] = {
            # Hierarchical Mapping
            "platform_customer_id": pcid,
            "application_customer_id": acid,
            "device_id": dev_id,
            
            # Asset Metadata (Matches Input Schema)
            "server_name": f"host-{i:06}",
            "model": "PowerEdge R750",
            "processor_vendor": "Intel",
            "server_generation": "15G",
            "tags": "production,critical",
            "status": True,
            "report_type": "telemetry_live",
            "metric_type": "power_metrics",
            "error_reason": "",
            
            # Geographical Metadata
            "location_id": f"LOC-{i % len(LOCATIONS) + 1:02}",
            "location_name": loc["name"],
            "location_city": loc["city"],
            "location_state": loc["state"],
            "location_country": loc["country"],
            
            # Inventory Metadata (Nested Block)
            "inventory_data": {
                "cpu_count": 2,
                "socket_count": 2,
                "cpu_inventory": [
                    {"model": "Intel Xeon Platinum 8380", "speed": 2300, "total_cores": 40}
                ],
                "memory_inventory": [
                    {"memory_size": 32, "operating_freq": 3200, "memory_device_type": "DDR4"}
                ]
            }
        }
        
        if i % 10000 == 0 and i > 0:
            print(f"  - Synchronized {i:,} entries...")

    with open(output_path, "wb") as f:
        f.write(orjson.dumps(devices))
        
    print(f" Registry Synchronized: {output_path} ({os.path.getsize(output_path) / 1024 / 1024:.2f} MB)")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--scale", type=int, default=80000)
    parser.add_argument("--output", type=str, default="device_configs.json")
    args = parser.parse_args()
    
    generate_registry(scale=args.scale, output_path=args.output)
