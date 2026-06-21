import orjson
import os
import argparse
import random

def generate_registry(pcids=5, acids=2, devices_per_acid=1000, output_path="device_configs.json"):
    """
    PowerPulse V3 Registry Bootstrapper (Schema Aligned):
    Generates a 100% mirrored device registry matching input_schema.py.
    """
    devices = {}
    total_scale = pcids * acids * devices_per_acid
    print(f"Bootstrapping Schema-Aligned Registry ({total_scale:,} devices)...")
    print(f"Hierarchy: {pcids} Platforms | {acids} ACIDs per Platform | {devices_per_acid} Devices per ACID")
    
    # Regional DC Locations for Regional Diversity
    LOCATIONS = [
        {"city": "Bangalore", "state": "Karnataka", "country": "India", "name": "Atlas-DC-01"},
        {"city": "Mumbai", "state": "Maharashtra", "country": "India", "name": "Atlas-DC-02"},
        {"city": "Chennai", "state": "Tamil Nadu", "country": "India", "name": "Atlas-DC-03"},
        {"city": "Hyderabad", "state": "Telangana", "country": "India", "name": "Atlas-DC-04"},
        {"city": "Delhi", "state": "NCR", "country": "India", "name": "Atlas-DC-05"}
    ]
    
    global_counter = 0
    for p_idx in range(1, pcids + 1):
        pcid = f"PLATCUST{p_idx:04}"
        
        for a_idx in range(1, acids + 1):
            acid = f"{pcid}_APPCUST{a_idx:02}"
            
            for d_idx in range(1, devices_per_acid + 1):
                dev_id = f"PLAT{p_idx:04}-APP{a_idx:02}-DEV-{d_idx:04}"
                
                # Hardware Pools for diversity
                MODELS = [
                    ("PowerEdge R750", "Intel", "15G", {"model": "Intel Xeon Platinum 8380", "speed": 2300, "total_cores": 40}),
                    ("ProLiant DL385", "AMD", "Gen10", {"model": "AMD EPYC 7763", "speed": 2450, "total_cores": 64}),
                    ("ThinkSystem SR650", "Intel", "V2", {"model": "Intel Xeon Gold 6330", "speed": 2000, "total_cores": 28}),
                    ("Cisco UCS C240", "AMD", "M6", {"model": "AMD EPYC 7313", "speed": 3000, "total_cores": 16})
                ]
                
                RAM_OPTIONS = [
                    {"memory_size": 32, "operating_freq": 3200, "memory_device_type": "DDR4"},
                    {"memory_size": 64, "operating_freq": 3200, "memory_device_type": "DDR4"},
                    {"memory_size": 128, "operating_freq": 4800, "memory_device_type": "DDR5"},
                    {"memory_size": 256, "operating_freq": 4800, "memory_device_type": "DDR5"}
                ]

                # Select Hardware
                hw_model, hw_vendor, hw_gen, hw_cpu = random.choice(MODELS)
                hw_ram = random.choice(RAM_OPTIONS)
                
                # Select Geographic Location (Rotation)
                loc = LOCATIONS[global_counter % len(LOCATIONS)]
                
                devices[dev_id] = {
                    "platform_customer_id": pcid,
                    "application_customer_id": acid,
                    "device_id": dev_id,
                    "server_name": f"host-{global_counter:06}",
                    "model": hw_model,
                    "processor_vendor": hw_vendor,
                    "server_generation": hw_gen,
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
                        "cpu_count": random.choice([1, 2, 4]),
                        "socket_count": random.choice([1, 2]),
                        "cpu_inventory": [hw_cpu],
                        "memory_inventory": [hw_ram]
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
    parser.add_argument("--pcids", type=int, default=5, help="Number of platform customers")
    parser.add_argument("--acids", type=int, default=2, help="Number of application customers per platform")
    parser.add_argument("--devices", type=int, default=1000, help="Number of devices per application")
    parser.add_argument("--scale", type=int, help="Total target scale size (overrides devices per application)")
    parser.add_argument("--output", type=str, default="device_configs.json")
    args = parser.parse_args()
    
    devices_per_acid = args.devices
    if args.scale is not None:
        devices_per_acid = max(1, args.scale // (args.pcids * args.acids))
    
    generate_registry(pcids=args.pcids, acids=args.acids, devices_per_acid=devices_per_acid, output_path=args.output)
