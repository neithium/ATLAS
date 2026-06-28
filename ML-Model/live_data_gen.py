import os
import uuid
import time
import logging
import argparse
import numpy as np
import pandas as pd
from datetime import datetime, timezone

# Import the core logic and schemas directly from the original data_generator.py
from data_generator import (
    generate_server_inventory, 
    ROLES, 
    LOC_TEMP_RANGES, 
    ROLE_COVARIATES, 
    build_cpu_profile
)

# ==============================================================================
# Live Telemetry Generator (Real-Time Inference Stream)
# ==============================================================================
# Generates a single hourly snapshot for all servers WITHOUT using the external 
# JSON registry. It generates the exact same servers used during training by
# importing generate_server_inventory with the identical seed.
# Ensures the exact 31-column schema output matching data_generator.py.
# ==============================================================================

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("LiveGen")

def generate_current_hour(inventory_df: pd.DataFrame, is_anomaly_test: bool = False, anomaly_rate: float = 0.03) -> pd.DataFrame:
    num_servers = len(inventory_df)
    now = datetime.now(timezone.utc)
    current_hour = now.hour
    
    logger.info(f"Generating live snapshot for {num_servers} servers at {now.strftime('%Y-%m-%d %H:00:00 UTC')}")
    
    rng = np.random.default_rng()
    
    cpu_util = np.zeros(num_servers)
    mem_util = np.zeros(num_servers)
    disk_util = np.zeros(num_servers)
    net_util = np.zeros(num_servers)
    
    # 1. Base Profiles
    for role in ROLES:
        server_mask = (inventory_df["tags"].str.contains(role)).values
        n_role = server_mask.sum()
        if n_role == 0: continue
        
        prof_cpu = build_cpu_profile(role)
        base_cpu = prof_cpu[current_hour]
        
        cpu_util[server_mask] = base_cpu + rng.normal(0, 3.0, n_role)
        mem_b, mem_s, disk_b, disk_s, net_b, net_s = ROLE_COVARIATES[role]
        
        mem_util[server_mask] = mem_b + (cpu_util[server_mask] * mem_s) + rng.normal(0, 1.0, n_role)
        disk_util[server_mask] = disk_b + (cpu_util[server_mask] * disk_s) + rng.normal(0, 2.0, n_role)
        net_util[server_mask] = net_b + (cpu_util[server_mask] * net_s) + rng.normal(0, 10.0, n_role)

    cpu_util = np.clip(cpu_util, 5, 95)
    mem_util = np.clip(mem_util, 10, 95)
    disk_util = np.clip(disk_util, 0, 95)
    net_util = np.clip(net_util, 0, 10000)

    # 2. Temperature based on location
    amb_temp = np.zeros(num_servers)
    amb_cycle = np.sin((current_hour - 8) * np.pi / 12)
    
    for loc, (t_min, t_max) in LOC_TEMP_RANGES.items():
        if loc not in inventory_df["location_name"].values:
            continue
            
        loc_mask = (inventory_df["location_name"] == loc).values
        n_loc = loc_mask.sum()
        if n_loc == 0: continue
        
        t_mid = (t_max + t_min) / 2.0
        t_amp = (t_max - t_min) / 2.0
        base_temp = t_mid + (amb_cycle * t_amp)
        
        amb_temp[loc_mask] = base_temp + rng.normal(0, 0.5, n_loc)
        
    amb_temp = np.clip(amb_temp, 15, 45)

    # 3. GPU Utilization
    gpu_util = np.zeros(num_servers)
    is_ai = (inventory_df["tags"].str.contains("AI")).values
    if is_ai.any():
        gpu_util[is_ai] = -100.0 + (cpu_util[is_ai] * 2.1) + rng.normal(0, 5.0, is_ai.sum())
    gpu_util = np.clip(gpu_util, 0, 95)

    # 4. Power Derivations
    socket_count = inventory_df["socket_count"].values
    mem_cap = inventory_df["memory_capacity_gb"].values
    gen = inventory_df["server_generation"].values
    
    gen_penalty = np.zeros(num_servers)
    gen_penalty[gen == "Gen10"] = 40.0
    gen_penalty[gen == "Gen11"] = 15.0
    gen_penalty[gen == "Gen12"] = 0.0
    
    base_pwr = 80 + (socket_count * 30) + (mem_cap * 0.2) + gen_penalty
    
    avg_power = base_pwr + (cpu_util * 3.5) + (gpu_util * 2.5) + rng.normal(0, 4.0, num_servers)
    avg_power = np.clip(avg_power, 120, 800)
    
    max_power = avg_power * rng.uniform(1.02, 1.05, size=num_servers)
    min_power = avg_power * rng.uniform(0.95, 0.98, size=num_servers)

    cpu_temp = 20.0 + (avg_power * 0.08) + ((amb_temp - 20) * 1.0) + rng.normal(0, 1.0, num_servers)
    cpu_temp = np.clip(cpu_temp, 35, 95)
    
    fan_speed = -1000.0 + (cpu_temp * 100.0) + rng.normal(0, 150.0, num_servers)
    fan_speed = np.clip(fan_speed, 1500, 12000)

    # 5. Live Anomaly Injection
    is_anomaly = np.zeros(num_servers, dtype=int)
    
    if is_anomaly_test:
        logger.warning(f"⚠️ INJECTING LIVE ANOMALIES FOR ML INFERENCE DETECTION (Rate: {anomaly_rate*100}%)!")
        n_anom_servers = int(num_servers * rng.uniform(anomaly_rate * 0.8, anomaly_rate * 1.2))
        anom_servers = rng.choice(num_servers, n_anom_servers, replace=False)
        
        types = [
            "thermal_failure", "memory_leak", "crypto_miner", 
            "network_exfil", "disk_failure", "psu_fault", "sensor_failure",
            "runaway_process", "thread_deadlock"
        ]
        
        for s in anom_servers:
            role = inventory_df["tags"].iloc[s]
            anom_type = rng.choice(types + ["gpu_overload"] if role == "AI" else types)
            is_anomaly[s] = 1
            
            if anom_type == "thermal_failure":
                fan_speed[s] = rng.uniform(0, 1000)
                cpu_temp[s] = rng.uniform(85, 105)
            elif anom_type == "memory_leak":
                mem_util[s] = rng.uniform(95.0, 100.0)
            elif anom_type == "crypto_miner":
                cpu_util[s] = rng.uniform(96, 100)
                max_hw_pwr = base_pwr[s] + (100 * 3.5) + (100 * 2.5 if "AI" in role else 0)
                avg_power[s] = max_hw_pwr * rng.uniform(1.1, 1.3)
                max_power[s] = avg_power[s] * 1.01
                min_power[s] = avg_power[s] * 0.99
                cpu_temp[s] = rng.uniform(90, 100)
                fan_speed[s] = rng.uniform(9000, 12000)
            elif anom_type == "network_exfil":
                net_util[s] = rng.uniform(6000, 10000)
                if role in ["Database", "ClickHouse"]:
                    disk_util[s] = rng.uniform(85, 95)
            elif anom_type == "disk_failure":
                disk_util[s] = rng.uniform(98.0, 100.0)
            elif anom_type == "psu_fault":
                avg_power[s] += rng.uniform(300, 500)
                max_power[s] += 400
            elif anom_type == "sensor_failure":
                cpu_util[s] = 98.0
                cpu_temp[s] = 38.0
                avg_power[s] = 600.0
                fan_speed[s] = 3000.0
            elif anom_type == "runaway_process":
                cpu_util[s] = np.clip(cpu_util[s] + rng.uniform(30.0, 50.0), 0, 100)
                avg_power[s] += rng.uniform(40, 80)
                cpu_temp[s] += rng.uniform(5, 12)
            elif anom_type == "thread_deadlock":
                net_util[s] = 0.0
                disk_util[s] = 0.0
                cpu_util[s] = rng.uniform(5.0, 10.0)
                avg_power[s] = base_pwr[s] + 15.0
            elif anom_type == "gpu_overload":
                gpu_util[s] = rng.uniform(96, 100)
                cpu_util[s] = rng.uniform(85, 95)
                hw_max = base_pwr[s] + (100 * 3.5) + (100 * 2.5)
                avg_power[s] = hw_max * rng.uniform(0.9, 1.05)
                cpu_temp[s] = rng.uniform(92, 105)
                fan_speed[s] = rng.uniform(7500, 10500)

    # EXACT SCHEMA MATCH WITH data_generator.py
    df_dict = {
        "report_id": [str(uuid.uuid4()) for _ in range(num_servers)],
        "device_id": inventory_df["device_id"].values,
        "server_name": inventory_df["server_name"].values,
        "application_customer_id": inventory_df["application_customer_id"].values,
        "platform_customer_id": inventory_df["platform_customer_id"].values,
        "tags": inventory_df["tags"].values,
        "location_name": inventory_df["location_name"].values,
        "location_city": inventory_df["location_city"].values,
        "location_state": inventory_df["location_state"].values,
        "location_country": inventory_df["location_country"].values,
        "processor_vendor": inventory_df["processor_vendor"].values,
        "server_generation": inventory_df["server_generation"].values,
        "cpu_inventory": inventory_df["cpu_inventory"].values,
        "memory_inventory": inventory_df["memory_inventory"].values,
        "pcie_devices_count": inventory_df["pcie_devices_count"].values,
        "socket_count": inventory_df["socket_count"].values,
        "last_maintenance_date": inventory_df["last_maintenance_date"].values,
        "last_boot_time": inventory_df["last_boot_time"].values,
        "metric_time": [now.strftime("%Y-%m-%dT%H:00:00Z")] * num_servers,
        "avg_metric_value": avg_power.round(2),
        "max_metric_value": max_power.round(2),
        "min_metric_value": min_power.round(2),
        "cpu_utilization": cpu_util.round(1),
        "memory_utilization": mem_util.round(1),
        "disk_utilization": disk_util.round(1),
        "network_throughput": net_util.round(1),
        "cpu_temperature": cpu_temp.round(1),
        "amb_temp": amb_temp.round(1),
        "fan_speed_rpm": fan_speed.round(0).astype(int),
        "gpu_utilization": gpu_util.round(1),
        "is_anomaly": is_anomaly
    }
    
    return pd.DataFrame(df_dict)


def main():
    parser = argparse.ArgumentParser(description="Live Server Telemetry Generator for ML Inference")
    parser.add_argument("--servers", type=int, default=1000, help="Number of servers (must match data_generator.py)")
    parser.add_argument("--seed", type=int, default=42, help="Seed used during data_generator.py to ensure identical servers")
    parser.add_argument("--outdir", type=str, default="telemetry-data/live", help="Directory to save live snapshots")
    parser.add_argument("--loop", action="store_true", help="Run continuously in a loop")
    parser.add_argument("--interval", type=int, default=3600, help="Seconds between generations (default 1 hour)")
    parser.add_argument("--anomalies", action="store_true", help="Randomly inject anomalies for inference testing")
    parser.add_argument("--anomaly-rate", type=float, default=0.03, help="Percentage of servers to infect (e.g. 0.15 for 15%)")
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    
    # Use data_generator.py's internal function to magically reconstruct the EXACT same servers!
    logger.info(f"Reconstructing exact hardware profiles for {args.servers} servers using seed {args.seed}...")
    inventory_df = generate_server_inventory(args.servers, args.seed)

    while True:
        now = datetime.now()
        df = generate_current_hour(inventory_df, is_anomaly_test=args.anomalies, anomaly_rate=args.anomaly_rate)
        
        filename = os.path.join(args.outdir, f"inference_batch_{now.strftime('%Y%m%d_%H%M%S')}.parquet")
        df.to_parquet(filename, engine='pyarrow', compression='snappy', index=False)
        logger.info(f"✅ Saved live snapshot to {filename} ({len(df)} rows)")
        
        if not args.loop:
            break
            
        logger.info(f"Sleeping for {args.interval} seconds until next live snapshot...")
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
