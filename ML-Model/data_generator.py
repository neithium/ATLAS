import os
import uuid
import logging
import argparse
import numpy as np
import pandas as pd
import random
from datetime import datetime, timedelta, timezone

# ==============================================================================
# AI Server Health Monitoring - Synthetic Telemetry Generator
# ==============================================================================
# Generates realistic telemetry data using a strict Causal Chain:
# Workload/Hour -> CPU Util -> Network/Disk/Power -> CPU Temperature
# Includes hardware-aware, role-aware progressive temporal anomalies 
# specifically injected into the test split.
# ==============================================================================

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("TelemetryGenerator")

DEFAULT_SERVERS = 1000
DEFAULT_DAYS = 30
DEFAULT_OUTPUT_DIR = "telemetry-data"
DEFAULT_SEED = 42

ROLES = ["UI", "Database", "Spark", "Kafka", "Redis", "ClickHouse", "AI", "Monitoring", "Backup"]
LOCATIONS = ["Bangalore_DC", "Hyderabad_DC", "Chennai_DC", "Mumbai_DC", "Delhi_DC"]
VENDORS = ["Intel", "AMD"]
GENERATIONS = ["Gen10", "Gen11", "Gen12"]
MEM_CAPACITIES = [64, 128, 256, 512]
SOCKETS = [1, 2]

LOCATION_DETAILS = {
    "Bangalore_DC": ("Bangalore", "Karnataka", "India"),
    "Hyderabad_DC": ("Hyderabad", "Telangana", "India"),
    "Chennai_DC": ("Chennai", "Tamil Nadu", "India"),
    "Mumbai_DC": ("Mumbai", "Maharashtra", "India"),
    "Delhi_DC": ("New Delhi", "Delhi", "India")
}

CPU_MODELS = {
    "Intel": ["Intel Xeon Gold 6230", "Intel Xeon Platinum 8280", "Intel Xeon Silver 4214"],
    "AMD": ["AMD EPYC 7742", "AMD EPYC 7502", "AMD EPYC 7302"]
}

LOC_TEMP_RANGES = {
    "Bangalore_DC": (20, 28),
    "Delhi_DC": (24, 36),
    "Chennai_DC": (25, 35),
    "Mumbai_DC": (22, 32),
    "Hyderabad_DC": (22, 32)
}

# Define only the BASE CPU profile (Workload State) per hour
def build_cpu_profile(role: str):
    cpu = np.full(24, 15.0) # Default Idle
    
    if role == "UI":
        cpu[9:21] = 55.0      # Business hours (30-80% with noise)
        cpu[0:9] = 15.0; cpu[21:24] = 15.0
    elif role == "Database":
        cpu[:] = 45.0         # Stable (30-60%)
    elif role == "Spark":
        cpu[0:5] = 82.0       # Night jobs (70-95%)
        cpu[5:24] = 15.0      # Idle
    elif role == "Kafka":
        cpu[:] = 45.0         # Moderate (30-60%)
    elif role == "Redis":
        cpu[:] = 22.0         # Low (10-35%)
    elif role == "ClickHouse":
        cpu[:] = 55.0         # Moderate (40-70%)
    elif role == "AI":
        cpu[:] = 82.0         # Constant Heavy (70-95%)
    elif role == "Monitoring":
        cpu[:] = 12.0         # Low (5-20%)
    elif role == "Backup":
        cpu[1:4] = 40.0       # Night window
        cpu[0] = 10.0; cpu[4:24] = 10.0
        
    return cpu

# Role specific derivations: (mem_base, mem_scale, disk_base, disk_scale, net_base, net_scale)
ROLE_COVARIATES = {
    "UI":         (60.0, 0.0,   0.0, 0.3, -40.0,  3.5),
    "Database":   (75.0, 0.0, -20.0, 1.6, -200.0, 10.0),
    "Spark":      (45.0, 0.1, -10.0, 0.8,    0.0,  2.0),
    "Kafka":      (60.0, 0.0,  20.0, 0.0, -400.0, 26.0),
    "Redis":      (80.0, 0.0,   5.0, 0.0,  100.0,  0.0),
    "ClickHouse": (65.0, 0.0, -25.0, 1.6, -580.0, 18.0),
    "AI":         (70.0, 0.0,  10.0, 0.2,  100.0,  5.0),
    "Monitoring": (30.0, 0.0,   0.0, 0.5,   -5.0,  3.0),
    "Backup":     (20.0, 0.0,   0.0, 1.9, -150.0, 20.0),
}


def generate_server_inventory(num_servers: int, seed: int) -> pd.DataFrame:
    logger.info("=" * 70)
    logger.info("ATLAS ML Data Generator - Historical Training Data")
    logger.info("=" * 70)
    logger.info(f"Generating inventory for {num_servers} servers...")
    rng = np.random.default_rng(seed)
    py_rng = random.Random(seed)
    
    pcids = [f"PCID-{str(uuid.UUID(int=py_rng.getrandbits(128)))[:8]}" for _ in range(20)]
    acids = [f"ACID-{str(uuid.UUID(int=py_rng.getrandbits(128)))[:8]}" for _ in range(100)]
    
    inventory = []
    for i in range(num_servers):
        loc_name = rng.choice(LOCATIONS)
        city, state, country = LOCATION_DETAILS[loc_name]
        vendor = rng.choice(VENDORS)
        mem_cap = rng.choice(MEM_CAPACITIES)
        
        inventory.append({
            "device_id": f"srv-{i:06d}",
            "server_name": f"host-{loc_name[:3].lower()}-{i:04d}",
            "application_customer_id": rng.choice(acids),
            "platform_customer_id": rng.choice(pcids),
            "location_name": loc_name,
            "location_city": city,
            "location_state": state,
            "location_country": country,
            "tags": rng.choice(ROLES),
            "socket_count": rng.choice(SOCKETS),
            "processor_vendor": vendor,
            "server_generation": rng.choice(GENERATIONS),
            "cpu_inventory": rng.choice(CPU_MODELS[vendor]),
            "memory_inventory": f"{mem_cap}GB DDR4 ECC",
            "memory_capacity_gb": mem_cap,
            "pcie_devices_count": int(rng.integers(1, 6)),
            "last_maintenance_date": (datetime.now(timezone.utc) - timedelta(days=int(rng.integers(180, 365)))).isoformat(),
            "last_boot_time": (datetime.now(timezone.utc) - timedelta(days=int(rng.integers(30, 180)))).isoformat()
        })
    return pd.DataFrame(inventory)


def generate_ar1_noise(total_hours: int, num_servers: int, rho: float, sigma: float, rng) -> np.ndarray:
    noise = np.zeros((total_hours, num_servers))
    shocks = rng.normal(0, sigma, size=(total_hours, num_servers))
    noise[0] = shocks[0]
    for h in range(1, total_hours):
        noise[h] = noise[h-1] * rho + shocks[h]
    return noise


def generate_hourly_metrics(inventory_df: pd.DataFrame, num_days: int, seed: int) -> pd.DataFrame:
    total_hours = num_days * 24
    num_servers = len(inventory_df)
    logger.info(f"Generating {total_hours} hours of telemetry for {num_servers} servers using Strict Causal Chain...")
    
    rng = np.random.default_rng(seed)
    start_time = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0) - timedelta(days=num_days)
    
    cpu_util = np.zeros((total_hours, num_servers))
    mem_util = np.zeros((total_hours, num_servers))
    disk_util = np.zeros((total_hours, num_servers))
    net_util = np.zeros((total_hours, num_servers))
    
    hour_of_day = np.arange(total_hours) % 24
    
    # ---------------------------------------------
    # 1. Generate Baseline CPU Workload State
    # ---------------------------------------------
    for role in ROLES:
        server_mask = (inventory_df["tags"] == role).values
        n_role = server_mask.sum()
        if n_role == 0: continue
        
        prof_cpu = build_cpu_profile(role)
        base_cpu = np.tile(prof_cpu, num_days)[:, None]
        
        cpu_util[:, server_mask] = base_cpu + generate_ar1_noise(total_hours, n_role, 0.85, 3.0, rng)
        
        mem_b, mem_s, disk_b, disk_s, net_b, net_s = ROLE_COVARIATES[role]
        
        mem_util[:, server_mask] = mem_b + (cpu_util[:, server_mask] * mem_s) + generate_ar1_noise(total_hours, n_role, 0.90, 1.0, rng)
        disk_util[:, server_mask] = disk_b + (cpu_util[:, server_mask] * disk_s) + generate_ar1_noise(total_hours, n_role, 0.80, 2.0, rng)
        net_util[:, server_mask] = net_b + (cpu_util[:, server_mask] * net_s) + generate_ar1_noise(total_hours, n_role, 0.80, 10.0, rng)

    cpu_util = np.clip(cpu_util, 5, 95)
    mem_util = np.clip(mem_util, 10, 95)
    disk_util = np.clip(disk_util, 0, 95)
    net_util = np.clip(net_util, 0, 10000)

    # ---------------------------------------------
    # 3. Ambient Temperature (Location Based)
    # ---------------------------------------------
    amb_temp = np.zeros((total_hours, num_servers))
    amb_cycle = np.sin((hour_of_day[:, None] - 8) * np.pi / 12)
    
    for loc, (t_min, t_max) in LOC_TEMP_RANGES.items():
        loc_mask = (inventory_df["location_name"] == loc).values
        n_loc = loc_mask.sum()
        if n_loc == 0: continue
        
        t_mid = (t_max + t_min) / 2.0
        t_amp = (t_max - t_min) / 2.0
        
        base_temp = t_mid + (amb_cycle * t_amp)
        amb_temp[:, loc_mask] = base_temp + generate_ar1_noise(total_hours, n_loc, 0.9, 0.5, rng)
        
    amb_temp = np.clip(amb_temp, 15, 45)

    # ---------------------------------------------
    # 4. GPU Utilization (Generated before Power)
    # ---------------------------------------------
    gpu_util = np.zeros((total_hours, num_servers))
    is_ai = (inventory_df["tags"] == "AI").values
    if is_ai.any():
        n_ai = is_ai.sum()
        gpu_util[:, is_ai] = -100.0 + (cpu_util[:, is_ai] * 2.1) + generate_ar1_noise(total_hours, n_ai, 0.80, 5.0, rng)
    gpu_util = np.clip(gpu_util, 0, 95)

    # ---------------------------------------------
    # 5. Power (Derived from CPU, Socket, Mem Cap, Gen, and GPU)
    # ---------------------------------------------
    socket_count = inventory_df["socket_count"].values
    mem_cap = inventory_df["memory_capacity_gb"].values
    tags = inventory_df["tags"].values
    gen = inventory_df["server_generation"].values
    
    gen_penalty = np.zeros(num_servers)
    gen_penalty[gen == "Gen10"] = 40.0
    gen_penalty[gen == "Gen11"] = 15.0
    gen_penalty[gen == "Gen12"] = 0.0
    
    base_pwr = 80 + (socket_count * 30) + (mem_cap * 0.2) + gen_penalty
    
    avg_power = base_pwr[None, :] + (cpu_util * 3.5) + (gpu_util * 2.5)
    avg_power += generate_ar1_noise(total_hours, num_servers, 0.85, 4.0, rng)
    avg_power = np.clip(avg_power, 120, 800)
    
    max_power = avg_power * rng.uniform(1.02, 1.05, size=(total_hours, num_servers))
    min_power = avg_power * rng.uniform(0.95, 0.98, size=(total_hours, num_servers))

    # ---------------------------------------------
    # 6. CPU Temperature (Derived from Power + Ambient)
    # ---------------------------------------------
    cpu_temp = 20.0 + (avg_power * 0.08) + ((amb_temp - 20) * 1.0)
    cpu_temp += generate_ar1_noise(total_hours, num_servers, 0.85, 1.0, rng)
    cpu_temp = np.clip(cpu_temp, 35, 95)
    
    # ---------------------------------------------
    # 7. Fan Speed
    # ---------------------------------------------
    fan_speed = -1000.0 + (cpu_temp * 100.0)
    fan_speed += generate_ar1_noise(total_hours, num_servers, 0.60, 150.0, rng)
    fan_speed = np.clip(fan_speed, 1500, 12000)

    # ---------------------------------------------
    # 8. Inject Temporal Anomalies (Test Data Only)
    # ---------------------------------------------
    is_anomaly = np.zeros((total_hours, num_servers), dtype=int)
    train_hours = int(total_hours * 0.8)
    
    if total_hours > train_hours:
        # Target 3-5% of servers for anomalous events during the test window
        n_anom_servers = int(num_servers * rng.uniform(0.03, 0.05))
        anom_servers = rng.choice(num_servers, n_anom_servers, replace=False)
        
        types = [
            "thermal_failure", "memory_leak", "crypto_miner", 
            "network_exfil", "disk_failure", "psu_fault", "sensor_failure",
            "runaway_process", "thread_deadlock"
        ]
        
        for s in anom_servers:
            role = inventory_df["tags"].iloc[s]
            anom_type = rng.choice(types + ["gpu_overload"] if role == "AI" else types)
            
            # Start anomaly in test set, duration 4-10 hours to simulate progression
            start_h = rng.integers(train_hours, total_hours - 12)
            duration = rng.integers(4, 10)
            end_h = start_h + duration
            
            is_anomaly[start_h:end_h, s] = 1
            
            if anom_type == "thermal_failure":
                # Fan dies instantly or drops extremely low
                fan_speed[start_h:end_h, s] = rng.uniform(0, 1000)
                # Temp climbs progressively
                start_temp = cpu_temp[start_h, s]
                cpu_temp[start_h:end_h, s] = start_temp + np.linspace(5, 40, duration)
                
            elif anom_type == "memory_leak":
                # Memory slowly creeps to 100%
                start_mem = mem_util[start_h, s]
                mem_util[start_h:end_h, s] = np.linspace(start_mem, 100.0, duration)
                
            elif anom_type == "crypto_miner":
                # CPU and Power surge massively and stay pegged
                cpu_util[start_h:end_h, s] = rng.uniform(96, 100, duration)
                max_hw_pwr = base_pwr[s] + (100 * 3.5) + (100 * 2.5 if role == "AI" else 0)
                avg_power[start_h:end_h, s] = max_hw_pwr * rng.uniform(1.1, 1.3, duration)
                max_power[start_h:end_h, s] = avg_power[start_h:end_h, s] * 1.01
                min_power[start_h:end_h, s] = avg_power[start_h:end_h, s] * 0.99
                cpu_temp[start_h:end_h, s] += np.linspace(5, 20, duration)
                fan_speed[start_h:end_h, s] += np.linspace(1000, 4000, duration)
                
            elif anom_type == "network_exfil":
                # Massive network spike with role-dependent disk behavior
                net_util[start_h:end_h, s] = rng.uniform(6000, 10000, duration)
                if role in ["Database", "ClickHouse"]:
                    disk_util[start_h:end_h, s] = rng.uniform(85, 95, duration)
                    
            elif anom_type == "disk_failure":
                # Disk locks up near 100%
                start_d = disk_util[start_h, s]
                disk_util[start_h:end_h, s] = np.linspace(start_d, 99.9, duration)
                
            elif anom_type == "psu_fault":
                # Sudden massive power spike independent of CPU load
                avg_power[start_h:end_h, s] += rng.uniform(300, 500, duration)
                max_power[start_h:end_h, s] += 400
                
            elif anom_type == "sensor_failure":
                # Impossible relationships (violates causal chain)
                cpu_util[start_h:end_h, s] = 98.0
                cpu_temp[start_h:end_h, s] = 38.0
                avg_power[start_h:end_h, s] = 600.0
                fan_speed[start_h:end_h, s] = 3000.0
                
            elif anom_type == "runaway_process":
                # Software Bug: Infinite loop spins CPU, but I/O and Mem remain flat
                start_cpu = cpu_util[start_h, s]
                cpu_util[start_h:end_h, s] = np.clip(start_cpu + rng.uniform(30.0, 50.0, duration), 0, 100)
                avg_power[start_h:end_h, s] += rng.uniform(40, 80, duration)
                cpu_temp[start_h:end_h, s] += rng.uniform(5, 12, duration)
                
            elif anom_type == "thread_deadlock":
                # Software Bug: App completely locks up. All I/O dies, CPU drops to idle.
                net_util[start_h:end_h, s] = 0.0
                disk_util[start_h:end_h, s] = 0.0
                cpu_util[start_h:end_h, s] = rng.uniform(5.0, 10.0, duration)
                avg_power[start_h:end_h, s] = base_pwr[s] + 15.0
                
            elif anom_type == "gpu_overload":
                # Specific to AI servers
                gpu_util[start_h:end_h, s] = rng.uniform(96, 100, duration)
                cpu_util[start_h:end_h, s] = rng.uniform(85, 95, duration)
                hw_max = base_pwr[s] + (100 * 3.5) + (100 * 2.5)
                avg_power[start_h:end_h, s] = hw_max * rng.uniform(0.9, 1.05, duration)
                cpu_temp[start_h:end_h, s] = rng.uniform(92, 105, duration)
                fan_speed[start_h:end_h, s] = rng.uniform(7500, 10500, duration)
                
    # Re-clip constraints after anomalies
    cpu_util = np.clip(cpu_util, 0, 100)
    mem_util = np.clip(mem_util, 0, 100)
    disk_util = np.clip(disk_util, 0, 100)
    gpu_util = np.clip(gpu_util, 0, 100)

    logger.info("Flattening matrices and stitching with metadata...")
    times = [start_time + timedelta(hours=int(h)) for h in range(total_hours)]
    metric_time_col = np.repeat(times, num_servers)
    
    df_dict = {
        "report_id": [str(uuid.uuid4()) for _ in range(total_hours * num_servers)],
        "device_id": np.tile(inventory_df["device_id"].values, total_hours),
        "server_name": np.tile(inventory_df["server_name"].values, total_hours),
        "application_customer_id": np.tile(inventory_df["application_customer_id"].values, total_hours),
        "platform_customer_id": np.tile(inventory_df["platform_customer_id"].values, total_hours),
        "tags": np.tile(inventory_df["tags"].values, total_hours),
        "location_name": np.tile(inventory_df["location_name"].values, total_hours),
        "location_city": np.tile(inventory_df["location_city"].values, total_hours),
        "location_state": np.tile(inventory_df["location_state"].values, total_hours),
        "location_country": np.tile(inventory_df["location_country"].values, total_hours),
        "processor_vendor": np.tile(inventory_df["processor_vendor"].values, total_hours),
        "server_generation": np.tile(inventory_df["server_generation"].values, total_hours),
        "cpu_inventory": np.tile(inventory_df["cpu_inventory"].values, total_hours),
        "memory_inventory": np.tile(inventory_df["memory_inventory"].values, total_hours),
        "pcie_devices_count": np.tile(inventory_df["pcie_devices_count"].values, total_hours),
        "socket_count": np.tile(inventory_df["socket_count"].values, total_hours),
        "last_maintenance_date": np.tile(inventory_df["last_maintenance_date"].values, total_hours),
        "last_boot_time": np.tile(inventory_df["last_boot_time"].values, total_hours),
        "metric_time": [t.strftime("%Y-%m-%dT%H:%M:%SZ") for t in metric_time_col],
        "avg_metric_value": avg_power.flatten().round(2),
        "max_metric_value": max_power.flatten().round(2),
        "min_metric_value": min_power.flatten().round(2),
        "cpu_utilization": cpu_util.flatten().round(1),
        "memory_utilization": mem_util.flatten().round(1),
        "disk_utilization": disk_util.flatten().round(1),
        "network_throughput": net_util.flatten().round(1),
        "cpu_temperature": cpu_temp.flatten().round(1),
        "amb_temp": amb_temp.flatten().round(1),
        "fan_speed_rpm": fan_speed.flatten().round(0).astype(int),
        "gpu_utilization": gpu_util.flatten().round(1),
        "is_anomaly": is_anomaly.flatten()
    }
    
    return pd.DataFrame(df_dict)


def write_parquet(df: pd.DataFrame, base_dir: str, train_ratio: float = 0.8):
    train_dir = os.path.join(base_dir, "train")
    test_dir = os.path.join(base_dir, "test")
    os.makedirs(train_dir, exist_ok=True)
    os.makedirs(test_dir, exist_ok=True)
    
    df['date_group'] = df['metric_time'].str[:10]
    days = sorted(df['date_group'].unique())
    num_train = int(len(days) * train_ratio)
    
    logger.info(f"Writing partitioned data to {base_dir} ({num_train} train days, {len(days)-num_train} test days)...")
    for i, day in enumerate(days):
        day_df = df[df['date_group'] == day].drop(columns=['date_group'])
        
        if i < num_train:
            target_dir = train_dir
            if "is_anomaly" in day_df.columns:
                day_df = day_df.drop(columns=["is_anomaly"])
        else:
            target_dir = test_dir
            
        filename = os.path.join(target_dir, f"day_{i+1:02d}.parquet")
        
        day_df.to_parquet(filename, engine='pyarrow', compression='snappy', index=False)
        logger.info(f"  -> Saved {filename} ({len(day_df):,} rows)")


def main():
    parser = argparse.ArgumentParser(description="Synthetic Server Telemetry Generator for ML")
    parser.add_argument("--servers", type=int, default=DEFAULT_SERVERS, help="Number of servers")
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS, help="Number of days")
    parser.add_argument("--out", type=str, default=DEFAULT_OUTPUT_DIR, help="Output directory")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED, help="Random seed")
    args = parser.parse_args()

    inventory_df = generate_server_inventory(args.servers, args.seed)
    telemetry_df = generate_hourly_metrics(inventory_df, args.days, args.seed)
    write_parquet(telemetry_df, args.out)
    
    logger.info("-" * 70)
    logger.info("✅ Generation Pipeline Complete!")
    logger.info("-" * 70)


if __name__ == "__main__":
    main()
