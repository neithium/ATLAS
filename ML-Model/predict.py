import glob
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

# ============================================================
# PATHS
# ============================================================

LIVE_DIR = Path("telemetry-data/live")
OUTPUT_DIR = Path("telemetry-data/predictions")
MODEL_DIR = Path("models")

OUTPUT_DIR.mkdir(exist_ok=True)

# ============================================================
# FEATURE ENGINEERING
# ============================================================

def engineer_features(df):

    df = df.copy()

    # --------------------------------------------------------
    # Convert timestamps
    # --------------------------------------------------------

    df["metric_time"] = pd.to_datetime(df["metric_time"])
    df["last_boot_time"] = pd.to_datetime(df["last_boot_time"])

    if "last_maintenance_date" in df.columns:
        df["last_maintenance_date"] = pd.to_datetime(
            df["last_maintenance_date"]
        )

    # --------------------------------------------------------
    # Time Features
    # --------------------------------------------------------

    df["hour_of_day"] = df["metric_time"].dt.hour
    df["day_of_week"] = df["metric_time"].dt.dayofweek

    df["uptime_hours"] = (
        df["metric_time"] -
        df["last_boot_time"]
    ).dt.total_seconds() / 3600

    if "last_maintenance_date" in df.columns:

        df["days_since_maintenance"] = (
            df["metric_time"] -
            df["last_maintenance_date"]
        ).dt.days

    # --------------------------------------------------------
    # Memory Capacity
    # --------------------------------------------------------

    if (
        "memory_capacity_gb" not in df.columns
        and
        "memory_inventory" in df.columns
    ):

        df["memory_capacity_gb"] = (
            df["memory_inventory"]
            .astype(str)
            .str.extract(r"(\d+)")
            .astype(float)
        )

    # --------------------------------------------------------
    # Derived Features
    # --------------------------------------------------------

    df["temperature_delta"] = (
        df["cpu_temperature"]
        -
        df["amb_temp"]
    )

    df["power_range"] = (
        df["max_metric_value"]
        -
        df["min_metric_value"]
    )

    df["fan_temp_ratio"] = (
        df["fan_speed_rpm"]
        /
        (df["cpu_temperature"] + 1)
    )

    if "socket_count" in df.columns:

        df["power_per_socket"] = (
            df["avg_metric_value"]
            /
            df["socket_count"]
        )

    else:

        df["power_per_socket"] = df["avg_metric_value"]

    df["cpu_memory_ratio"] = (
        df["cpu_utilization"]
        /
        (df["memory_utilization"] + 1)
    )

    df["cpu_disk_ratio"] = (
        df["cpu_utilization"]
        /
        (df["disk_utilization"] + 1)
    )

    # --------------------------------------------------------
    # Drop Metadata
    # --------------------------------------------------------

    drop_columns = [

        "report_id",

        "device_id",

        "server_name",

        "application_customer_id",

        "platform_customer_id",

        "location_city",

        "location_state",

        "location_country",

        "cpu_inventory",

        "memory_inventory",

        "metric_time",

        "last_boot_time",

        "last_maintenance_date"

    ]

    for col in drop_columns:

        if col in df.columns:

            df.drop(columns=col, inplace=True)

    # --------------------------------------------------------
    # Remove Label
    # --------------------------------------------------------

    if "is_anomaly" in df.columns:

        df.drop(columns=["is_anomaly"], inplace=True)

    return df

# ============================================================
# LOAD MODELS
# ============================================================

print("Loading models...")

model = joblib.load(MODEL_DIR / "isolation_forest.pkl")
preprocessor = joblib.load(MODEL_DIR / "preprocessor.pkl")
health_cfg = joblib.load(MODEL_DIR / "health_score_config.pkl")

print("Models Loaded")


# ============================================================
# HEALTH SCORE
# ============================================================

def health_score(score):

    minimum = health_cfg["min_score"]
    maximum = health_cfg["max_score"]

    value = 100 * (
        (score - minimum)
        /
        (maximum - minimum)
    )

    value = np.clip(value, 0, 100)

    return value


# ============================================================
# PREDICT ONE FILE
# ============================================================

def predict_file(file_path):

    print(f"\nProcessing {file_path.name}")

    raw = pd.read_parquet(file_path)

    output = raw.copy()

    # --------------------------------------------------------
    # Feature Engineering
    # --------------------------------------------------------

    features = engineer_features(raw)

    X = preprocessor.transform(features)

    # --------------------------------------------------------
    # Prediction
    # --------------------------------------------------------

    anomaly_score = model.decision_function(X)

    if "threshold" in health_cfg:

        threshold = health_cfg["threshold"]

    else:

        print("Threshold not found. Using 20th percentile.")

        threshold = np.percentile(
            anomaly_score,
            20
        )

    prediction = np.where(
        anomaly_score < threshold,
        -1,
        1
    )

    health = health_score(anomaly_score)

    # --------------------------------------------------------
    # Add Results
    # --------------------------------------------------------

    output["prediction"] = prediction

    output["anomaly_score"] = anomaly_score

    output["health_score"] = health.round(2)
    output["health_status"] = output["health_score"].apply(
        lambda x: "Healthy" if x >= 90 else "Warning" if x >= 50 else "Degraded"
    )

    # --------------------------------------------------------
    # Uptime
    # --------------------------------------------------------

    output["uptime_hours"] = (

        pd.to_datetime(output["metric_time"])

        -

        pd.to_datetime(output["last_boot_time"])

    ).dt.total_seconds() / 3600

    # --------------------------------------------------------
    # Memory Capacity (safe)
    # --------------------------------------------------------

    if "memory_capacity_gb" not in output.columns:

        if "memory_inventory" in output.columns:

            output["memory_capacity_gb"] = (

                output["memory_inventory"]

                .astype(str)

                .str.extract(r"(\d+)")

                .astype(float)

            )

        else:

            output["memory_capacity_gb"] = np.nan

    # --------------------------------------------------------
    # Final Output Schema
    # --------------------------------------------------------

    required_columns = [

        "device_id",

        "server_name",

        "tags",

        "location_name",

        "metric_time",

        "avg_metric_value",

        "cpu_utilization",

        "memory_utilization",

        "disk_utilization",

        "network_throughput",

        "cpu_temperature",

        "amb_temp",

        "fan_speed_rpm",

        "gpu_utilization",

        "uptime_hours",

        "processor_vendor",

        "server_generation",

        "memory_capacity_gb",

        "prediction",

        "anomaly_score",

        "health_score",
        
        "health_status"

    ]
    if "is_anomaly" in output.columns:
        required_columns.append("is_anomaly")

    final_columns = [

        c

        for c in required_columns

        if c in output.columns

    ]

    output = output[final_columns]

    output_file = OUTPUT_DIR / file_path.name

    output.to_parquet(

        output_file,

        index=False

    )

    print(f"Saved -> {output_file}")

    print(

        f"Normal : {(prediction==1).sum()} | "

        f"Anomaly : {(prediction==-1).sum()}"

    )

    return output


# ============================================================
# MAIN
# ============================================================

def main():

    files = sorted(

        LIVE_DIR.glob("*.parquet"),

        key=lambda x: x.stat().st_mtime,

        reverse=True

    )

    if len(files) == 0:

        print("No live telemetry found.")

        return

    # Process only latest live file
    predict_file(files[0])


if __name__ == "__main__":

    main()