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
# COPY engineer_features() FROM train_model.py HERE
# ============================================================

# Paste the EXACT SAME engineer_features(df) function here.
# DO NOT MODIFY IT.
def engineer_features(df):

    df = df.copy()

    ###########################################################
    # Convert timestamps
    ###########################################################

    df["metric_time"] = pd.to_datetime(df["metric_time"])

    df["last_boot_time"] = pd.to_datetime(df["last_boot_time"])

    if "last_maintenance_date" in df.columns:

        df["last_maintenance_date"] = pd.to_datetime(
            df["last_maintenance_date"]
        )

    ###########################################################
    # Hour
    ###########################################################

    df["hour_of_day"] = df.metric_time.dt.hour

    df["day_of_week"] = df.metric_time.dt.dayofweek

    ###########################################################
    # Uptime
    ###########################################################

    df["uptime_hours"] = (

        df.metric_time

        -

        df.last_boot_time

    ).dt.total_seconds() / 3600

    ###########################################################
    # Days Since Maintenance
    ###########################################################

    if "last_maintenance_date" in df.columns:

        df["days_since_maintenance"] = (

            df.metric_time

            -

            df.last_maintenance_date

        ).dt.days

    ###########################################################
    # Memory Capacity
    ###########################################################

    if "memory_capacity_gb" not in df.columns:

        df["memory_capacity_gb"] = (

            df["memory_inventory"]

            .str.extract(r"(\d+)")

            .astype(float)

        )

    ###########################################################
    # Derived Features
    ###########################################################

    df["temperature_delta"] = (

        df.cpu_temperature

        -

        df.amb_temp

    )

    df["power_range"] = (

        df.max_metric_value

        -

        df.min_metric_value

    )

    df["fan_temp_ratio"] = (

        df.fan_speed_rpm

        /

        (df.cpu_temperature + 1)

    )

    df["power_per_socket"] = (

        df.avg_metric_value

        /

        df.socket_count

    )

    df["cpu_memory_ratio"] = (

        df.cpu_utilization

        /

        (df.memory_utilization + 1)

    )

    df["cpu_disk_ratio"] = (

        df.cpu_utilization

        /

        (df.disk_utilization + 1)

    )

    ###########################################################
    # Drop Metadata
    ###########################################################

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

    ###########################################################
    # Remove Label
    ###########################################################

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

    value = 100 * ((score - minimum) / (maximum - minimum))

    value = np.clip(value, 0, 100)

    return value


# ============================================================
# PREDICT ONE FILE
# ============================================================

def predict_file(file_path):

    print(f"\nProcessing {file_path.name}")

    raw = pd.read_parquet(file_path)

    output = raw.copy()

    features = engineer_features(raw)

    X = preprocessor.transform(features)

    prediction = model.predict(X)

    anomaly_score = model.decision_function(X)

    health = health_score(anomaly_score)

    output["prediction"] = prediction

    output["anomaly_score"] = anomaly_score

    output["health_score"] = health.round(2)

    output_file = OUTPUT_DIR / file_path.name

    output.to_parquet(

        output_file,

        index=False

    )

    print(f"Saved -> {output_file}")

    return output


# ============================================================
# MAIN
# ============================================================

def main():

    files = sorted(LIVE_DIR.glob("*.parquet"))

    if len(files) == 0:

        print("No live telemetry found.")

        return

    for file in files:

        predict_file(file)


if __name__ == "__main__":

    main()