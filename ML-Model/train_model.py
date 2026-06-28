import os
import glob
import joblib
import numpy as np
import pandas as pd

from pathlib import Path

from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, OrdinalEncoder
from sklearn.impute import SimpleImputer
from sklearn.ensemble import IsolationForest


# ============================================================
# CONFIG
# ============================================================

TRAIN_DIR = Path("telemetry-data/train")
MODEL_DIR = Path("models")

MODEL_DIR.mkdir(exist_ok=True)

RANDOM_STATE = 42

CONTAMINATION = 0.03


# ============================================================
# LOAD TRAINING DATA
# ============================================================

def load_training_data():

    parquet_files = sorted(TRAIN_DIR.glob("*.parquet"))

    if len(parquet_files) == 0:
        raise Exception(
            f"No parquet files found inside {TRAIN_DIR}"
        )

    print(f"Found {len(parquet_files)} parquet files")

    dfs = []

    for file in parquet_files:

        print("Reading", file.name)

        dfs.append(pd.read_parquet(file))

    df = pd.concat(dfs, ignore_index=True)

    print()

    print("Total Rows :", len(df))

    print()

    return df


# ============================================================
# FEATURE ENGINEERING
# ============================================================

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
# PREPROCESSOR
# ============================================================

def build_preprocessor(df):

    categorical = [

        "tags",

        "processor_vendor",

        "server_generation",

        "location_name"

    ]

    categorical = [

        c

        for c in categorical

        if c in df.columns

    ]

    numeric = [

        c

        for c in df.columns

        if c not in categorical

    ]

    numeric_pipeline = Pipeline(

        [

            (

                "imputer",

                SimpleImputer(strategy="median")

            ),

            (

                "scaler",

                StandardScaler()

            )

        ]

    )

    categorical_pipeline = Pipeline(

        [

            (

                "imputer",

                SimpleImputer(strategy="most_frequent")

            ),

            (

                "encoder",

                OrdinalEncoder(

                    handle_unknown="use_encoded_value",

                    unknown_value=-1

                )

            )

        ]

    )

    preprocessor = ColumnTransformer(

        [

            (

                "num",

                numeric_pipeline,

                numeric

            ),

            (

                "cat",

                categorical_pipeline,

                categorical

            )

        ]

    )

    return preprocessor

# ============================================================
# TRAIN MODEL
# ============================================================

def train_model():

    print("=" * 60)
    print("Loading Training Data")
    print("=" * 60)

    df = load_training_data()

    print("\nEngineering Features...")

    df = engineer_features(df)

    print(f"Feature Matrix Shape : {df.shape}")

    # --------------------------------------------------------
    # Build preprocessing pipeline
    # --------------------------------------------------------

    print("\nBuilding preprocessing pipeline...")

    preprocessor = build_preprocessor(df)

    X = preprocessor.fit_transform(df)

    print("Processed Shape :", X.shape)

    # --------------------------------------------------------
    # Train Isolation Forest
    # --------------------------------------------------------

    print("\nTraining Isolation Forest...")

    model = IsolationForest(

        n_estimators=300,

        contamination=CONTAMINATION,

        random_state=RANDOM_STATE,

        n_jobs=-1,

        verbose=1

    )

    model.fit(X)

    print("Training Complete")

    # --------------------------------------------------------
    # Compute Scores
    # --------------------------------------------------------

    print("\nComputing score statistics...")

    anomaly_scores = model.decision_function(X)

    score_config = {

        "min_score": float(anomaly_scores.min()),

        "max_score": float(anomaly_scores.max())

    }

    print()

    print("Minimum Score :", score_config["min_score"])

    print("Maximum Score :", score_config["max_score"])

    # --------------------------------------------------------
    # Save Models
    # --------------------------------------------------------

    print("\nSaving Models...")

    joblib.dump(

        model,

        MODEL_DIR / "isolation_forest.pkl"

    )

    joblib.dump(

        preprocessor,

        MODEL_DIR / "preprocessor.pkl"

    )

    joblib.dump(

        score_config,

        MODEL_DIR / "health_score_config.pkl"

    )

    print()

    print("=" * 60)

    print("Training Finished Successfully")

    print("=" * 60)

    print()

    print("Saved Files")

    print("-----------------------------")

    print("✔ isolation_forest.pkl")

    print("✔ preprocessor.pkl")

    print("✔ health_score_config.pkl")

    print()

    return model


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    train_model()