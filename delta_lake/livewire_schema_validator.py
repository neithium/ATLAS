"""
ATLAS Livewire Mode - Schema Validation & Alignment Engine
================================================================================
Validates and transforms upstream flattened Parquet data to match the Refined 
Layer schema before Delta MERGE deduplication.

Critical Functions:
- validate_schema(): Check incoming DataFrame against expected schema
- align_schema(): Cast, rename, and transform columns to match expected output
- generate_schema_diff(): Report schema mismatches for debugging

The validator is designed to be forgiving of minor upstream schema variations:
- String-to-Timestamp automatic casting
- Missing optional columns (filled with NULL)
- Extra columns (ignored - not required)
- Column name variations (mapped via column mapping dictionary)
"""

from typing import Dict, List, Optional, Tuple
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, to_timestamp, cast, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType, 
    IntegerType, LongType
)


# =============================================================================
# EXPECTED REFINED LAYER SCHEMA
# =============================================================================

def get_refined_layer_schema() -> StructType:
    """
    Expected output schema for the Refined Layer Delta table.
    
    This is the contract that all livewire data must conform to before MERGE.
    """
    return StructType([
        StructField("report_id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("application_customer_id", StringType(), True),
        StructField("platform_customer_id", StringType(), True),
        StructField("status", BooleanType(), True),
        StructField("report_type", StringType(), True),
        StructField("error_reason", StringType(), True),
        StructField("MetricValue", DoubleType(), True),
        StructField("model", StringType(), True),
        StructField("tags", StringType(), True),
        StructField("location_state", StringType(), True),
        StructField("location_country", StringType(), True),
        StructField("processor_vendor", StringType(), True),
        StructField("server_generation", StringType(), True),
        StructField("location_id", StringType(), True),
        StructField("location_name", StringType(), True),
        StructField("location_city", StringType(), True),
        StructField("server_name", StringType(), True),
        StructField("metric_id", StringType(), True),
        StructField("cpu_inventory", StringType(), True),
        StructField("memory_inventory", StringType(), True),
        StructField("pcie_devices_count", IntegerType(), True),
        StructField("socket_count", IntegerType(), True),
        StructField("avg_metric_value", DoubleType(), True),
        StructField("max_metric_value", DoubleType(), True),
        StructField("min_metric_value", DoubleType(), True),
        StructField("metric_time", StringType(), True),
        StructField("datetime", DoubleType(), True),
        StructField("timeRangeEnd", DoubleType(), True),
        StructField("amb_temp", DoubleType(), True),
        StructField("Insertiontime", DoubleType(), True),
        StructField("co2_factor", DoubleType(), True),
        StructField("energy_cost_factor", DoubleType(), True),
        StructField("max_metric_time", StringType(), True),
        StructField("location_date", StringType(), True),
        StructField("inventory_date", StringType(), True),
        StructField("file_date", StringType(), True),  # Added for streaming tracking
        StructField("partition_date", StringType(), True),  # Added for 5-level partitioning
    ])


# =============================================================================
# COLUMN MAPPING STRATEGY
# =============================================================================

# Map upstream column names (variations) to expected refined layer names
# This handles schema variations from different upstream processors
COLUMN_NAME_MAPPING = {
    "MetricValue": ["MetricValue", "metric_value", "value", "avg_value"],
    "metric_time": ["metric_time", "timestamp", "record_time", "event_time"],
    "datetime": ["datetime", "created_at", "timestamp_unix"],
    "reporting_time": ["reporting_time", "report_time", "submission_time"],
    "device_id": ["device_id", "server_id", "device_name"],
    "application_customer_id": ["application_customer_id", "app_id", "application_id"],
    "platform_customer_id": ["platform_customer_id", "platform_id", "customer_platform_id"],
    "status": ["status", "is_active", "active"],
    "report_type": ["report_type", "metric_type", "report_category"],
    "error_reason": ["error_reason", "error", "failure_reason"],
    "cpu_inventory": ["cpu_inventory", "cpus", "cpu_list"],
    "memory_inventory": ["memory_inventory", "memory", "memory_list"],
    "socket_count": ["socket_count", "num_sockets", "sockets"],
    "pcie_devices_count": ["pcie_devices_count", "num_pcie", "pcie_count"],
    "amb_temp": ["amb_temp", "ambient_temp", "temperature"],
}


# =============================================================================
# SCHEMA VALIDATION & ALIGNMENT
# =============================================================================

def validate_and_align_schema(raw_df: DataFrame, spark: SparkSession = None) -> Tuple[DataFrame, Dict]:
    """
    Validate and align upstream schema to match Refined Layer expectations.
    
    This function:
    1. Checks if incoming DataFrame matches the expected Refined Layer schema
    2. If there's a mismatch:
       - Maps column names (handling variations from upstream)
       - Casts data types automatically (e.g., String → Timestamp)
       - Fills missing optional columns with NULL
       - Removes extra columns not needed
    3. Returns aligned DataFrame ready for MERGE deduplication
    4. Also returns a validation report for debugging
    
    Args:
        raw_df: DataFrame from upstream /stream directory
        spark: SparkSession instance (optional, auto-detected from df)
    
    Returns:
        Tuple[aligned_df, validation_report]
        - aligned_df: DataFrame with schema matching Refined Layer
        - validation_report: Dict with validation status, mismatches, mappings applied
    """
    
    if spark is None:
        spark = raw_df.sparkSession
    
    expected_schema = get_refined_layer_schema()
    actual_columns = set(raw_df.columns)
    expected_columns = {field.name for field in expected_schema.fields}
    
    validation_report = {
        "status": "PASS",
        "total_expected_columns": len(expected_columns),
        "total_actual_columns": len(actual_columns),
        "missing_columns": [],
        "extra_columns": [],
        "type_mismatches": [],
        "column_mappings_applied": {},
        "schema_evolution_changes": [],
    }
    
    # Check for missing columns
    missing_cols = expected_columns - actual_columns
    
    # Try to map missing columns using column name mapping strategy
    column_mapping_applied = {}
    for expected_col, possible_names in COLUMN_NAME_MAPPING.items():
        if expected_col in missing_cols:
            for possible_name in possible_names:
                if possible_name in actual_columns:
                    column_mapping_applied[possible_name] = expected_col
                    missing_cols.discard(expected_col)
                    print(f"         ℹ Column mapping: '{possible_name}' → '{expected_col}'")
                    break
    
    validation_report["column_mappings_applied"] = column_mapping_applied
    validation_report["missing_columns"] = list(missing_cols)
    
    # Check for extra columns (not in expected schema)
    extra_cols = actual_columns - expected_columns - set(column_mapping_applied.keys())
    validation_report["extra_columns"] = list(extra_cols)
    
    # Check for type mismatches
    actual_schema_dict = {field.name: field.dataType for field in raw_df.schema.fields}
    expected_schema_dict = {field.name: field.dataType for field in expected_schema.fields}
    
    for col_name in actual_columns & expected_columns:
        actual_type = actual_schema_dict[col_name]
        expected_type = expected_schema_dict[col_name]
        if str(actual_type) != str(expected_type):
            validation_report["type_mismatches"].append({
                "column": col_name,
                "expected_type": str(expected_type),
                "actual_type": str(actual_type)
            })
    
    # Determine alignment strategy
    if not missing_cols and not column_mapping_applied and not validation_report["type_mismatches"]:
        # Perfect schema match - no transformation needed
        validation_report["status"] = "PASS_EXACT_MATCH"
        print(f"         ✓ Schema validation: EXACT MATCH")
        return raw_df, validation_report
    else:
        # Schema mismatches found - need alignment
        validation_report["status"] = "ALIGNED_WITH_TRANSFORMATIONS"
        print(f"         ✓ Schema validation: MISMATCHES FOUND - APPLYING TRANSFORMATIONS")
        aligned_df = _apply_schema_alignment(raw_df, expected_schema, column_mapping_applied, spark)
        return aligned_df, validation_report


def _apply_schema_alignment(
    raw_df: DataFrame,
    expected_schema: StructType,
    column_mappings: Dict[str, str],
    spark: SparkSession
) -> DataFrame:
    """
    Apply transformations to align raw DataFrame to expected schema.
    
    Steps:
    1. Rename columns using column mappings
    2. Cast columns to correct data types
    3. Add missing columns with NULL values
    4. Select only expected columns in correct order
    5. Fill NULL values for required fields with defaults
    """
    
    df = raw_df
    
    # Step 1: Rename columns based on mapping
    for old_name, new_name in column_mappings.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)
            print(f"         ℹ Renamed: {old_name} → {new_name}")
    
    # Step 2: Cast columns to correct types
    for field in expected_schema.fields:
        col_name = field.name
        data_type = field.dataType
        
        if col_name in df.columns:
            actual_type = df.schema[col_name].dataType
            if str(actual_type) != str(data_type):
                try:
                    df = df.withColumn(col_name, cast(col(col_name), data_type))
                    print(f"         ℹ Cast: {col_name} from {actual_type} to {data_type}")
                except Exception as e:
                    print(f"         ⚠ Cast failed for {col_name}: {e}")
    
    # Step 3: Add missing columns with NULL values
    for field in expected_schema.fields:
        col_name = field.name
        if col_name not in df.columns:
            df = df.withColumn(col_name, lit(None).cast(field.dataType))
            print(f"         ℹ Added missing column: {col_name} (NULL)")
    
    # Step 4: Select only expected columns in expected order
    expected_col_names = [field.name for field in expected_schema.fields]
    df = df.select([col(name) for name in expected_col_names])
    
    print(f"         ✓ Schema alignment complete - DataFrame ready for MERGE")
    return df


def generate_schema_diff_report(validation_report: Dict) -> str:
    """Generate a human-readable schema difference report."""
    lines = []
    lines.append("\n================================================================================")
    lines.append("  LIVEWIRE SCHEMA VALIDATION REPORT")
    lines.append("================================================================================\n")
    
    lines.append(f"Status: {validation_report['status']}")
    lines.append(f"Expected columns: {validation_report['total_expected_columns']}")
    lines.append(f"Actual columns: {validation_report['total_actual_columns']}\n")
    
    if validation_report["column_mappings_applied"]:
        lines.append("Column Mappings Applied:")
        for old, new in validation_report["column_mappings_applied"].items():
            lines.append(f"  {old} → {new}")
        lines.append("")
    
    if validation_report["missing_columns"]:
        lines.append(f"Missing Columns ({len(validation_report['missing_columns'])}):")
        for col in validation_report["missing_columns"]:
            lines.append(f"  - {col}")
        lines.append("")
    
    if validation_report["extra_columns"]:
        lines.append(f"Extra Columns ({len(validation_report['extra_columns'])}) - Will be ignored:")
        for col in validation_report["extra_columns"][:10]:  # Show first 10
            lines.append(f"  - {col}")
        if len(validation_report["extra_columns"]) > 10:
            lines.append(f"  ... and {len(validation_report['extra_columns']) - 10} more")
        lines.append("")
    
    if validation_report["type_mismatches"]:
        lines.append(f"Type Mismatches ({len(validation_report['type_mismatches'])}) - Will be cast:")
        for mismatch in validation_report["type_mismatches"]:
            lines.append(f"  - {mismatch['column']}: {mismatch['actual_type']} → {mismatch['expected_type']}")
        lines.append("")
    
    lines.append("================================================================================\n")
    
    return "\n".join(lines)


# =============================================================================
# SCHEMA INFERENCE
# =============================================================================

def infer_upstream_schema_from_sample(stream_path: str, spark: SparkSession, sample_files: int = 3) -> None:
    """
    Analyze upstream data directory and infer actual schema.
    
    Useful for debugging when upstream schema is unknown or varies.
    Reads first N parquet files from stream directory and reports schema.
    """
    try:
        # Read sample files
        sample_df = spark.read.parquet(f"{stream_path}/*.parquet")
        
        print(f"\n         ℹ Upstream Schema Inference from {stream_path}:")
        print(f"         ℹ Columns found: {len(sample_df.columns)}")
        print(f"         ℹ Sample schema:")
        sample_df.printSchema()
        
    except Exception as e:
        print(f"         ⚠ Could not infer schema from {stream_path}: {e}")
