# Bugs to be Fixed in `delta_lake` folder

Based on the analysis of the `delta_lake` pipeline code, here are the critical and minor bugs identified across the processing scripts that need to be resolved to ensure data integrity and pipeline stability:

## Critical Bugs

### 1. `partition_date` and `file_date` Population Logic Bug (`livewire_streaming.py`)
- **Location:** `livewire_merge_batch` in `livewire_streaming.py`
- **Issue:** The schema validation (`validate_and_align_schema`) is executed *before* `partition_date` and `file_date` are derived from `metric_time`. Since the `Refined Layer Schema` defines these columns, the validator proactively inserts them as `NULL` columns. Subsequently, the derived column logic (`if "partition_date" not in batch_df.columns:`) evaluates to `False`, bypassing the derivation logic entirely.
- **Impact:** All real-time streaming data gets a `NULL` partition date and file date, meaning all data falls into `__HIVE_DEFAULT_PARTITION__`.
- **Fix:** Move the logic to derive `partition_date` and `file_date` from `metric_time` *before* calling `validate_and_align_schema`, or adjust the condition to check if the column values are entirely null.

### 2. Inconsistent Schema Casting for `partition_date` (`livewire_streaming.py`)
- **Location:** `livewire_merge_batch` in `livewire_streaming.py`
- **Issue:** The code does `to_date(col("metric_time"), "yyyy-MM-dd HH:mm:ss")`, which generates a Spark `DateType`. However, the established target schema for the refined Delta table requires `partition_date` to be a `StringType` (`"yyyy-MM-dd"`). 
- **Impact:** Writing this DataFrame to Delta can cause a `TypeMismatchException` because `DateType` contradicts the Delta schema's `StringType()`.
- **Fix:** Format it explicitly as string: `date_format(to_date(col("metric_time")), "yyyy-MM-dd")`.

### 3. Missing Compression Codec for First Batch (`livewire_streaming.py`)
- **Location:** Initial batch append block in `livewire_streaming.py` (`else:` branch for `target_exists`)
- **Issue:** While initializing the empty Delta table sets the compression codec option (`.option("parquet.compression", LivewireConfig.COMPRESSION_CODEC)`), the immediate follow-up `append` logic for the first batch's data omits it.
- **Impact:** The first batch will fall back to Spark's default compression (typically `snappy` or `gzip`), ignoring the configured optimal codec (`zstd`), causing inconsistent file sizes and suboptimal storage compression.
- **Fix:** Add `.option("parquet.compression", LivewireConfig.COMPRESSION_CODEC)` to the write block of the append branch.

## Moderate / Minor Bugs

### 4. Over-Aggressive Date Formatting in `prepare_partition_columns` (`delta_merge_pipeline.py`)
- **Location:** `prepare_partition_columns` in `delta_merge_pipeline.py`
- **Issue:** If `partition_date` is already in the columns, it calls `date_format(col("partition_date"), "yyyy-MM-dd")`. Since `partition_date` from `generate_data.py` is already explicitly formatted as a `"yyyy-MM-dd"` string, feeding a formatted string back to Spark's `date_format()` can evaluate to `null` or throw errors in specific Spark 3.x configuration regimes without an intermediate `to_date` cast.
- **Fix:** Either trust the existing format if present or cast it explicitly: `date_format(to_date(col("partition_date")), "yyyy-MM-dd")`.

### 5. Missing Schema Report Edge Case Validation (`livewire_streaming.py`)
- **Location:** `metrics.record_batch(...)` inside `livewire_merge_batch`
- **Issue:** If `LivewireConfig.VALIDATE_SCHEMA` is set to `False`, the inline `if` expression evaluates accurately without causing a `NameError`. However, this relies on Python's lazy evaluation, which reduces readability and makes the code fragile if future lines accidentally access `schema_report["status"]`. 
- **Fix:** Pre-initialize `schema_report = {"status": "PASS_EXACT_MATCH"}` at the start of the block, bypassing the need for complicated inline `if` checks.

### 6. Streaming Performance / Throttling Gap (`livewire_streaming.py`)
- **Location:** `run_livewire_streaming`
- **Issue:** The Parquet readStream setup (`spark.readStream.format("parquet").load(...)`) does not specify `maxFilesPerTrigger`.
- **Impact:** While `maxFileAge` is present, a sudden influx of large historical files into the stream directory could overwhelm the driver during a single micro-batch.
- **Fix:** Define `.option("maxFilesPerTrigger", 100)` or similar batch limitation to ensure graceful back-pressure.

### 7. Delta Merge Retry Scope Error (`delta_merge_pipeline.py`)
- **Location:** `execute_merge_deduplication` exception block
- **Issue:** Upon encountering `Py4JJavaError` for a file missing, `spark.catalog.clearCache()` and `delta_table = DeltaTable.forPath` are executed to reset the state. However, the alias in the query (`delta_table.alias("target")`) inside the retry loop refers to the *re-assigned* variable correctly, but doing so could risk `ConcurrentModificationException` failures on highly conflicted transactions.
- **Fix:** Safely re-initialize the bounds of the merge.

---
 
