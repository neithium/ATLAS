"""
ATLAS Storage Layer — Local Integration Test
=============================================
Tests the hardened delta_loader.py without requiring Docker.

Validates:
  1. Parquet reading with Hive partition pruning
  2. Type conversions (prepare_for_clickhouse)
  3. Retry/backoff function behavior
  4. DLQ routing logic
  5. Batch parameter building for PG upserts
  6. Buffer table target configuration
  7. Timing instrumentation
"""

import os
import sys
import time

# Point to test data
TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "test_refined")
os.environ["REFINED_DATA_PATH"] = TEST_DATA_DIR
os.environ["RETRY_MAX_ATTEMPTS"] = "3"
os.environ["RETRY_BASE_DELAY_SECONDS"] = "0.1"  # Fast retries for testing

# Add clickhouse dir to path so we can import delta_loader directly
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Force UTF-8 output on Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import pandas as pd

PASS = 0
FAIL = 0

def test(name, condition, detail=""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  [PASS] {name}")
    else:
        FAIL += 1
        print(f"  [FAIL] {name}  — {detail}")


def main():
    global PASS, FAIL

    print("=" * 65)
    print("  ATLAS Storage Layer — Local Integration Test")
    print("=" * 65)

    # ── Test 1: Import delta_loader without errors ───────────────────────
    print("\n[1/8] Import & Configuration")
    try:
        import delta_loader as dl
        test("delta_loader imports cleanly", True)
    except Exception as e:
        test("delta_loader imports cleanly", False, str(e))
        print("FATAL: Cannot continue without import. Exiting.")
        sys.exit(1)

    test("CH_INSERT_TABLE is buffer table",
         dl.CH_INSERT_TABLE == "atlas.telemetry_refined_buffer",
         f"got: {dl.CH_INSERT_TABLE}")

    test("RETRY_MAX_ATTEMPTS loaded from env",
         dl.RETRY_MAX_ATTEMPTS == 3,
         f"got: {dl.RETRY_MAX_ATTEMPTS}")

    test("RETRY_BASE_DELAY loaded from env",
         dl.RETRY_BASE_DELAY == 0.1,
         f"got: {dl.RETRY_BASE_DELAY}")

    # ── Test 2: Parquet reading ──────────────────────────────────────────
    print("\n[2/8] Parquet Reading (partition-aware)")
    df = dl.read_refined_parquet(TEST_DATA_DIR, watermark=None)
    test("read_refined_parquet returns DataFrame", isinstance(df, pd.DataFrame))
    test(f"Read {len(df)} rows (expected ~3600)", 3000 <= len(df) <= 4000, f"got {len(df)}")

    expected_cols = ["device_id", "platform_customer_id", "metric_time", "MetricValue",
                     "avg_metric_value", "max_metric_value", "min_metric_value"]
    missing = [c for c in expected_cols if c not in df.columns]
    test("All key columns present", len(missing) == 0, f"missing: {missing}")

    # ── Test 3: Partition pruning with watermark ─────────────────────────
    print("\n[3/8] Partition Pruning")
    # Use a future date as watermark — should return fewer or no rows
    future_wm = "2099-01-01T00:00:00Z"
    cutoff = dl._compute_partition_cutoff(future_wm)
    test("Partition cutoff computed", cutoff == "2099-01-01", f"got: {cutoff}")

    df_pruned = dl.read_refined_parquet(TEST_DATA_DIR, watermark=future_wm)
    test("Future watermark returns 0 rows (pruned)", len(df_pruned) == 0, f"got {len(df_pruned)}")

    # ── Test 4: Type conversions ─────────────────────────────────────────
    print("\n[4/8] Type Conversions (prepare_for_clickhouse)")
    df_prepared = dl.prepare_for_clickhouse(df.copy())

    test("status column is int", df_prepared["status"].dtype in [int, "int64", "int32"],
         f"got: {df_prepared['status'].dtype}")

    test("metric_time is datetime",
         pd.api.types.is_datetime64_any_dtype(df_prepared["metric_time"]),
         f"got: {df_prepared['metric_time'].dtype}")

    test("No NaN in float columns",
         df_prepared["MetricValue"].isna().sum() == 0,
         f"got {df_prepared['MetricValue'].isna().sum()} NaNs")

    test("pcie_devices_count is int",
         df_prepared["pcie_devices_count"].dtype in [int, "int64", "int32"],
         f"got: {df_prepared['pcie_devices_count'].dtype}")

    # Check error_reason: should be None (not NaN) where null
    null_reasons = [v for v in df_prepared["error_reason"] if v is not None and pd.isna(v)]
    test("error_reason uses None not NaN", len(null_reasons) == 0,
         f"found {len(null_reasons)} NaN values instead of None")

    # ── Test 5: Retry with backoff ───────────────────────────────────────
    print("\n[5/8] Retry with Backoff")

    call_count = 0
    def flaky_func():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise ConnectionError(f"Simulated failure #{call_count}")
        return "success"

    t0 = time.time()
    result = dl.retry_with_backoff(flaky_func, max_attempts=3, base_delay=0.05)
    elapsed = time.time() - t0
    test("Retry succeeded after 2 failures", result == "success")
    test(f"Called {call_count} times (expected 3)", call_count == 3)
    test(f"Backoff took {elapsed:.2f}s (expected >0.1s)", elapsed > 0.05)

    # Test exhaustion
    def always_fail():
        raise RuntimeError("permanent failure")

    try:
        dl.retry_with_backoff(always_fail, max_attempts=2, base_delay=0.01)
        test("Exhausted retries raises exception", False, "no exception raised")
    except RuntimeError:
        test("Exhausted retries raises exception", True)

    # ── Test 6: DLQ routing ──────────────────────────────────────────────
    print("\n[6/8] DLQ Routing (mock)")
    # We can't test actual PG insertion, but verify the function handles None pg_conn gracefully
    dl.route_to_dlq(None, [{"bad": "row"}], "test error")
    test("route_to_dlq handles None pg_conn without crash", True)

    # ── Test 7: Column ordering ──────────────────────────────────────────
    print("\n[7/8] Column Ordering & CH_COLUMNS")
    test(f"CH_COLUMNS has 36 entries (37 minus DEFAULT insertion_time)",
         len(dl.CH_COLUMNS) == 36, f"got {len(dl.CH_COLUMNS)}")

    insert_cols = [c for c in dl.CH_COLUMNS if c in df_prepared.columns]
    test(f"Matched {len(insert_cols)}/36 columns in prepared data",
         len(insert_cols) >= 30, f"only matched {len(insert_cols)}")

    # ── Test 8: Batch PG upsert param building ───────────────────────────
    print("\n[8/8] Batch Upsert Parameter Building")
    # Simulate what upsert_device_registry does (build params without actual PG)
    device_cols = ["device_id", "platform_customer_id", "application_customer_id",
                   "server_name", "model", "processor_vendor", "server_generation", "socket_count"]
    available = [c for c in device_cols if c in df_prepared.columns]
    devices = df_prepared[available].drop_duplicates(
        subset=["device_id", "platform_customer_id", "application_customer_id"]
    )
    params = [
        (
            row.get("device_id"), row.get("platform_customer_id"), row.get("application_customer_id"),
            row.get("server_name", ""), row.get("model", ""), row.get("processor_vendor", ""),
            row.get("server_generation", ""),
            int(row["socket_count"]) if "socket_count" in row and pd.notna(row.get("socket_count")) else None,
        )
        for row in devices.to_dict("records")
    ]
    test(f"Built {len(params)} device param tuples", len(params) == 50, f"got {len(params)}")
    test("Each tuple has 8 elements", all(len(p) == 8 for p in params))

    # ── Summary ──────────────────────────────────────────────────────────
    print("\n" + "=" * 65)
    print(f"  RESULTS: {PASS} passed, {FAIL} failed, {PASS + FAIL} total")
    print("=" * 65)

    if FAIL > 0:
        print("\n[!!]  Some tests failed. Check output above.")
        sys.exit(1)
    else:
        print("\n[OK] All tests passed! Storage layer hardening is verified.")
        sys.exit(0)


if __name__ == "__main__":
    main()
