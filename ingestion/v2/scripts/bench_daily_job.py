import asyncio
import time
import sys
import os
import logging

# Add the API directory to the path so we can import api_v2
# This assumes the script is in ingestion/v2/scripts/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'api'))

from api_v2 import daily_archival_job, load_registry, get_db_pool

async def run_bench():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    log = logging.getLogger("bench-archive")
    
    log.info("[DAILY JOB]Initializing Production Registry and DB Pool...")
    load_registry()
    await get_db_pool()
    
    log.info("=" * 70)
    log.info("⏱️  Starting Production DAILY_ARCHIVAL_JOB...")
    log.info("=" * 70)
    start_time = time.monotonic()
    
    try:
        await daily_archival_job()
        
        duration = time.monotonic() - start_time
        log.info("-" * 70)
        log.info(f"[DAILY JOB] JOB COMPLETE!")
        log.info(f"[DAILY JOB] EXACT TIME TAKEN: {duration:.2f} seconds ({(duration/60):.2f} minutes)")
        log.info("-" * 70)
        
        # ── Verify _SUCCESS marker in both RAW and ARCHIVE ──────────────
        import glob
        import json
        
        passed = True
        for store_name, base_path in [("RAW", "/app/data/raw"), ("ARCHIVE", "/app/data/archive")]:
            success_files = glob.glob(f"{base_path}/production/year=*/month=*/day=*/full_7day/_SUCCESS")
            if not success_files:
                log.error(f"[DAILY JOB] [{store_name}] No _SUCCESS file found!")
                passed = False
                continue
            
            latest_success = max(success_files, key=os.path.getmtime)
            log.info(f"[DAILY JOB] [{store_name}] _SUCCESS at: {latest_success}")
            
            with open(latest_success, "r") as f:
                metadata = json.load(f)
            log.info(f"[DAILY JOB] [{store_name}] Metadata: {json.dumps(metadata, indent=2)}")
            
            # Validate silo parquet files exist alongside _SUCCESS
            silo_dir = os.path.dirname(latest_success)
            silo_files = glob.glob(os.path.join(silo_dir, "daily_silo_*.parquet"))
            expected_silos = metadata.get("total_silos", 0)
            total_size_mb = sum(os.path.getsize(f) for f in silo_files) / (1024 * 1024)
            
            if len(silo_files) == expected_silos:
                log.info(f"[DAILY JOB] [{store_name}] Silo validation PASSED: {len(silo_files)}/{expected_silos} files | {total_size_mb:.1f} MB on disk")
            else:
                log.error(f"[DAILY JOB] [{store_name}] Silo validation FAILED: {len(silo_files)}/{expected_silos} files")
                passed = False
        
        if passed:
            log.info("[DAILY JOB] ALL VALIDATIONS PASSED")
        else:
            log.error("[DAILY JOB] VALIDATION FAILED — check missing files above")
            sys.exit(1)
            
    except Exception as e:
        log.error(f"[DAILY JOB] Job failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(run_bench())

# RUN COMMAND: docker exec atlas-ingestion python3 /app/v2/scripts/bench_daily_job.py