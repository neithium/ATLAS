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
    
    log.info("🚀 Initializing Production Registry and DB Pool...")
    load_registry()
    await get_db_pool()
    
    log.info("⏱️  Starting Production DAILY_ARCHIVAL_JOB...")
    start_time = time.monotonic()
    
    try:
        await daily_archival_job()
        
        duration = time.monotonic() - start_time
        log.info(f"✅ JOB COMPLETE!")
        log.info(f"📊 EXACT TIME TAKEN: {duration:.2f} seconds ({(duration/60):.2f} minutes)")
        
        # 🔍 Verify the _SUCCESS file was written
        import glob
        import json
        success_files = glob.glob("/app/data/raw/production/year=*/month=*/day=*/full_7day/_SUCCESS")
        if success_files:
            latest_success = max(success_files, key=os.path.getmtime)
            log.info(f"📁 Found _SUCCESS file at: {latest_success}")
            with open(latest_success, "r") as f:
                metadata = json.load(f)
                log.info(f"📄 Metadata Payload: {json.dumps(metadata, indent=2)}")
        else:
            log.warning("⚠️ No _SUCCESS file found in /app/data/raw/production/year=*/month=*/day=*/full_7day/_SUCCESS!")
            
    except Exception as e:
        log.error(f"💥 Job failed: {e}")

if __name__ == "__main__":
    asyncio.run(run_bench())

"""
took around 6 mins to push 10k devices into raw directory.
100 per batch and 10 batches for 10000 devices 
can be optimized further by increasing the batch size and  
by assigning multiple workers to push the data to raw.
"""
# RUN COMMAND: docker exec atlas-ingestion python3 /app/v2/scripts/bench_daily_job.py