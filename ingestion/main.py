"""
main.py — Unified Entrypoint for PowerPulse V2 Ingestion
───────────────────────────────────────────────────────
Starts the FastAPI V2 interface and the background Poller.
Architecture: Hot/Cold (TimescaleDB + MinIO)
"""

import logging
import os
import uvicorn
from fastapi import FastAPI
from v2.api.api_v2 import app as api_app
from core.poller import start as start_poller, stop as stop_poller

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
log = logging.getLogger("main")

app = api_app

@app.on_event("startup")
async def startup_event():
    enable_poller = os.getenv("ENABLE_POLLER", "true").lower() == "true"
    if enable_poller:
        log.info("Starting Background Poller (5-min interval)...")
        start_poller(run_immediately=True)
    else:
        log.info("Background Poller is DISABLED via environment.")

@app.on_event("shutdown")
async def shutdown_event():
    log.info("Shutting down Poller...")
    stop_poller()

if __name__ == "__main__":
    port = int(os.getenv("API_PORT", "8000"))
    log.info(f"Starting Unified V2 API on port {port}...")
    uvicorn.run(app, host="0.0.0.0", port=port)
