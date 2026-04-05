import asyncio
import uvicorn
from core.poller import start, LAST_POLL
from v2.api.api_v2 import app
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("v2-api")

@app.on_event("startup")
async def startup():
    log.info("Starting V2 TSDB + Poller API...")
    start(run_immediately=True)
    log.info("API + Poller ready!")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8002, log_level="debug")

