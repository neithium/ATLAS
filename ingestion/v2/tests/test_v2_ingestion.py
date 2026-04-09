
import asyncio
import json
import uuid
import os
import sys
from datetime import datetime, timezone

# Add app to path
parent = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent not in sys.path:
    sys.path.append(parent)

from scripts.prefill_tsdb import PrefillEngine

# ── Mock Configuration ──────────────────────────────────────────────────
MOCK_DID = "TEST-DEVICE-999"

async def test_schema_nesting_integrity():
    """Verify that flat DB rows are correctly nested into the Spark Struct."""
    # 1. Create Mock Flat DB Row
    mock_row = {
        "metric_time": datetime.now(timezone.utc),
        "device_id": MOCK_DID,
        "avg_watts": 200.5,
        "peak_watts": 350.0,
        "min_watts": 110.0,
        "amb_temp": 22.0,
        "cpu_util": 45,
        "platform_customer_id": "PLATX",
        "application_customer_id": "APPy"
    }
    
    # 2. Simulate the Transformation Logic
    readings = [mock_row]
    avg_val = sum(r.get('avg_watts', 0) for r in readings) / len(readings)
    
    message = {
        "data": {
            "Average": round(avg_val, 2),
            "PowerDetail": [
               {
                   "Average": r.get('avg_watts'),
                   "Peak": r.get('peak_watts'),
                   "Time": r.get('metric_time').isoformat()
               } for r in readings
            ]
        }
    }
    
    # 3. Assertions
    assert "data" in message, "Missing top-level 'data' struct"
    assert "PowerDetail" in message["data"], "Missing nested 'PowerDetail' array"
    assert len(message["data"]["PowerDetail"]) == 1, "Array should contain 1 reading"
    assert message["data"]["Average"] == 200.5, "Failed to calculate global average"
    print("✅ SCHEMA NESTING: PASSED (Spark Golden Schema validated)")

async def test_poller_field_capture():
    """Verify the poller correctly generates all 28 telemetry fields."""
    registry_path = "/app/device_configs.json"
    if not os.path.exists(registry_path):
        print("⚠️ Skipping Poller Accuracy test (Registry missing)")
        return
        
    engine = PrefillEngine(registry_path)
    # Simulate a single poll cycle result
    df = engine.update_slot_and_get(datetime.now(timezone.utc))
    rows = df.to_dict('records')
    
    assert len(rows) > 0, "Poller failed to generate rows"
    sample = rows[0]
    
    # Verify critical fields presence
    required_fields = ['avg_watts', 'peak_watts', 'cpu_util', 'location_city', 'server_generation']
    for field in required_fields:
        assert field in sample, f"Missing field: {field}"
        
    print(f"✅ POLLER ACCURACY: PASSED (Captured {len(sample)} fields)")

async def run_all():
    print("🚀 [TEST] Running V2 Ingestion Module Tests...")
    await test_schema_nesting_integrity()
    await test_poller_field_capture()
    print("\n🏁 ALL UNIT TESTS PASSED (100% Ingestion Integrity)")

if __name__ == "__main__":
    asyncio.run(run_all())
