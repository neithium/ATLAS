import pytest
import asyncio
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock

# Import the FastAPI app from our api_v2 module
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'api')))
from api_v2 import app, CACHED_REGISTRY, HIERARCHY_INDEX, ACTIVE_HIERARCHIES

client = TestClient(app)

# =============================================================================
# MOCK FIXTURES (Prevents the need for Docker/Real Databases)
# =============================================================================

@pytest.fixture(autouse=True)
def mock_dependencies(tmp_path):
    """
    Automatically mock out Postgres, Kafka, and the background task executor
    so tests run instantly without needing Docker containers.
    Also redirect the hardcoded Docker paths to safe temporary directories.
    """
    dummy_registry = tmp_path / "device_configs.json"
    
    with patch('api_v2.get_db_pool', new_callable=asyncio.Future) as mock_pool, \
         patch('api_v2.get_kafka', new_callable=asyncio.Future) as mock_kafka, \
         patch('api_v2.BackgroundTasks.add_task') as mock_bg_task, \
         patch('api_v2.REGISTRY_PATH', str(dummy_registry)):
        
        # Resolve the futures instantly with dummy objects
        mock_pool.set_result(MagicMock())
        mock_kafka.set_result(MagicMock())
        
        yield

@pytest.fixture(autouse=True)
def clean_state():
    """Ensure every test starts with a clean registry and lock state."""
    CACHED_REGISTRY.clear()
    HIERARCHY_INDEX.clear()
    ACTIVE_HIERARCHIES.clear()
    yield

# =============================================================================
# TESTS
# =============================================================================

def test_health_endpoint():
    """Test that the deep health probe works without crashing."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert "status" in data
    assert data["status"] == "online"

def test_register_device():
    """Test that a new device is hot-loaded into the RAM registry successfully."""
    payload = {
        "device_id": "TEST-DEV-001",
        "application_customer_id": "ACID_123",
        "platform_customer_id": "PCID_456",
        "server_name": "Test Server",
        "location_city": "Austin",
        "location_country": "USA",
        "inventory_data": {}
    }
    
    response = client.post("/register/device", json=payload)
    
    # Assert successful HTTP response
    assert response.status_code == 200
    assert response.json()["status"] == "success"
    
    # Assert it was properly added to the Python CACHED_REGISTRY
    assert "TEST-DEV-001" in CACHED_REGISTRY
    assert CACHED_REGISTRY["TEST-DEV-001"]["application_customer_id"] == "ACID_123"

def test_export_empty_hierarchy():
    """Test that requesting an export for a non-existent hierarchy fails gracefully."""
    response = client.post("/pcid/FAKE_PCID/acid/FAKE_ACID/telemetry/latest/export")
    assert response.status_code == 200
    assert response.json()["status"] == "Empty Hierarchy"

def test_export_concurrency_lock():
    """Test that the system prevents two overlapping exports for the same customer."""
    
    # 1. Manually add a fake device so the hierarchy exists
    CACHED_REGISTRY["DEV-01"] = {
        "platform_customer_id": "PCID_1",
        "application_customer_id": "ACID_1"
    }
    HIERARCHY_INDEX[("PCID_1", "ACID_1")] = ["DEV-01"]
    
    # 2. Simulate an active export running
    ACTIVE_HIERARCHIES.add("PCID_1:ACID_1")
    
    # 3. Attempt to trigger another export for the same hierarchy
    response = client.post("/pcid/PCID_1/acid/ACID_1/telemetry/latest/export")
    
    # 4. It should safely block it
    assert response.status_code == 200
    assert response.json()["status"] == "error"
    assert "already active" in response.json()["message"]

def test_hydration_transformation():
    """
    Test the core PyArrow vectorized transformation engine.
    Ensures that raw TSDB rows are correctly aggregated (Average, Min, Max)
    and perfectly mapped to the PascalCase Golden Record schema.
    """
    import pyarrow as pa
    import orjson
    from datetime import datetime, timezone
    from api_v2 import process_device_batch_hydration, _WORKER_REGISTRY
    
    # 1. Setup mock worker registry for metadata injection
    _WORKER_REGISTRY["DEV-999"] = {
        "server_name": "Test-Server-999",
        "model": "Dell PowerEdge",
        "platform_customer_id": "PCID_TEST",
        "application_customer_id": "ACID_TEST"
    }
    
    # 2. Create a fake PyArrow table representing raw DB rows
    # We will pass 3 rows for the same device to test aggregation math.
    now = datetime.utcnow()
    data = {
        "device_id": ["DEV-999", "DEV-999", "DEV-999"],
        "metric_time": [now, now, now],
        "amb_temp": [22.0, 23.0, 24.0],
        "avg_watts": [100.0, 200.0, 300.0],  # Average should be 200.0
        "cpu_avg_freq": [2500, 2500, 2500],
        "cpu_max": [3500, 3500, 3500],
        "cpu_pwr_sav_lim": [250, 250, 250],
        "cpu_util": [50, 60, 70],
        "cpu_watts": [50, 60, 70],
        "gpu_watts": [10, 10, 10],
        "min_watts": [90.0, 190.0, 290.0],   # Min should be 90.0
        "peak_watts": [150.0, 250.0, 350.0]  # Max should be 350.0
    }
    table = pa.table(data)
    
    # 3. Execute the transformation function
    # Mock pyarrow.compute.strftime to avoid the Windows tzdata bug on local testing
    from unittest.mock import patch
    import pyarrow.compute as pc
    
    def mock_strftime(col, format):
        return col.cast(pa.string())
        
    with patch('pyarrow.compute.strftime', side_effect=mock_strftime):
        results = process_device_batch_hydration(table, count=100)
    
    # 4. Assertions
    assert len(results) == 1
    device_id, json_payload = results[0]
    
    assert device_id == "DEV-999"
    
    # Parse the resulting JSON
    parsed = orjson.loads(json_payload)
    
    # Check Metadata Hydration
    assert parsed["server_name"] == "Test-Server-999"
    assert parsed["platform_customer_id"] == "PCID_TEST"
    
    # Check Aggregation Math
    data_block = parsed["data"]
    assert data_block["Average"] == 200.0
    assert data_block["Minimum"] == 90.0
    assert data_block["Maximum"] == 350.0
    
    # Check PascalCase mapping (orjson.loads automatically parses fragments into lists)
    power_details = data_block["PowerDetail"]
    
    assert len(power_details) == 3
    assert "AmbTemp" in power_details[0]
    assert power_details[0]["AmbTemp"] in [22.0, 23.0, 24.0]
    assert power_details[0]["Average"] in [100.0, 200.0, 300.0]

def test_daily_archival_job():
    """
    Test the daily archival job.
    Ensures that it fetches data from the DB, batches it, formats it via the Golden Record schema, 
    and simulates writing Parquet silos + _SUCCESS markers.
    """
    from api_v2 import daily_archival_job, CACHED_REGISTRY
    from unittest.mock import patch, MagicMock, mock_open
    from datetime import datetime, timezone
    
    # 1. Setup mock registry
    CACHED_REGISTRY.clear()
    CACHED_REGISTRY["DEV-ARCHIVE-TEST"] = {
        "model": "TestArchival",
        "inventory_data": {},
        "platform_customer_id": "PCID",
        "application_customer_id": "ACID"
    }
    
    # 2. Setup mock DB pool to return dummy TSDB records
    mock_pool = MagicMock()
    mock_conn = MagicMock()
    # Mock the async context manager: async with pool.acquire() as conn:
    mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
    
    now = datetime.utcnow()
    fake_record = {
        "device_id": "DEV-ARCHIVE-TEST", "metric_time": now, "amb_temp": 25.0,
        "avg_watts": 100.0, "cpu_avg_freq": 2000, "cpu_max": 3000,
        "cpu_pwr_sav_lim": 200, "cpu_util": 50, "cpu_watts": 50,
        "gpu_watts": 10, "min_watts": 50.0, "peak_watts": 150.0,
        "server_name": "S1"
    }
    # Mocking conn.fetch() to return our fake record
    # Note: async mock requires resolving an awaitable
    import asyncio
    future = asyncio.Future()
    future.set_result([fake_record])
    mock_conn.fetch.return_value = future
    
    # 3. Patch disk and DB
    with patch('api_v2.get_db_pool', return_value=future) as mock_get_pool, \
         patch('api_v2.os.makedirs') as mock_makedirs, \
         patch('builtins.open', mock_open()) as mock_file, \
         patch('api_v2.pq.ParquetWriter') as mock_pq_writer, \
         patch('api_v2.os.path.getsize', return_value=1024): # mock getsize for log info
         
        mock_get_pool.return_value = mock_pool
        import asyncio
        asyncio.run(daily_archival_job())
         
    # 4. Assertions
    # Verify it attempted to create exactly 2 directories (raw and archive)
    assert mock_makedirs.call_count == 2
    
    # Verify it opened files to write the Parquet Silos and the _SUCCESS metadata
    # 2 writes for parquet (raw + archive) + 2 writes for _SUCCESS (raw + archive) = 4 file writes
    assert mock_file.call_count == 4
    
    # Verify the ParquetWriter was instantiated and write_table was called
    assert mock_pq_writer.called
    assert mock_pq_writer.return_value.write_table.called
    assert mock_pq_writer.return_value.close.called
