# TODO: Add inventory_data support to match input_schema

## Analysis

- The input_schema includes `inventory_data` with fields:
  - `cpu_count` (IntegerType)
  - `socket_count` (IntegerType)
  - `cpu_inventory` (Array of: model, speed, total_cores)
  - `memory_inventory` (Array of: memory_size, operating_freq, memory_device_type)

- Current code does NOT fetch or return this data

## Implementation Plan - COMPLETED

### ✅ Step 1: Add IPMI inventory fetching functions to `core/ipmi_reader.py`

- Added `_run_ipmitool()` helper function
- Added `_get_inventory_real()` function to fetch real BMC inventory using ipmitool
- Added `_get_inventory_mock()` function for dev/testing
- Added `fetch_inventory()` public function
- Commands used:
  - `ipmitool fru print` - Field Replaceable Unit info (CPU, memory)
  - `ipmitool dcmi info` - DCMI info including processor count
  - `ipmitool dcmi get memory_info` - Memory information (when available)

### ✅ Step 2: Update `core/response_builder.py`

- Added import for `fetch_inventory`
- Added inventory_data to the response structure in `build_response()`
- Added inventory_data to `_empty_response()` function

### ✅ Step 3: Add endpoint in `main.py`

- Added `GET /devices/{device_id}/inventory` endpoint
- Added import for `fetch_inventory`

## How to Test

### Testing Mock Mode (default - no real IPMI needed):

```bash
# Start the API
uvicorn main:app --reload

# Test the new inventory endpoint
curl http://localhost:8000/devices/DEV-SERVER-01/inventory

# Test full device data (includes inventory_data)
curl http://localhost:8000/devices/DEV-SERVER-01
```

### Testing with Real IPMI:

```bash
# Set MOCK_IPMI=false to use real IPMI commands
export MOCK_IPMI=false
export IPMI_HOST_01=your-bmc-ip
export IPMI_USER_01=admin
export IPMI_PASS_01=password

uvicorn main:app --reload
```

### IPMI Commands Used:

```bash
# CPU and FRU info
ipmitool -I lanplus -H <host> -U <user> -P <pass> fru print

# Processor count
ipmitool -I lanplus -H <host> -U <user> -P <pass> dcmi info

# Memory info (if available)
ipmitool -I lanplus -H <host> -U <user> -P <pass> dcmi get memory_info

# CPU sensors
ipmitool -I lanplus -H <host> -U <user> -P <pass> sensor list CPU
```
