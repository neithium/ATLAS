"""
core/response_builder.py
------------------------
Assembles the final JSON document that the API returns.

Data composition per response
──────────────────────────────
  Redis  : last 288 readings  (24 hours)   ← recent data
  MinIO  : previous 1728 readings (6 days) ← historical data
  Combined: 2016 readings     (7 days)

The output matches input_schema exactly:
  top-level metadata  → from DEVICES registry
  data.PowerDetail   → 2016-entry array (oldest → newest)
  data.Average/Max/Min→ computed from PowerDetail
  inventory_data     → CPU and memory inventory from IPMI
  summary            → aggregated stats

Warmup handling
───────────────
  - Never crashes - returns available data instead of failing
  - coverage_pct - tells clients exact completion % (Day 1: ~8.9%, Day 7+: 100%)
  - complete - boolean explicitly indicates full 7-day data availability
"""

import asyncio
import logging
import uuid
from datetime import datetime, timezone
from typing import Optional

from config.devices import DEVICES, TOTAL_READINGS, FRESH_READINGS, REDIS_READINGS
from core.redis_store import get_history
from core.ipmi_reader import fetch_inventory

log = logging.getLogger(__name__)


async def build_response(device_id: str, preloaded_readings: list[dict] = None) -> dict:
    """
    Build the complete JSON document for a device_id.

    Reading composition:
      • Fetches from Redis (last 288 readings = 24 hours)
      • Fetches from MinIO (previous 1728 readings = 6 days)
      • Merges both sources for full 7-day coverage
      
    If preloaded_readings is provided, use that data instead of fetching from Redis/MinIO.
    """
    meta     = DEVICES[device_id]

    # ── Pull from Redis and MinIO (or use preloaded) ─────────────────────────
    if preloaded_readings is not None:
        all_readings = preloaded_readings
    else:
        from core.history_service import get_combined_history
        all_readings = await get_combined_history(device_id, last_n=TOTAL_READINGS)
        

    if not all_readings:
        return await _empty_response(device_id, meta)

    total = len(all_readings)
    coverage_pct = round(total / TOTAL_READINGS * 100, 1)
    complete = total >= TOTAL_READINGS

    # ── Annotate which readings are "fresh" vs "historical" ──────────────────
    # Last FRESH_READINGS (12) = fresh, everything before = historical
    fresh_start_idx = max(0, total - FRESH_READINGS)

    for i, r in enumerate(all_readings):
        r["is_fresh"] = i >= fresh_start_idx   # True for last 12 readings

    # ── Compute summary stats ─────────────────────────────────────────────────
    powers   = [r["Average"]  for r in all_readings if r.get("Average")  is not None]
    peaks    = [r["Peak"]     for r in all_readings if r.get("Peak")     is not None]
    mins     = [r["Minimum"]  for r in all_readings if r.get("Minimum")  is not None]
    cpu_utils= [r["CpuUtil"]  for r in all_readings if r.get("CpuUtil")  is not None]
    temps    = [r["AmbTemp"]  for r in all_readings if r.get("AmbTemp")  is not None]

    interval_h   = 5 / 60          # 5-min intervals → fraction of hour
    total_energy = round(sum(p * interval_h / 1000 for p in powers), 4)

    avg_power = round(sum(powers) / len(powers), 3) if powers else None
    max_power = float(max(peaks))                   if peaks  else None
    min_power = float(min(mins))                    if mins   else None

    # ── Assemble document (matches input_schema) ─────────────────────────────
    
    # Fetch inventory data from IPMI (or mock)
    try:
        inventory_data = fetch_inventory(
            device_id    = device_id,
            ipmi_host    = meta["ipmi_host"],
            ipmi_user    = meta["ipmi_user"],
            ipmi_password= meta["ipmi_password"],
            ipmi_port    = meta.get("ipmi_port", 623),
        )
    except Exception as e:
        log.warning(f"Could not fetch inventory for {device_id}: {e}")
        inventory_data = {"cpu_count": 0, "socket_count": 0, "cpu_inventory": [], "memory_inventory": []}
    
    return {
        # ── metadata ─────────────────────────────────────────────────────────
        "report_id"              : str(uuid.uuid4()),
        "created_at"             : datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "schema_version"         : "1.0.0",
        "report_type"            : "power",
        "metric_type"            : "power_consumption",
        "status"                 : True,
        "error_reason"           : None,
        "model"                  : meta["model"],
        "tags"                   : "datacenter,power,ipmi",
        "device_id"              : device_id,
        "server_name"            : meta["server_name"],
        "location_id"            : meta["location_id"],
        "location_city"          : meta["location_city"],
        "location_state"         : meta["location_state"],
        "location_country"       : meta["location_country"],
        "location_name"          : meta["location_name"],
        "processor_vendor"       : meta["processor_vendor"],
        "server_generation"      : meta["server_generation"],
        "platform_customer_id"   : meta["platform_customer_id"],
        "application_customer_id": meta["application_customer_id"],

        # ── warmup status ────────────────────────────────────────────────────
        "coverage_pct"           : coverage_pct,
        "complete"               : complete,
        "data_source"            : {
            "redis_count"        : min(total, REDIS_READINGS),
            "minio_count"        : max(0, total - REDIS_READINGS),
        },

        # ── data block ───────────────────────────────────────────────────────
        "data": {
            "Id"         : str(uuid.uuid4()),
            "Average"    : avg_power,
            "Maximum"    : max_power,
            "Minimum"    : min_power,
            "Name"       : f"PowerReport-{device_id}",
            "PowerDetail": all_readings,      # ← up to 2016-entry nested array
        },
        
        # ── inventory data ───────────────────────────────────────────────────
        "inventory_data": inventory_data,

        # ── summary ───────────────────────────────────────────────────────────
        "summary": {
            "period_start"        : all_readings[0]["Time"],
            "period_end"          : all_readings[-1]["Time"],
            "total_readings"      : total,
            "fresh_readings"      : min(FRESH_READINGS, total),
            "historical_readings" : max(0, total - FRESH_READINGS),
            "avg_active_power_w"  : avg_power,
            "peak_w"              : int(max_power) if max_power else None,
            "min_w"               : int(min_power) if min_power else None,
            "avg_cpu_util_pct"    : round(sum(cpu_utils)/len(cpu_utils), 2) if cpu_utils else None,
            "avg_amb_temp_c"      : round(sum(temps)/len(temps), 2)         if temps     else None,
            "total_energy_kwh"    : total_energy,
            "coverage_pct"        : coverage_pct,
            "complete"            : complete,
        },
    }


async def _empty_response(device_id: str, meta: dict) -> dict:
    """Returned when no readings are buffered yet for a device."""
    # Try to fetch inventory even when no readings
    try:
        inventory_data = fetch_inventory(
            device_id    = device_id,
            ipmi_host    = meta["ipmi_host"],
            ipmi_user    = meta["ipmi_user"],
            ipmi_password= meta["ipmi_password"],
            ipmi_port    = meta.get("ipmi_port", 623),
        )
    except Exception:
        inventory_data = {"cpu_count": 0, "socket_count": 0, "cpu_inventory": [], "memory_inventory": []}
    
    return {
        "report_id"   : str(uuid.uuid4()),
        "created_at"  : datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "schema_version": "1.0.0",
        "device_id"   : device_id,
        "server_name" : meta["server_name"],
        "model"       : meta.get("model"),
        "tags"        : "datacenter,power,ipmi",
        "status"      : False,
        "error_reason": "No readings buffered yet. "
                        "Poller may not have run or device is unreachable.",
        "location_id"            : meta.get("location_id"),
        "location_city"         : meta.get("location_city"),
        "location_state"         : meta.get("location_state"),
        "location_country"       : meta.get("location_country"),
        "location_name"          : meta.get("location_name"),
        "processor_vendor"       : meta.get("processor_vendor"),
        "server_generation"      : meta.get("server_generation"),
        "platform_customer_id"   : meta.get("platform_customer_id"),
        "application_customer_id": meta.get("application_customer_id"),
        "report_type"            : "power",
        "metric_type"            : "power_consumption",
        "data"       : {"Id": None, "Average": None, "Maximum": None,
                        "Minimum": None, "Name": None, "PowerDetail": []},
        "inventory_data": inventory_data,
        "summary"    : None,
        # ── warmup status (even for empty) ─────────────────────────────────
        "coverage_pct": 0.0,
        "complete"   : False,
    }

