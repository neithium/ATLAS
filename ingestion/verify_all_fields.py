#!/usr/bin/env python3
"""
Detailed Kafka Message Field Presence Verifier
==============================================

Compares actual Kafka messages against the complete input schema
to verify ALL expected fields are present.

Usage:
    python3 verify_all_fields.py --samples=10
"""

import asyncio
import json
import sys
from collections import defaultdict, OrderedDict

try:
    from aiokafka import AIOKafkaConsumer
except ImportError:
    print("ERROR: aiokafka not installed. Run: pip install aiokafka")
    sys.exit(1)


# ───────────────────────────────────────────────────────────────
# COMPLETE SCHEMA FIELD DEFINITIONS
# ───────────────────────────────────────────────────────────────

EXPECTED_SCHEMA = {
    "top_level": [
        "device_id", "report_id", "created_at", "status", "model",
        "tags", "report_type", "server_name", "error_reason",
        "location_id", "location_city", "location_name",
        "location_state", "location_country", "processor_vendor",
        "server_generation", "platform_customer_id",
        "application_customer_id", "metric_type", "data", "inventory_data"
    ],
    "data": [
        "Id", "Average", "Maximum", "Minimum", "Name", "PowerDetail"
    ],
    "powerdetail": [
        "AmbTemp", "Average", "CpuAvgFreq", "CpuMax", "CpuPwrSavLim",
        "CpuUtil", "CpuWatts", "GpuWatts", "Minimum", "Peak", "Time"
    ],
    "inventory_data": [
        "cpu_count", "socket_count", "cpu_inventory", "memory_inventory"
    ],
    "cpu_inventory": [
        "model", "speed", "total_cores"
    ],
    "memory_inventory": [
        "memory_size", "operating_freq", "memory_device_type"
    ]
}


def flatten_dict(d, parent_key='', sep='.'):
    """Flatten nested dict to see all field paths."""
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list) and len(v) > 0 and isinstance(v[0], dict):
            # Handle arrays of objects
            for i, item in enumerate(v):
                items.extend(flatten_dict(item, f"{new_key}[{i}]", sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def check_fields(msg, msg_num):
    """
    Thoroughly check if message contains all expected fields.
    Returns detailed report of what's present/missing.
    """
    report = {
        "message_num": msg_num,
        "device_id": msg.get("device_id", "UNKNOWN"),
        "all_present": True,
        "missing_fields": [],
        "present_fields": [],
        "field_counts": {},
        "extra_fields": [],
        "nested_structure": {}
    }

    # ─── Check top-level fields ───
    for field in EXPECTED_SCHEMA["top_level"]:
        if field in msg:
            report["present_fields"].append(field)
        else:
            report["missing_fields"].append(field)
            report["all_present"] = False

    # ─── Check data object ───
    if "data" in msg:
        data = msg["data"]
        if isinstance(data, dict):
            report["nested_structure"]["data"] = {}
            for field in EXPECTED_SCHEMA["data"]:
                if field in data:
                    report["nested_structure"]["data"][field] = "✓"
                    report["present_fields"].append(f"data.{field}")
                else:
                    report["nested_structure"]["data"][field] = "✗ MISSING"
                    report["missing_fields"].append(f"data.{field}")
                    report["all_present"] = False

            # Check PowerDetail array
            if "PowerDetail" in data and isinstance(data["PowerDetail"], list):
                report["field_counts"]["PowerDetail_items"] = len(data["PowerDetail"])
                
                # Check first item detailed
                if len(data["PowerDetail"]) > 0:
                    first_pd = data["PowerDetail"][0]
                    report["nested_structure"]["PowerDetail[0]"] = {}
                    for field in EXPECTED_SCHEMA["powerdetail"]:
                        if field in first_pd:
                            report["nested_structure"]["PowerDetail[0]"][field] = "✓"
                            report["present_fields"].append(f"data.PowerDetail[0].{field}")
                        else:
                            report["nested_structure"]["PowerDetail[0]"][field] = "✗ MISSING"
                            report["missing_fields"].append(f"data.PowerDetail[0].{field}")
                            report["all_present"] = False

    # ─── Check inventory_data object ───
    if "inventory_data" in msg:
        inv = msg["inventory_data"]
        if isinstance(inv, dict):
            report["nested_structure"]["inventory_data"] = {}
            for field in EXPECTED_SCHEMA["inventory_data"]:
                if field in inv:
                    report["nested_structure"]["inventory_data"][field] = "✓"
                    report["present_fields"].append(f"inventory_data.{field}")
                else:
                    report["nested_structure"]["inventory_data"][field] = "✗ MISSING"
                    report["missing_fields"].append(f"inventory_data.{field}")
                    report["all_present"] = False

            # Check cpu_inventory array
            if "cpu_inventory" in inv and isinstance(inv["cpu_inventory"], list):
                report["field_counts"]["cpu_inventory_items"] = len(inv["cpu_inventory"])
                if len(inv["cpu_inventory"]) > 0:
                    first_cpu = inv["cpu_inventory"][0]
                    report["nested_structure"]["cpu_inventory[0]"] = {}
                    for field in EXPECTED_SCHEMA["cpu_inventory"]:
                        if field in first_cpu:
                            report["nested_structure"]["cpu_inventory[0]"][field] = "✓"
                            report["present_fields"].append(f"inventory_data.cpu_inventory[0].{field}")
                        else:
                            report["nested_structure"]["cpu_inventory[0]"][field] = "✗ MISSING"
                            report["missing_fields"].append(f"inventory_data.cpu_inventory[0].{field}")
                            report["all_present"] = False

            # Check memory_inventory array
            if "memory_inventory" in inv and isinstance(inv["memory_inventory"], list):
                report["field_counts"]["memory_inventory_items"] = len(inv["memory_inventory"])
                if len(inv["memory_inventory"]) > 0:
                    first_mem = inv["memory_inventory"][0]
                    report["nested_structure"]["memory_inventory[0]"] = {}
                    for field in EXPECTED_SCHEMA["memory_inventory"]:
                        if field in first_mem:
                            report["nested_structure"]["memory_inventory[0]"][field] = "✓"
                            report["present_fields"].append(f"inventory_data.memory_inventory[0].{field}")
                        else:
                            report["nested_structure"]["memory_inventory[0]"][field] = "✗ MISSING"
                            report["missing_fields"].append(f"inventory_data.memory_inventory[0].{field}")
                            report["all_present"] = False

    # Check for extra unexpected fields
    flattened = flatten_dict(msg)
    for field_path in flattened.keys():
        if not any(field_path.startswith(exp) for exp in report["present_fields"]):
            if field_path not in ["data.PowerDetail", "inventory_data.cpu_inventory", "inventory_data.memory_inventory"]:
                report["extra_fields"].append(field_path)

    return report


def print_detailed_report(report):
    """Print detailed field verification report."""
    device = report["device_id"]
    msg_num = report["message_num"]
    status = "✅ COMPLETE" if report["all_present"] else "❌ INCOMPLETE"

    print(f"\n{'='*80}")
    print(f"MSG {msg_num}: {status} - {device}")
    print(f"{'='*80}")

    # ─── Present fields ───
    print(f"\n✅ Present Fields ({len(report['present_fields'])} total):")
    for field in sorted(report["present_fields"])[:10]:
        print(f"   ✓ {field}")
    if len(report["present_fields"]) > 10:
        print(f"   ... and {len(report['present_fields']) - 10} more")

    # ─── Missing fields ───
    if report["missing_fields"]:
        print(f"\n❌ MISSING Fields ({len(report['missing_fields'])}):")
        for field in report["missing_fields"]:
            print(f"   ✗ {field}")

    # ─── Nested structure detail ───
    if report["nested_structure"]:
        print(f"\n📊 Nested Structure Details:")
        for obj_name, fields in report["nested_structure"].items():
            print(f"\n   {obj_name}:")
            for field_name, status_char in sorted(fields.items()):
                print(f"      {status_char:30s} {field_name}")

    # ─── Array counts ───
    if report["field_counts"]:
        print(f"\n📈 Array Item Counts:")
        for array_name, count in report["field_counts"].items():
            print(f"   {array_name}: {count} items")

    # ─── Extra fields ───
    if report["extra_fields"]:
        print(f"\n⚠️  Extra/Unexpected Fields ({len(report['extra_fields'])}):")
        for field in report["extra_fields"][:5]:
            print(f"   ⚠ {field}")
        if len(report["extra_fields"]) > 5:
            print(f"   ... and {len(report['extra_fields']) - 5} more")


async def main():
    print(f"\n🔍 Kafka Message Field Presence Verifier")
    print(f"{'='*80}")
    print(f"Checking if ALL schema fields are present in Kafka messages")
    print(f"{'='*80}\n")

    samples = 5
    consumer = None
    all_reports = []

    try:
        consumer = AIOKafkaConsumer(
            'raw-server-metrics',
            bootstrap_servers='broker1:9092',
            auto_offset_reset='latest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=60000,
            session_timeout_ms=30000,
            request_timeout_ms=60000
        )

        await consumer.start()
        print(f"📡 Connected to Kafka. Reading messages...\n")

        msg_count = 0
        async for msg in consumer:
            msg_count += 1
            try:
                value = msg.value
                report = check_fields(value, msg_count)
                all_reports.append(report)
                print_detailed_report(report)
            except Exception as e:
                print(f"❌ MSG {msg_count}: ERROR - {str(e)}")

            if msg_count >= samples:
                break

    except Exception as e:
        print(f"❌ Kafka connection error: {str(e)}")
        sys.exit(1)
    finally:
        if consumer:
            await consumer.stop()

    # ─── Summary Report ───
    print(f"\n\n{'='*80}")
    print(f"FIELD PRESENCE SUMMARY ({samples} messages sampled)")
    print(f"{'='*80}\n")

    complete_count = sum(1 for r in all_reports if r["all_present"])
    incomplete_count = len(all_reports) - complete_count

    print(f"📊 Overall Results:")
    print(f"   ✅ Complete messages (all fields): {complete_count}/{len(all_reports)}")
    print(f"   ❌ Incomplete messages (missing fields): {incomplete_count}/{len(all_reports)}")

    # Detailed missing fields across all messages
    all_missing = []
    for report in all_reports:
        all_missing.extend(report["missing_fields"])

    if all_missing:
        print(f"\n❌ Missing Fields Across All Messages:")
        missing_counts = defaultdict(int)
        for field in all_missing:
            missing_counts[field] += 1
        for field, count in sorted(missing_counts.items(), key=lambda x: -x[1]):
            print(f"   ✗ {field:40s} - Missing in {count}/{len(all_reports)} messages")
    else:
        print(f"\n✅ NO MISSING FIELDS - All schema fields present in all messages!")

    # Extra fields
    all_extra = []
    for report in all_reports:
        all_extra.extend(report["extra_fields"])

    if all_extra:
        print(f"\n⚠️  Extra Fields Found (not in schema):")
        extra_counts = defaultdict(int)
        for field in all_extra:
            extra_counts[field] += 1
        for field, count in sorted(extra_counts.items(), key=lambda x: -x[1])[:10]:
            print(f"   ⚠ {field:40s} - Found in {count}/{len(all_reports)} messages")

    # Exit code
    sys.exit(0 if complete_count == len(all_reports) else 1)


if __name__ == "__main__":
    asyncio.run(main())
