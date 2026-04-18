#!/usr/bin/env python3
"""
Kafka Schema Consistency Verifier for PowerPulse
================================================

Pulls recent messages from raw-server-metrics topic and validates:
1. Schema compliance with input_schema.py
2. Field consistency across messages
3. Data type correctness
4. Required fields presence
5. Inventory data completeness

Usage:
    python3 verify_kafka_schema_consistency.py [--samples=50] [--topic=raw-server-metrics]
"""

import asyncio
import json
import sys
from collections import defaultdict
from datetime import datetime
import argparse

try:
    from aiokafka import AIOKafkaConsumer
except ImportError:
    print("ERROR: aiokafka not installed. Run: pip install aiokafka")
    sys.exit(1)


# ───────────────────────────────────────────────────────────────
# SCHEMA DEFINITIONS (per input_schema.py)
# ───────────────────────────────────────────────────────────────

REQUIRED_TOP_LEVEL = {
    "device_id", "report_id", "created_at", "status", "model", "tags",
    "report_type", "server_name", "error_reason", "location_id",
    "location_city", "location_name", "location_state", "location_country",
    "processor_vendor", "server_generation", "platform_customer_id",
    "application_customer_id", "metric_type", "data", "inventory_data"
}

REQUIRED_DATA = {"Id", "Average", "Maximum", "Minimum", "Name", "PowerDetail"}

REQUIRED_POWER_DETAIL = {
    "AmbTemp", "Average", "CpuAvgFreq", "CpuMax", "CpuPwrSavLim",
    "CpuUtil", "CpuWatts", "GpuWatts", "Minimum", "Peak", "Time"
}

REQUIRED_INVENTORY = {
    "cpu_count", "socket_count", "cpu_inventory", "memory_inventory"
}

REQUIRED_CPU_INVENTORY = {"model", "speed", "total_cores"}
REQUIRED_MEMORY_INVENTORY = {"memory_size", "operating_freq", "memory_device_type"}

# Expected data types
EXPECTED_TYPES = {
    # Top-level strings
    "device_id": str, "report_id": str, "created_at": str, "model": str,
    "tags": str, "report_type": str, "server_name": str, "error_reason": str,
    "location_id": str, "location_city": str, "location_name": str,
    "location_state": str, "location_country": str, "processor_vendor": str,
    "server_generation": str, "platform_customer_id": str,
    "application_customer_id": str, "metric_type": str,
    # Top-level boolean
    "status": bool,
    # Complex types
    "data": dict, "inventory_data": dict
}


class MessageValidator:
    def __init__(self):
        self.valid_count = 0
        self.invalid_count = 0
        self.errors_by_type = defaultdict(list)
        self.field_coverage = defaultdict(lambda: {"present": 0, "missing": 0})
        self.sample_messages = []
        self.data_types_found = defaultdict(set)

    def validate_message(self, msg: dict, msg_num: int) -> bool:
        """
        Validates a single message against input_schema.py.
        Returns True if valid, False otherwise.
        """
        errors = []

        # ─── Check data type ───
        if not isinstance(msg, dict):
            self.errors_by_type["type_error"].append(f"MSG {msg_num}: Not a dict, is {type(msg).__name__}")
            return False

        # ─── Check top-level fields ───
        missing_top = REQUIRED_TOP_LEVEL - set(msg.keys())
        if missing_top:
            self.errors_by_type["missing_top_level"].append(
                f"MSG {msg_num}: Missing fields: {missing_top}"
            )
            errors.append(f"Missing top-level fields: {missing_top}")

        # Track field coverage
        for field in REQUIRED_TOP_LEVEL:
            if field in msg:
                self.field_coverage[field]["present"] += 1
            else:
                self.field_coverage[field]["missing"] += 1

        # ─── Validate data types ───
        for field, expected_type in EXPECTED_TYPES.items():
            if field in msg:
                actual_type = type(msg[field]).__name__
                self.data_types_found[field].add(actual_type)
                
                if expected_type == bool and isinstance(msg[field], bool):
                    pass  # OK
                elif expected_type == dict and isinstance(msg[field], dict):
                    pass  # OK
                elif not isinstance(msg[field], expected_type):
                    self.errors_by_type["type_mismatch"].append(
                        f"MSG {msg_num} field '{field}': expected {expected_type.__name__}, got {actual_type}"
                    )
                    errors.append(f"Type mismatch in '{field}'")

        # ─── Check data block ───
        if "data" in msg:
            data = msg["data"]
            if not isinstance(data, dict):
                self.errors_by_type["data_not_dict"].append(
                    f"MSG {msg_num}: 'data' must be dict, got {type(data).__name__}"
                )
                errors.append("'data' must be dict")
            else:
                missing_data = REQUIRED_DATA - set(data.keys())
                if missing_data:
                    self.errors_by_type["missing_data_fields"].append(
                        f"MSG {msg_num}: Missing data fields: {missing_data}"
                    )
                    errors.append(f"Missing data fields: {missing_data}")

                # Check PowerDetail
                if "PowerDetail" in data:
                    if not isinstance(data["PowerDetail"], list):
                        self.errors_by_type["powerdetail_not_list"].append(
                            f"MSG {msg_num}: 'data.PowerDetail' must be array, got {type(data['PowerDetail']).__name__}"
                        )
                        errors.append("'data.PowerDetail' must be list")
                    elif len(data["PowerDetail"]) == 0:
                        self.errors_by_type["powerdetail_empty"].append(
                            f"MSG {msg_num}: 'data.PowerDetail' is empty"
                        )
                    else:
                        # Validate PowerDetail items
                        for i, pd in enumerate(data["PowerDetail"]):
                            if not isinstance(pd, dict):
                                self.errors_by_type["powerdetail_item_not_dict"].append(
                                    f"MSG {msg_num} PowerDetail[{i}]: not a dict"
                                )
                                continue
                            
                            missing_pd = REQUIRED_POWER_DETAIL - set(pd.keys())
                            if missing_pd:
                                self.errors_by_type["missing_powerdetail_fields"].append(
                                    f"MSG {msg_num} PowerDetail[{i}]: Missing fields: {missing_pd}"
                                )
                                errors.append(f"Missing PowerDetail fields: {missing_pd}")

        # ─── Check inventory_data ───
        if "inventory_data" in msg:
            inv = msg["inventory_data"]
            if not isinstance(inv, dict):
                self.errors_by_type["inventory_not_dict"].append(
                    f"MSG {msg_num}: 'inventory_data' must be dict, got {type(inv).__name__}"
                )
                errors.append("'inventory_data' must be dict")
            else:
                missing_inv = REQUIRED_INVENTORY - set(inv.keys())
                if missing_inv:
                    self.errors_by_type["missing_inventory_fields"].append(
                        f"MSG {msg_num}: Missing inventory fields: {missing_inv}"
                    )
                    errors.append(f"Missing inventory fields: {missing_inv}")

                # Check cpu_inventory
                if "cpu_inventory" in inv:
                    if not isinstance(inv["cpu_inventory"], list):
                        self.errors_by_type["cpu_inventory_not_list"].append(
                            f"MSG {msg_num}: 'cpu_inventory' must be array"
                        )
                    else:
                        for i, cpu in enumerate(inv["cpu_inventory"]):
                            if isinstance(cpu, dict):
                                missing_cpu = REQUIRED_CPU_INVENTORY - set(cpu.keys())
                                if missing_cpu:
                                    self.errors_by_type["missing_cpu_inventory_fields"].append(
                                        f"MSG {msg_num} cpu[{i}]: Missing fields: {missing_cpu}"
                                    )

                # Check memory_inventory
                if "memory_inventory" in inv:
                    if not isinstance(inv["memory_inventory"], list):
                        self.errors_by_type["memory_inventory_not_list"].append(
                            f"MSG {msg_num}: 'memory_inventory' must be array"
                        )
                    else:
                        for i, mem in enumerate(inv["memory_inventory"]):
                            if isinstance(mem, dict):
                                missing_mem = REQUIRED_MEMORY_INVENTORY - set(mem.keys())
                                if missing_mem:
                                    self.errors_by_type["missing_memory_inventory_fields"].append(
                                        f"MSG {msg_num} memory[{i}]: Missing fields: {missing_mem}"
                                    )

        # Record result
        if errors:
            self.invalid_count += 1
            print(f"❌ MSG {msg_num}: FAIL - {device_id(msg)}")
            for error in errors[:3]:  # Show first 3 errors
                print(f"   └─ {error}")
            if len(errors) > 3:
                print(f"   └─ ... and {len(errors) - 3} more errors")
            return False
        else:
            self.valid_count += 1
            device = device_id(msg)
            print(f"✅ MSG {msg_num}: PASS - {device}")
            self.sample_messages.append({"message_num": msg_num, "device": device})
            return True

    def print_summary(self):
        """Print validation summary."""
        print("\n" + "=" * 80)
        print("SCHEMA CONSISTENCY VERIFICATION SUMMARY")
        print("=" * 80)
        
        total = self.valid_count + self.invalid_count
        pass_rate = (self.valid_count / total * 100) if total > 0 else 0
        
        print(f"\n📊 Results:")
        print(f"   ✅ Valid messages:   {self.valid_count}/{total} ({pass_rate:.1f}%)")
        print(f"   ❌ Invalid messages: {self.invalid_count}/{total}")
        
        if self.invalid_count > 0:
            print(f"\n⚠️  Error Breakdown:")
            for error_type, messages in sorted(self.errors_by_type.items(), key=lambda x: -len(x[1])):
                print(f"   • {error_type}: {len(messages)} occurrence(s)")
                if len(messages) <= 3:
                    for msg in messages:
                        print(f"     - {msg}")
                else:
                    for msg in messages[:2]:
                        print(f"     - {msg}")
                    print(f"     - ... and {len(messages) - 2} more")
        
        print(f"\n📋 Field Coverage:")
        for field in sorted(REQUIRED_TOP_LEVEL):
            coverage = self.field_coverage[field]
            if coverage["present"] > 0:
                coverage_pct = (coverage["present"] / total * 100) if total > 0 else 0
                status = "✅" if coverage["present"] == total else "⚠️"
                print(f"   {status} {field:30s}: {coverage['present']}/{total} ({coverage_pct:.1f}%)")

        print(f"\n🔍 Data Types Found:")
        for field in sorted(self.data_types_found.keys()):
            types = self.data_types_found[field]
            expected = EXPECTED_TYPES.get(field, "unknown")
            if len(types) == 1 and expected.__name__ in str(list(types)[0]):
                print(f"   ✅ {field:30s}: {list(types)[0]}")
            else:
                print(f"   ⚠️  {field:30s}: {', '.join(sorted(types))} (expected: {expected.__name__})")


def device_id(msg: dict) -> str:
    """Safe device_id extraction."""
    return msg.get("device_id", msg.get("data", {}).get("Id", "UNKNOWN"))


async def main():
    parser = argparse.ArgumentParser(description="Verify Kafka schema consistency")
    parser.add_argument("--samples", type=int, default=50, help="Number of messages to sample (default: 50)")
    parser.add_argument("--topic", default="raw-server-metrics", help="Kafka topic to read from")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--timeout", type=int, default=60, help="Timeout in seconds waiting for messages")
    
    args = parser.parse_args()

    print(f"🔍 Kafka Schema Consistency Verifier")
    print(f"   Topic: {args.topic}")
    print(f"   Bootstrap: {args.bootstrap}")
    print(f"   Samples: {args.samples}")
    print(f"   Timeout: {args.timeout}s\n")

    validator = MessageValidator()
    consumer = None

    try:
        consumer = AIOKafkaConsumer(
            args.topic,
            bootstrap_servers=args.bootstrap,
            auto_offset_reset='latest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=args.timeout * 1000,
            session_timeout_ms=30000,
            request_timeout_ms=60000
        )
        
        await consumer.start()
        print(f"📡 Connected to Kafka. Waiting for messages...\n")

        msg_count = 0
        async for msg in consumer:
            msg_count += 1
            try:
                value = msg.value
                validator.validate_message(value, msg_count)
            except Exception as e:
                validator.errors_by_type["parse_error"].append(f"MSG {msg_count}: {str(e)}")
                print(f"❌ MSG {msg_count}: ERROR parsing message - {str(e)[:80]}")

            if msg_count >= args.samples:
                break

        if msg_count == 0:
            print("⚠️  No messages received within timeout period.")
            print("   Check if Kafka is running and the topic has data.")

    except Exception as e:
        print(f"❌ Kafka connection error: {str(e)}")
        print(f"   Check: bootstrap servers, topic name, and Kafka connectivity")
        sys.exit(1)
    finally:
        if consumer:
            await consumer.stop()

    # Print summary
    validator.print_summary()

    # Exit code based on validation
    sys.exit(0 if validator.invalid_count == 0 else 1)


if __name__ == "__main__":
    asyncio.run(main())
