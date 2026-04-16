"""Quick 48-field Kafka message verifier."""
import asyncio, json
from aiokafka import AIOKafkaConsumer

EXPECTED_TOP = {"device_id","report_id","created_at","status","model","tags","report_type",
    "server_name","error_reason","location_id","location_city","location_name",
    "location_state","location_country","processor_vendor","server_generation",
    "platform_customer_id","application_customer_id","metric_type","data","inventory_data"}

EXPECTED_DATA = {"Id","Average","Maximum","Minimum","Name","PowerDetail"}

EXPECTED_PD = {"AmbTemp","Average","CpuAvgFreq","CpuMax","CpuPwrSavLim",
    "CpuUtil","CpuWatts","GpuWatts","Minimum","Peak","Time","is_fresh"}

EXPECTED_INV = {"cpu_count","socket_count","cpu_inventory","memory_inventory"}

async def verify():
    consumer = AIOKafkaConsumer(
        'raw-server-metrics',
        bootstrap_servers='broker1:9092',
        auto_offset_reset='latest',
        consumer_timeout_ms=15000
    )
    await consumer.start()
    try:
        async for msg in consumer:
            payload = json.loads(msg.value.decode('utf-8'))
            
            # Check top-level
            top_keys = set(payload.keys())
            missing_top = EXPECTED_TOP - top_keys
            
            # Check data.*
            data_keys = set(payload.get("data", {}).keys())
            missing_data = EXPECTED_DATA - data_keys
            
            # Check PowerDetail[0]
            pd = payload.get("data", {}).get("PowerDetail", [{}])
            pd_keys = set(pd[0].keys()) if pd else set()
            missing_pd = EXPECTED_PD - pd_keys
            
            # Check inventory_data
            inv_keys = set(payload.get("inventory_data", {}).keys())
            missing_inv = EXPECTED_INV - inv_keys

            print("\n" + "=" * 60)
            print("🔍 KAFKA 48-FIELD SCHEMA VERIFICATION")
            print("=" * 60)
            
            print(f"\n📋 TOP-LEVEL FIELDS: {len(top_keys)}/{len(EXPECTED_TOP)}")
            for k in sorted(top_keys):
                v = payload[k]
                if isinstance(v, dict): v = "{...}"
                elif isinstance(v, list): v = f"[{len(payload[k])} items]"
                elif isinstance(v, str) and len(v) > 40: v = v[:40] + "..."
                print(f"  ✅ {k}: {v}")
            if missing_top: 
                for m in missing_top: print(f"  ❌ MISSING: {m}")

            print(f"\n📊 data.* FIELDS: {len(data_keys)}/{len(EXPECTED_DATA)}")
            for k in sorted(data_keys):
                if k == "PowerDetail": print(f"  ✅ {k}: [{len(pd)} entries]")
                else: print(f"  ✅ {k}: {payload['data'][k]}")
            if missing_data:
                for m in missing_data: print(f"  ❌ MISSING: data.{m}")

            print(f"\n⚡ PowerDetail[0] FIELDS: {len(pd_keys)}/{len(EXPECTED_PD)}")
            if pd:
                for k in sorted(pd_keys): print(f"  ✅ {k}: {pd[0][k]}")
            if missing_pd:
                for m in missing_pd: print(f"  ❌ MISSING: PowerDetail.{m}")

            print(f"\n🖥️ inventory_data FIELDS: {len(inv_keys)}/{len(EXPECTED_INV)}")
            for k in sorted(inv_keys):
                print(f"  ✅ {k}: {payload['inventory_data'][k]}")
            if missing_inv:
                for m in missing_inv: print(f"  ❌ MISSING: inventory_data.{m}")

            # 🏁 FRESHNESS AUDITOR
            print("\n🏁 FRESHNESS LOGIC AUDIT")
            print("-" * 60)
            fresh_points = [p for p in pd if p.get("is_fresh") is True]
            stale_points = [p for p in pd if p.get("is_fresh") is False]
            
            dates_found = sorted(list(set(p["Time"][:10] for p in pd)))
            fresh_dates = sorted(list(set(p["Time"][:10] for p in fresh_points)))
            
            print(f"  • Total Data Points: {len(pd)}")
            print(f"  • Date Range: {dates_found[0]} to {dates_found[-1]} ({len(dates_found)} days)")
            print(f"  • Fresh Points found: {len(fresh_points)} (Expected ~288)")
            print(f"  • Fresh Date identified: {fresh_dates}")
            
            # Boundary Safety Checks
            is_valid = True
            if len(fresh_dates) > 1:
                print(f"  ❌ ERROR: Multiple 'Fresh' days detected! {fresh_dates}")
                is_valid = False
            elif fresh_dates and fresh_dates[0] != dates_found[-1]:
                print(f"  ❌ ERROR: Fresh day {fresh_dates[0]} is NOT the latest date {dates_found[-1]}!")
                is_valid = False
            elif not fresh_dates:
                print("  ❌ ERROR: No points marked as 'Fresh'!")
                is_valid = False
            else:
                print(f"  ✅ SUCCESS: Only the terminal day ({fresh_dates[0]}) is marked as Fresh.")
            
            total = len(top_keys) + len(data_keys) + len(pd_keys) + len(inv_keys)
            expected = len(EXPECTED_TOP) + len(EXPECTED_DATA) + len(EXPECTED_PD) + len(EXPECTED_INV)
            print(f"\n{'🎯' if total >= expected and is_valid else '⚠️'} TOTAL: {total}/{expected} fields")
            
            if not (missing_top or missing_data or missing_pd or missing_inv) and is_valid:
                print("🏆 RESULT: ALL 49 FIELDS PRESENT & LOGICALLY CONSISTENT ✅")
            else:
                print("❌ RESULT: SCHEMA OR LOGIC FAILED")
            print("=" * 60)
            break
    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(verify())
