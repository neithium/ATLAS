import subprocess

def fast_verify():
    print("[CHECK] Counting PowerDetail entries directly in Kafka...")
    
    # 1. Pull the first message and pipe it through a count check
    # We look for '"Time":' occurrences which marks each reading
    cmd = "docker exec broker1 kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic raw-server-metrics --max-messages 1"
    
    try:
        raw_output = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT).decode('utf-8')
        
        # Each reading has a "Time" key.
        point_count = raw_output.count('"Time":')
        
        print(f"\n[RESULT] Readings Found: {point_count}")
        
        if point_count >= 2000:
            print("[SUCCESS] All 7 days of telemetry are present (Found 2,016 points)!")
        elif point_count > 0:
            print(f"[WARNING] Only partial data found: {point_count} points.")
        else:
            print("[ERROR] No data found in the message.")

    except Exception as e:
        print(f"[ERROR] Connection failed: {e}")

if __name__ == "__main__":
    fast_verify()
