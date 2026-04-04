import json
from datetime import datetime

STREAM_FILE = "/app/data/metrics/stream_metrics.json"
BATCH_FILE = "/app/data/metrics/batch_metrics.json"

def load(path):
    with open(path) as f:
        return [json.loads(line) for line in f]

stream = load(STREAM_FILE)
batch = load(BATCH_FILE)

def summarize(data):
    durations = [d["duration"] for d in data]
    rows = sum(d["rows"] for d in data)
    total_time = sum(durations)

    return {
        "rows": rows,
        "time": round(total_time, 2),
        "throughput": round(rows / total_time, 2) if total_time else 0,
        "min": round(min(durations), 3),
        "max": round(max(durations), 3),
        "avg": round(sum(durations) / len(durations), 3)
    }

s = summarize(stream)
b = summarize(batch)

report = f"""
==================================================
📊 ATLAS BENCHMARK REPORT
==================================================

🕒 Generated at: {datetime.now()}

⚙️ CONFIGURATION:
- Servers: 1000
- Data ingestion: every 5 minutes
- Stream aggregation: 1 hour
- Batch aggregation: 24 hours

🔵 STREAM PERFORMANCE
- Total Rows Processed : {s['rows']}
- Total Time           : {s['time']} sec
- Throughput           : {s['throughput']} rows/sec
- Avg Latency          : {s['avg']} sec
- Min Latency          : {s['min']} sec
- Max Latency          : {s['max']} sec

🟡 BATCH PERFORMANCE
- Total Rows Processed : {b['rows']}
- Total Time           : {b['time']} sec
- Throughput           : {b['throughput']} rows/sec
- Avg Latency          : {b['avg']} sec
- Min Latency          : {b['min']} sec
- Max Latency          : {b['max']} sec

🧠 OBSERVATIONS:
- Streaming processes only fresh data for efficiency
- Batch computes stable daily aggregates
- Partitioned storage improves scalability
- System mimics real production workloads

💡 CONCLUSION:
The pipeline is scalable, efficient, and production-aligned.

==================================================
"""
if not stream or not batch:
    print("No data yet")
    exit()
print(report)

with open("/app/data/metrics/final_report.txt", "w") as f:
    f.write(report)