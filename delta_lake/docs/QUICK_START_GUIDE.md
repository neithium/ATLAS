# ATLAS Data Sync - Quick Start Guide

## 🚀 What Was Implemented

Your ATLAS Spark cluster now has **automatic data synchronization** to `C:\Users\Public\atlas-data\`.

- ✅ **Local workers** access data directly (no network latency)
- ✅ **Remote workers** access data via SMB network sharing
- ✅ **Automatic sync** after benchmark completes
- ✅ **Real-time monitoring** available

---

## 📋 Quick Commands

### Run the Full Pipeline
```powershell
cd C:\Users\manth\Documents\GitHub\ATLAS
.\Run-ClusterBenchmark.ps1
```

This will:
1. Start Spark Master and Worker containers
2. Run the benchmark (generate 1000 devices, 3 days of data)
3. Automatically sync Parquet files to `C:\Users\Public\atlas-data\`
4. Display sync status

### Watch Data Sync in Real-Time
```powershell
.\Run-ClusterBenchmark.ps1 -WatchSync
```

Continuously monitors for new Parquet files every 30 seconds. Press `Ctrl+C` to stop.

### Check Sync Status Anytime
```powershell
. .\Sync-AtlasData.ps1
Show-SyncStatus
```

Shows:
- Number of Parquet files in `/raw` and `/refined`
- Total size in MB
- Last modified timestamp

### Clear Old Data
```powershell
. .\Sync-AtlasData.ps1
Clear-SyncedData -Force $true
```

---

## 📁 Data Location

```
C:\Users\Public\atlas-data\
├── raw\                    # Input telemetry data
│   └── *.parquet           # Generated benchmark data
└── refined\                # Deduplicated output
    └── *.parquet           # Delta Lake deduplication results
```

This location is:
- ✅ Writable by Docker containers
- ✅ Accessible to all local applications
- ✅ **Shareable over SMB to remote machines**

---

## 🔗 Setting Up Remote Workers

### On Laptop1 (Your Current Machine - Master)

No additional setup needed! The benchmark script handles everything.

The data at `C:\Users\Public\atlas-data\` is automatically shared via Windows File Sharing.

**Network Path:** `\\<YOUR_MACHINE_NAME>\Public\atlas-data`

To find your machine name:
```powershell
$env:COMPUTERNAME
```

### On Laptop2 (Remote Worker Machine)

**1. Mount the shared folder (Linux/WSL):**

First, find Laptop1's IP address. On Laptop1:
```powershell
ipconfig | findstr "IPv4"
# Look for something like 192.168.x.x
```

On Laptop2 (Windows):

**1. Find Laptop1's IP address:**

On Laptop1 (your current machine):
```powershell
ipconfig | findstr "IPv4"
# Look for something like 192.168.1.15
```

**2. Mount the shared folder on Laptop2 (Windows):**

Option A - Using PowerShell (Recommended):
```powershell
# Replace 192.168.1.15 with Laptop1's actual IP
# Replace your_username with your Windows username
net use Z: \\192.168.1.15\Public\atlas-data /persistent:yes
# When prompted, enter your Laptop1 Windows password
```

Option B - Using File Explorer (Manual):
1. Open File Explorer
2. Right-click "This PC" → "Map network drive"
3. Drive: `Z:`
4. Folder: `\\192.168.1.15\Public\atlas-data` (replace IP)
5. Check "Reconnect at sign-in"
6. Click Finish and enter credentials

**3. Verify the mount (PowerShell):**
```powershell
dir Z:\
# Should show: raw  refined
```

**4. Start the remote Spark worker on Laptop2 (Windows):**

First, ensure Docker Desktop is running. Then run:
```powershell
docker run -d --name atlas-remote-worker `
  --net host `
  -e SPARK_MODE=worker `
  -e SPARK_DAEMON_USER=root `
  -e SPARK_MASTER_URL=spark://192.168.1.15:7077 `
  -e SPARK_WORKER_MEMORY=2G `
  -e SPARK_WORKER_CORES=2 `
  -e SPARK_RPC_AUTHENTICATION_ENABLED=no `
  -e SPARK_RPC_ENCRYPTION_ENABLED=no `
  -v Z:/atlas-data/raw:/raw `
  -v Z:/atlas-data/refined:/refined `
  bitnamilegacy/spark:3.5
```

**5. Install Delta Lake dependencies on the worker:**
```powershell
docker exec atlas-remote-worker pip install delta-spark==3.1.0 pyarrow>=14.0.0
```

**6. Verify worker is connected:**

Visit `http://192.168.1.15:8080` in your browser to see:
- Your Laptop1 local worker
- Your Laptop2 remote worker (should show "ALIVE")

**2. Start the remote Spark worker:**

```bash
docker run -d --name atlas-remote-worker \
  --net host \
  -e SPARK_MODE=worker \
  -e SPARK_MASTER_URL=spark://192.168.1.15:7077 \
  -e SPARK_WORKER_MEMORY=8g \
  -e SPARK_WORKER_CORES=4 \
  -v /mnt/atlas-data/raw:/raw \
  -v /mnt/atlas-data/refined:/refined \
  bitnamilegacy/spark:3.5
```

**3. Verify worker is connected:**

Visit `http://192.168.1.15:8080` in your browser to see:
- Your local worker (localhost)
- Your remote worker (Laptop2)

Both workers now see the **same** `/raw` and `/refined` data!

---

## 🎯 Usage Scenarios

### Scenario 1: Generate Data & Sync
```powershell
# This does everything automatically:
.\Run-ClusterBenchmark.ps1

# Check what was synced:
. .\Sync-AtlasData.ps1
Show-SyncStatus
```

### Scenario 2: Monitor Sync in Real-Time
```powershell
# Watch files being copied as they're generated:
.\Run-ClusterBenchmark.ps1 -WatchSync
```

### Scenario 3: Scale to Remote Workers
```powershell
# On Laptop1:
.\Run-ClusterBenchmark.ps1

# While that's running, on Laptop2:
# 1. Mount the shared folder
# 2. Start the remote worker
# 3. Spark automatically distributes work to both workers
```

### Scenario 4: Clear Old Data
```powershell
# Before running a new benchmark, clean up:
. .\Sync-AtlasData.ps1
Clear-SyncedData -Force $true

# Then run fresh:
.\Run-ClusterBenchmark.ps1
```

---

## 📊 What Gets Synced

| Component | Location | Size | Files |
|-----------|----------|------|-------|
| Raw Input Data | `C:\Users\Public\atlas-data\raw\` | ~500MB | Multiple Parquet |
| Refined Output | `C:\Users\Public\atlas-data\refined\` | ~200MB | Delta Lake tables |
| **Total** | **Both directories** | **~700MB** | **Varies** |

All sizes depend on `--devices` and `--days` parameters.

---

## ⚡ Performance Notes

### Local Workers (Laptop1)
- **I/O Speed:** Native filesystem speed (~500MB/s)
- **Latency:** <1ms
- **Advantage:** Maximum performance for processing

### Remote Workers (Laptop2)
- **I/O Speed:** Network SMB speed (~50-100MB/s)
- **Latency:** 1-10ms (depends on network)
- **Advantage:** Can still process large datasets effectively

The network latency only affects I/O. Spark's in-memory processing is still very fast.

---

## 🛠️ Troubleshooting

### Q: "No files synced" after running benchmark
**A:** This is expected initially. Run with `-WatchSync` to see files as they're created:
```powershell
.\Run-ClusterBenchmark.ps1 -WatchSync
```

### Q: "Permission denied" errors
**A:** Reset permissions:
```powershell
docker exec -u root atlas-spark-master bash -c "chmod -R 777 /raw /refined"
```

### Q: Remote worker can't see data
**A:** Verify SMB mount on remote machine:
```bash
# Check mount
mount | grep atlas-data

# Verify files exist
ls -la /mnt/atlas-data/raw/
```

### Q: Docker containers won't start
**A:** Check Docker and ports:
```powershell
docker ps
docker ps -a    # Show stopped containers too
```

### Q: Want to skip sync this time?
**A:** Use the `-NoSync` flag:
```powershell
.\Run-ClusterBenchmark.ps1 -NoSync
```

---

## 📚 File Reference

| File | Purpose |
|------|---------|
| `Run-ClusterBenchmark.ps1` | Main benchmark orchestration (updated) |
| `Sync-AtlasData.ps1` | Data sync functions (NEW) |
| `docker-compose.yml` | Container definitions (updated with bind mounts) |
| `IMPLEMENTATION_REPORT.md` | Detailed technical documentation |
| `QUICK_START_GUIDE.md` | This file |

---

## ✅ Verification Checklist

- [ ] Run `.\Run-ClusterBenchmark.ps1` and see data synced
- [ ] Run `Show-SyncStatus` and see file counts
- [ ] Check `C:\Users\Public\atlas-data\` has files
- [ ] (Optional) Set up remote worker on Laptop2
- [ ] (Optional) Verify remote worker in Spark UI at `http://localhost:8080`

---

## 🎓 Next Steps

1. **Run the benchmark:** `.\Run-ClusterBenchmark.ps1`
2. **Verify sync:** `Show-SyncStatus`
3. **Set up remote workers** (if needed)
4. **Monitor execution:** Use `-WatchSync` flag
5. **Check Spark UI:** Visit `http://localhost:8080` to see worker status

---

## 💡 Key Points

✨ **All data is automatically synced** - No manual copying needed!

✨ **Works seamlessly with remote workers** - Same data visible to all workers

✨ **Low overhead** - Sync uses efficient `docker cp` command

✨ **Real-time monitoring** - Watch mode lets you see progress

✨ **Easy to maintain** - Clear, well-documented scripts

---

**Questions?** Check `IMPLEMENTATION_REPORT.md` for detailed technical documentation.

**Ready to start?** Run: `.\Run-ClusterBenchmark.ps1`
