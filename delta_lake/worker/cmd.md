docker run -d --name atlas-remote-worker --net host \
  -e SPARK_MODE=worker \
  -e SPARK_DAEMON_USER=root \
  -e SPARK_MASTER_URL=spark://<YOUR_WINDOWS_MACHINE_IP>:7077 \
  -e SPARK_WORKER_MEMORY=2G \
  -e SPARK_WORKER_CORES=2 \
  -e SPARK_RPC_AUTHENTICATION_ENABLED=no \
  -e SPARK_RPC_ENCRYPTION_ENABLED=no \
  -v //windows-machine-ip/Users/Public/atlas-data/raw:/raw \
  -v //windows-machine-ip/Users/Public/atlas-data/refined:/refined \
  bitnamilegacy/spark:3.5

# IMPORTANT DEPENDENCIES:
# This command expects the bitnamilegacy/spark:3.5 image to have:
# 1. Java 11+ (included in base image)
# 2. Spark 3.5.x (included in base image)
# 3. delta-spark==3.1.0 (must be pip installed - see note below)
# 4. pyarrow>=14.0.0 (must be pip installed - see note below)
# 5. Shared /raw and /refined directories from master (network mounts)
# 6. UID 1001 with write permissions on mounted volumes
#
# The base image DOES NOT include delta-spark or pyarrow by default!
# You must use the custom Dockerfile in delta_lake/ or pre-install these packages:
#
# Option A: Use the ATLAS custom Dockerfile (RECOMMENDED)
# docker build -t atlas-spark:3.5 -f delta_lake/Dockerfile .
# Then replace 'bitnamilegacy/spark:3.5' with 'atlas-spark:3.5' in the command above
#
# Option B: Pre-install packages in existing container
# docker exec atlas-remote-worker pip install --timeout 120 delta-spark==3.1.0 pyarrow>=14.0.0
#
# VERIFICATION: After starting the worker, check dependencies:
# docker exec atlas-remote-worker python3 -c "import delta; import pyarrow; print('✓ Delta Lake ready')"
# docker exec atlas-remote-worker java -version
# docker exec atlas-remote-worker ls -la /raw /refined