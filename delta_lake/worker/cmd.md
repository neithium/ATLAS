docker run -d --name ext-worker --net host \
  -e SPARK_MODE=worker \
  -e SPARK_MASTER_URL=spark://<YOUR_WINDOWS_MACHINE_IP>:7077 \
  -v raw-volume:/raw \
  -v refined-volume:/refined \
  bitnamilegacy/spark:3.5