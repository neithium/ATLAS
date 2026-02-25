@echo off
REM TVMJNS — Run Spark Alert Processor
REM This runs the Spark job inside the Docker container

echo ============================================================
echo TVMJNS — Spark Alert Processor
echo ============================================================
echo.

docker exec spark-master /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/ivy2 --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3" /scripts/spark_docker_alerts.py

echo.
echo ============================================================
echo View alerts in Adminer: http://localhost:8888
echo ============================================================
