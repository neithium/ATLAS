docker exec -it atlas-analytics clickhouse-client -u atlas --password atlas_secure_pwd -d atlas -q "TRUNCATE TABLE telemetry_refined;"
docker exec -it atlas-analytics psql -U atlas -d atlas_metadata -c "TRUNCATE TABLE data_load_watermarks;"
docker exec -it atlas-analytics psql -U atlas -d atlas_metadata -c "ALTER TABLE data_load_watermarks ADD UNIQUE (source, device_id);"