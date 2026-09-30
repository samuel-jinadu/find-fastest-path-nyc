#!/usr/bin/env bash
set -euo pipefail

echo "== Airflow UI =="
curl -sf -o /dev/null -w "airflow-apiserver: %{http_code}\n" http://localhost:8080/api/v2/version

echo "== MinIO health =="
docker compose exec -T airflow-worker curl -s http://minio:9000/minio/health/live && echo

echo "== MinIO bucket exists =="
docker compose exec -T airflow-worker python -c "
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
h = S3Hook(aws_conn_id='s3')
print('datalake exists:', h.check_for_bucket('datalake'))
"

echo "== Citi Bike API (expect 401) =="
docker compose exec -T airflow-worker \
  curl -s -o /dev/null -w "citibike_api: %{http_code}\n" http://citibike_api:5000/

echo "== Taxi fileserver =="
docker compose exec -T airflow-worker curl -s http://taxi_fileserver/ | head -c 200; echo

echo "== Result DB tables =="
docker compose exec -T airflow-worker python -c "
from airflow.providers.postgres.hooks.postgres import PostgresHook
h = PostgresHook(postgres_conn_id='result_db')
print(h.get_records(\"SELECT tablename FROM pg_tables WHERE schemaname='public';\"))
"