# Data Contract

## Citi Bike API
- Endpoint: `http://citibike_api:5000/recent/minute/15`
- Auth: HTTP basic, user=`citibike`, password=`cycling`
- Returns: JSON list of ride objects
- Columns per row: tripduration, starttime, stoptime,
  start_station_id, start_station_name, start_station_latitude,
  start_station_longitude, end_station_id, end_station_name,
  end_station_latitude, end_station_longitude
- Cadence: request-driven; source DB updates continuously
- Retention: full 2023 dataset in DB, shifted to "now" by
  year_offset = current_year - 2023
- Backfill: possible (query further back with larger `amount`)

## Taxi fileserver
- Index: `http://taxi_fileserver/` (JSON list of files)
- File: `http://taxi_fileserver/{filename}.csv`
- Auth: none
- Columns: pickup_datetime, dropoff_datetime,
  pickup_locationid, dropoff_locationid, trip_distance
- Cadence: new file every 15 min (cron)
- Retention: **files older than 59 minutes are DELETED**
- Backfill: **impossible beyond 1 hour**

## Result DB (Postgres)
- Connection: `result_db` (nyc / tr4N5p0RT4TI0N)
- Tables: `taxi_rides`, `citi_bike_rides`
- Columns: tripduration, starttime, start_location_id,
  stoptime, end_location_id, airflow_execution_date

## MinIO (S3-compatible)
- Endpoint: `http://minio:9000`
- Bucket: `datalake`
- Layout:
  - raw/citibike/{ts}.json
  - raw/taxi/{filename}.csv
  - processed/citibike/{ts}.parquet
  - processed/taxi/{ts}.parquet

## Key constraints
1. Taxi retention is 1 hour. Scheduler downtime > 1h = permanent
   data loss for that window. Citi Bike can be backfilled; taxi cannot.
2. DAG schedule `*/15 * * * *` matches source cadence. Changing it
   either loses data (slower) or duplicates API calls (faster).
3. All timestamps are 2023 data shifted to look current. Don't join
   with external time-series data without accounting for this.