CREATE OR REPLACE EXTERNAL TABLE `de_zoomcamp_dw.external_yellow_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://ny-taxi-yellow-bucket/yellow_tripdata_2024-*.parquet']
);

SELECT PULocationID FROM de_zoomcamp_dw.external_yellow_tripdata