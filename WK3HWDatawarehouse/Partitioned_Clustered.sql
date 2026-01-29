CREATE OR REPLACE TABLE de_zoomcamp_dw.yellow_tripdata_partitioned_clustered
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM de_zoomcamp_dw.yellow_tripdata_non_partitioned;