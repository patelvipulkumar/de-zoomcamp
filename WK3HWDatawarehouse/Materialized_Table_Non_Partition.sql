CREATE OR REPLACE TABLE de_zoomcamp_dw.yellow_tripdata_non_partitioned AS
SELECT * FROM de_zoomcamp_dw.external_yellow_tripdata;

select count(1)  from de_zoomcamp_dw.yellow_tripdata_non_partitioned where fare_amount = 0

--155.12 MB
--310.24 MB


SELECT count(*) from de_zoomcamp_dw.yellow_tripdata_non_partitioned