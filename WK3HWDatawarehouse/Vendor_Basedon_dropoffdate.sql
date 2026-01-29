select distinct VendorID from de_zoomcamp_dw.yellow_tripdata_partitioned_clustered where tpep_dropoff_datetime>='2024-03-01' and  tpep_dropoff_datetime<='2024-03-15';

select distinct VendorID from de_zoomcamp_dw.yellow_tripdata_non_partitioned where tpep_dropoff_datetime>='2024-03-01' and  tpep_dropoff_datetime<='2024-03-15';

--310.24
--26.84