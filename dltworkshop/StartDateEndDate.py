import duckdb
con = duckdb.connect('taxi_pipeline.duckdb')
# Get date range from pickup/dropoff datetime columns
con.sql('''
  SELECT
    MIN(trip_pickup_date_time) AS start_date,
    MAX(trip_pickup_date_time) AS end_date_pickup,
    MIN(trip_dropoff_date_time) AS start_dropoff,
    MAX(trip_dropoff_date_time) AS end_date_dropoff
  FROM nytaxi_pipeline.trips
''').show()
