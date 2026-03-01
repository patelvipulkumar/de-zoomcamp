import duckdb

con = duckdb.connect("taxi_pipeline.duckdb")
con.sql("""
  SELECT
    COUNT(*) AS total_trips,
    SUM(CASE WHEN Payment_Type = 'Credit' THEN 1 ELSE 0 END) AS credit_trips,
    1.0 * SUM(CASE WHEN Payment_Type = 'Credit' THEN 1 ELSE 0 END) / COUNT(*) AS credit_share
  FROM nytaxi_pipeline.trips
""").show()