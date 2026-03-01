import duckdb

con = duckdb.connect("taxi_pipeline.duckdb")
con.sql("""
  SELECT
    SUM(Tip_Amt) AS total_tip_amount
  FROM nytaxi_pipeline.trips
""").show()