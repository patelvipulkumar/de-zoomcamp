#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import uv

# Green taxi trip data types
green_dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime"
]

# Taxi zone lookup data types
zone_dtype = {
    "LocationID": "Int64",
    "Borough": "string",
    "Zone": "string",
    "service_zone": "string"
}


@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--green-url', default='https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet', help='Green taxi data URL')
@click.option('--zone-url', default='https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv', help='Taxi zone lookup CSV URL')
@click.option('--green-table', default='green_taxi_data', help='Green taxi data table name')
@click.option('--zone-table', default='taxi_zone_lookup', help='Taxi zone lookup table name')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for reading data')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, green_url, zone_url, green_table, zone_table, chunksize):
    """Ingest NYC green taxi trip data and taxi zone lookup into PostgreSQL database."""
    
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    # Ingest green taxi trip data from parquet file
    print(f"Ingesting green taxi data from {green_url}...")
    df_green = pd.read_parquet(green_url)
    
    # Convert date columns to datetime if needed
    for col in parse_dates:
        if col in df_green.columns and df_green[col].dtype == 'object':
            df_green[col] = pd.to_datetime(df_green[col])
    
    # Process green taxi data in chunks
    first = True
    for i in tqdm(range(0, len(df_green), chunksize), desc="Writing green taxi data"):
        df_chunk = df_green.iloc[i:i+chunksize]
        
        if first:
            df_chunk.head(0).to_sql(
                name=green_table,
                con=engine,
                if_exists='replace'
            )
            first = False

        df_chunk.to_sql(
            name=green_table,
            con=engine,
            if_exists='append',
            index=False
        )

    # Ingest taxi zone lookup from CSV
    print(f"\nIngesting taxi zone lookup from {zone_url}...")
    df_zone = pd.read_csv(zone_url)
    
    # Process zone data in chunks
    first = True
    for i in tqdm(range(0, len(df_zone), chunksize), desc="Writing zone lookup data"):
        df_chunk = df_zone.iloc[i:i+chunksize]
        
        if first:
            df_chunk.head(0).to_sql(
                name=zone_table,
                con=engine,
                if_exists='replace'
            )
            first = False

        df_chunk.to_sql(
            name=zone_table,
            con=engine,
            if_exists='append',
            index=False
        )

    print(f"\nSuccessfully ingested data:")
    print(f"  - {len(df_green)} green taxi records to table '{green_table}'")
    print(f"  - {len(df_zone)} zone lookup records to table '{zone_table}'")

if __name__ == '__main__':
    run()