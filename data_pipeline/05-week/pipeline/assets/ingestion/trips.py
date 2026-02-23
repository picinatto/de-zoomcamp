"""@bruin
name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: pickup_datetime
    type: timestamp
    description: "When the meter was engaged"
  - name: dropoff_datetime
    type: timestamp
    description: "When the meter was disengaged"
@bruin"""

import json
import os

import pandas as pd
import requests


def materialize():
    start_date = os.environ["BRUIN_START_DATE"]
    end_date = os.environ["BRUIN_END_DATE"]
    taxi_types = json.loads(os.environ["BRUIN_VARS"]).get("taxi_types", ["yellow"])

    # Generate list of months between start and end dates
    # Fetch parquet files from:
    # https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month}.parquet
    months = []
    for year in range(start_date.year, end_date.year + 1):
        for month in range(1, 13):
            months.append(f"{year}-{month:02d}")

    for taxi_type in taxi_types:
        for month in months:
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{month}.parquet"
            response = requests.get(url)
            response.raise_for_status()
            df = pd.read_parquet(response.content)
            df["taxi_type"] = taxi_type
            df["month"] = month
            yield df
