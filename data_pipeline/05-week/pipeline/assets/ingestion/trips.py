"""@bruin

name: ingestion.trips
connection: duckdb-default

materialization:
  type: table
  strategy: create+replace
image: python:3.11

columns:
  - name: pickup_datetime
    type: timestamp
    description: When the meter was engaged
  - name: dropoff_datetime
    type: timestamp
    description: When the meter was disengaged

@bruin"""

import json
import os
from datetime import datetime

import pandas as pd
import requests

# NYC TLC trip data: parquet per taxi type per month; column names normalized
# for staging (green=lpep_*, yellow=tpep_* in source).


# NYC TLC trip data (parquet, one file per taxi type per month)
# https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
FILE_PATTERN = "{taxi_type}_tripdata_{year}-{month:02d}.parquet"

# Output columns expected by staging.trips and reports
OUTPUT_COLUMNS = [
    "pickup_datetime",
    "dropoff_datetime",
    "pickup_location_id",
    "dropoff_location_id",
    "fare_amount",
    "payment_type",
    "taxi_type",
    "month",
]

# Source column names per taxi type (NYC TLC parquet schema)
COLUMN_MAP = {
    "green": {
        "pickup_datetime": "lpep_pickup_datetime",
        "dropoff_datetime": "lpep_dropoff_datetime",
        "pickup_location_id": "PULocationID",
        "dropoff_location_id": "DOLocationID",
    },
    "yellow": {
        "pickup_datetime": "tpep_pickup_datetime",
        "dropoff_datetime": "tpep_dropoff_datetime",
        "pickup_location_id": "PULocationID",
        "dropoff_location_id": "DOLocationID",
    },
}


def _parse_run_config():
    """Read date range and taxi_types from Bruin environment."""
    start_str = os.environ["BRUIN_START_DATE"]
    end_str = os.environ["BRUIN_END_DATE"]
    start_date = datetime.fromisoformat(start_str.replace("Z", "+00:00")).date()
    end_date = datetime.fromisoformat(end_str.replace("Z", "+00:00")).date()
    vars_str = os.environ.get("BRUIN_VARS", "{}")
    taxi_types = json.loads(vars_str).get("taxi_types", ["yellow"])
    return start_date, end_date, taxi_types


def _month_range(start_date, end_date):
    """Yield (year, month) tuples for every month in [start_date, end_date]."""
    for year in range(start_date.year, end_date.year + 1):
        first_month = start_date.month if year == start_date.year else 1
        last_month = end_date.month if year == end_date.year else 12
        for month in range(first_month, last_month + 1):
            yield year, month


def _normalize_trips(df, taxi_type, year, month):
    """
    Map source columns to the unified schema and keep only OUTPUT_COLUMNS.
    Ensures dtypes are serialization-friendly (no object dtype) for Bruin/Arrow.
    """
    if taxi_type not in COLUMN_MAP:
        raise ValueError(f"Unknown taxi_type: {taxi_type}. Expected one of: {list(COLUMN_MAP)}")
    rename = COLUMN_MAP[taxi_type]
    df = df.rename(columns={v: k for k, v in rename.items() if v in df.columns})
    df["taxi_type"] = str(taxi_type)
    df["month"] = f"{year}-{month:02d}"

    missing = [c for c in OUTPUT_COLUMNS if c not in df.columns]
    if missing:
        raise RuntimeError(
            f"Parquet missing columns: {missing}. Available: {list(df.columns)}"
        )
    out = df[OUTPUT_COLUMNS].copy()
    # Normalize dtypes for reliable Arrow/DuckDB serialization (avoids schema dump on stdout)
    out["pickup_datetime"] = pd.to_datetime(out["pickup_datetime"], utc=False)
    out["dropoff_datetime"] = pd.to_datetime(out["dropoff_datetime"], utc=False)
    return out


def materialize():
    """
    Returns a single DataFrame of trip data for the run's date range and taxi types.
    Fetches each parquet file (one per taxi type per month), normalizes, and concatenates.
    Returning a single DataFrame (instead of yielding) matches Bruin's documented flow
    and avoids serialization issues with the Arrow memory-mapped path.
    """
    start_date, end_date, taxi_types = _parse_run_config()
    chunks = []

    for taxi_type in taxi_types:
        for year, month in _month_range(start_date, end_date):
            filename = FILE_PATTERN.format(
                taxi_type=taxi_type, year=year, month=month
            )
            url = TLC_BASE_URL + filename

            try:
                response = requests.get(url, timeout=120)
                response.raise_for_status()
            except requests.RequestException as e:
                raise RuntimeError(f"Failed to fetch {url}: {e}") from e

            try:
                raw = pd.read_parquet(response.content)
            except Exception as e:
                raise RuntimeError(f"Failed to read parquet from {url}: {e}") from e

            chunks.append(_normalize_trips(raw, taxi_type, year, month))

    if not chunks:
        return []

    combined = pd.concat(chunks, ignore_index=True)
    # Return list of dicts (rows) so Bruin's runner doesn't treat a DataFrame
    # as an iterable (list(df)=column names) or hit the Arrow path that dumps schema to stdout.
    combined["pickup_datetime"] = combined["pickup_datetime"].dt.strftime("%Y-%m-%dT%H:%M:%S")
    combined["dropoff_datetime"] = combined["dropoff_datetime"].dt.strftime("%Y-%m-%dT%H:%M:%S")
    # orient="records" gives list of dicts; NaN becomes None for JSON/Arrow
    return combined.replace({pd.NA: None}).to_dict(orient="records")
