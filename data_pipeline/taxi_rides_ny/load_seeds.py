#!/usr/bin/env python
"""
Load seed data from Parquet files to BigQuery using the BigQuery Python client.
This bypasses dbt's CSV loader and avoids error 3848323.
"""

from pathlib import Path
from google.cloud import bigquery
import pandas as pd

# Configuration
PROJECT_ID = "de-zoomcamp-397401"
DATASET_ID = "zoomcamp"
LOCATION = "US"
SEEDS_DIR = Path(__file__).parent / "seeds"

# Initialize BigQuery client
client = bigquery.Client(project=PROJECT_ID)
dataset_ref = client.dataset(DATASET_ID, project=PROJECT_ID)

# Seed files to load
SEEDS = [
    ("payment_type_lookup.parquet", "payment_type_lookup"),
    ("taxi_zone_lookup.parquet", "taxi_zone_lookup"),
]


def load_seed_to_bigquery(parquet_file, table_name):
    """Load a Parquet file to BigQuery table."""
    file_path = SEEDS_DIR / parquet_file

    if not file_path.exists():
        print(f"✗ File not found: {file_path}")
        return False

    try:
        # Read parquet file
        df = pd.read_parquet(file_path)
        print(f"\n📁 Loading {parquet_file}:")
        print(f"   Rows: {len(df)}, Columns: {list(df.columns)}")

        # Create table reference
        table_ref = dataset_ref.table(table_name)

        # Configure load job
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=False,  # Use explicit schema to avoid type inference issues
        )

        # Infer schema from parquet and set it explicitly
        schema_fields = []
        for col, dtype in zip(df.columns, df.dtypes):
            # Map pandas dtypes to BigQuery types
            if dtype == "int64":
                bq_type = "INTEGER"
            elif dtype == "object":
                bq_type = "STRING"
            else:
                bq_type = "STRING"
            schema_fields.append(bigquery.SchemaField(col, bq_type))

        job_config.schema = schema_fields

        # Load data
        load_job = client.load_table_from_dataframe(
            df, table_ref, job_config=job_config, location=LOCATION
        )
        load_job.result()  # Wait for the job to complete

        print(f"   ✓ Successfully loaded {len(df)} rows to {DATASET_ID}.{table_name}")
        return True

    except Exception as e:
        print(f"   ✗ Error loading {table_name}: {e}")
        return False


def main():
    """Load all seeds to BigQuery."""
    print(f"\n🚀 Loading seeds to BigQuery ({PROJECT_ID}.{DATASET_ID})\n")
    print(f"   Location: {LOCATION}")
    print(f"   Seeds directory: {SEEDS_DIR}")

    results = []
    for parquet_file, table_name in SEEDS:
        result = load_seed_to_bigquery(parquet_file, table_name)
        results.append((table_name, result))

    # Summary
    print("\n" + "=" * 60)
    successful = sum(1 for _, result in results if result)
    print(f"Summary: {successful}/{len(results)} seeds loaded successfully")

    if successful == len(results):
        print("✓ All seeds loaded successfully!")
        return 0
    else:
        print("✗ Some seeds failed to load")
        for name, result in results:
            status = "✓" if result else "✗"
            print(f"  {status} {name}")
        return 1


if __name__ == "__main__":
    exit(main())
