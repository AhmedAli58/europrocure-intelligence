"""
etl_pipeline.py
---------------
Loads the cleaned parquet file into Snowflake RAW schema.

Pipeline steps:
1. Read cleaned parquet file
2. Create target table in Snowflake RAW schema
3. Load data in batches
4. Log row counts and load metadata

Run this script once after clean_data.py has completed.
"""

import pandas as pd
import numpy as np
import os
import logging
from datetime import datetime
from snowflake.connector.pandas_tools import write_pandas
from scripts.snowflake_connector import get_connection

# ── Logging setup ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s — %(levelname)s — %(message)s'
)
log = logging.getLogger(__name__)

# ── Paths ──────────────────────────────────────────────────────
BASE_DIR = os.path.expanduser("~/Desktop/europrocure-intelligence")
PARQUET_PATH = os.path.join(BASE_DIR, "data/processed/ted_can_cleaned.parquet")

# ── Target table ───────────────────────────────────────────────
TARGET_TABLE = "TED_CONTRACTS"
TARGET_SCHEMA = "RAW"


def load_parquet(path):
    """Load the cleaned parquet file."""
    log.info(f"Loading parquet file from {path}...")
    df = pd.read_parquet(path, engine='pyarrow')
    log.info(f"Loaded {len(df):,} rows and {len(df.columns)} columns")
    return df


def prepare_dataframe(df):
    """
    Prepare dataframe for Snowflake loading.
    - Convert column names to uppercase (Snowflake convention)
    - Convert boolean columns to strings (Snowflake compatibility)
    - Replace NaN with None for proper NULL handling
    """
    log.info("Preparing dataframe for Snowflake...")

    # Uppercase column names for Snowflake
    df.columns = df.columns.str.upper()

    # Convert boolean columns to string — Snowflake handles these better
    bool_cols = df.select_dtypes(include='bool').columns.tolist()
    for col in bool_cols:
        df[col] = df[col].map({True: 'Y', False: 'N'})

    # Convert datetime columns to string for Snowflake compatibility
    datetime_cols = df.select_dtypes(include='datetime64').columns.tolist()
    for col in datetime_cols:
        df[col] = df[col].dt.strftime('%Y-%m-%d').where(df[col].notna(), None)

    # Replace numpy NaN with None for proper NULL loading
    df = df.replace({np.nan: None})

    log.info(f"Dataframe prepared — {len(df.columns)} columns")
    log.info(f"Boolean columns converted: {bool_cols}")
    log.info(f"Datetime columns converted: {datetime_cols}")
    return df


def create_table(conn, df):
    """
    Create the target table in Snowflake RAW schema.
    Drops and recreates if it already exists.
    Column types are inferred from the dataframe.
    """
    log.info(f"Creating table {TARGET_SCHEMA}.{TARGET_TABLE}...")

    # Map pandas dtypes to Snowflake types
    type_map = {
        'int64': 'NUMBER',
        'float64': 'FLOAT',
        'object': 'VARCHAR',
        'bool': 'VARCHAR',
        'datetime64[ns]': 'VARCHAR',
    }

    col_defs = []
    for col, dtype in df.dtypes.items():
        sf_type = type_map.get(str(dtype), 'VARCHAR')
        col_defs.append(f'"{col}" {sf_type}')

    ddl = f"""
    CREATE OR REPLACE TABLE {TARGET_SCHEMA}.{TARGET_TABLE} (
        {','.join(col_defs)},
        LOAD_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    )
    """

    cur = conn.cursor()
    cur.execute(f"USE SCHEMA {TARGET_SCHEMA}")
    cur.execute(ddl)
    log.info(f"Table {TARGET_SCHEMA}.{TARGET_TABLE} created successfully")


def load_to_snowflake(conn, df):
    """
    Load dataframe into Snowflake using write_pandas.
    write_pandas uses the Snowflake bulk loader internally
    which is optimised for large datasets.
    """
    log.info(f"Loading {len(df):,} rows into {TARGET_SCHEMA}.{TARGET_TABLE}...")
    log.info("This will take several minutes for 6.2M rows...")

    start = datetime.now()

    success, num_chunks, num_rows, output = write_pandas(
        conn=conn,
        df=df,
        table_name=TARGET_TABLE,
        schema=TARGET_SCHEMA,
        chunk_size=100_000,
        auto_create_table=False,
        overwrite=False
    )

    elapsed = (datetime.now() - start).seconds
    log.info(f"Load complete in {elapsed}s")
    log.info(f"Success      : {success}")
    log.info(f"Chunks loaded: {num_chunks}")
    log.info(f"Rows loaded  : {num_rows:,}")

    return num_rows


def verify_load(conn, expected_rows):
    """Verify row count in Snowflake matches expected."""
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {TARGET_SCHEMA}.{TARGET_TABLE}")
    actual_rows = cur.fetchone()[0]
    log.info(f"Verification — Expected: {expected_rows:,} | Actual in Snowflake: {actual_rows:,}")
    assert actual_rows == expected_rows, \
        f"Row count mismatch — expected {expected_rows:,}, got {actual_rows:,}"
    log.info("Row count verification passed")


def log_load_metadata(conn, num_rows):
    """
    Create a simple load log table and record this run.
    Useful for tracking when data was last loaded.
    """
    cur = conn.cursor()

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.LOAD_LOG (
            load_id       NUMBER AUTOINCREMENT,
            table_name    VARCHAR,
            rows_loaded   NUMBER,
            loaded_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            source_file   VARCHAR
        )
    """)

    cur.execute(f"""
        INSERT INTO {TARGET_SCHEMA}.LOAD_LOG
            (table_name, rows_loaded, source_file)
        VALUES
            ('{TARGET_TABLE}', {num_rows}, 'ted_can_cleaned.parquet')
    """)

    log.info("Load metadata recorded in RAW.LOAD_LOG")


def main():
    log.info("=" * 60)
    log.info("EuroProcure — ETL Pipeline")
    log.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    # Load parquet
    df = load_parquet(PARQUET_PATH)

    # Prepare for Snowflake
    df = prepare_dataframe(df)

    # Connect to Snowflake
    conn = get_connection(schema=TARGET_SCHEMA)

    # Create table
    create_table(conn, df)

    # Load data
    num_rows = load_to_snowflake(conn, df)

    # Verify
    verify_load(conn, num_rows)

    # Log metadata
    log_load_metadata(conn, num_rows)

    conn.close()

    log.info("=" * 60)
    log.info("ETL pipeline complete")
    log.info("=" * 60)


if __name__ == "__main__":
    main()