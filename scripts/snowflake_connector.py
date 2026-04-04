"""
snowflake_connector.py
----------------------
Reusable Snowflake connection helper.
Reads credentials from .env file — never hardcoded.
Used by etl_pipeline.py and streamlit app.
"""

import snowflake.connector
from dotenv import load_dotenv
import os
import logging

log = logging.getLogger(__name__)

def get_connection(schema: str = None):
    """
    Returns an active Snowflake connection.
    Schema defaults to SNOWFLAKE_SCHEMA in .env
    but can be overridden by passing schema argument.
    """
    load_dotenv()

    account  = os.getenv('SNOWFLAKE_ACCOUNT')
    user     = os.getenv('SNOWFLAKE_USER')
    password = os.getenv('SNOWFLAKE_PASSWORD')
    warehouse= os.getenv('SNOWFLAKE_WAREHOUSE')
    database = os.getenv('SNOWFLAKE_DATABASE')
    role     = os.getenv('SNOWFLAKE_ROLE')
    schema   = schema or os.getenv('SNOWFLAKE_SCHEMA', 'RAW')

    conn = snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        warehouse=warehouse,
        database=database,
        schema=schema,
        role=role
    )

    log.info(f"Connected to Snowflake — {database}.{schema}")
    return conn


def test_connection():
    """Quick connection test — prints current user and schema."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT CURRENT_USER(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
    result = cur.fetchone()
    print(f"Connected as : {result[0]}")
    print(f"Database     : {result[1]}")
    print(f"Schema       : {result[2]}")
    conn.close()


if __name__ == "__main__":
    test_connection()