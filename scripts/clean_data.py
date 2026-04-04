"""
clean_data.py
-------------
Reads the raw TED Contract Award Notices CSV (2018-2023),
applies all cleaning and feature engineering decisions from EDA,
and saves the result as a parquet file ready for Snowflake loading.

Decisions documented in:
- notebooks/01_data_profiling.ipynb
- notebooks/02_missingness_analysis.ipynb
- notebooks/03_eda_distributions.ipynb
"""

import pandas as pd
import numpy as np
import os
import logging
from datetime import datetime

# ── Logging setup ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s — %(levelname)s — %(message)s'
)
log = logging.getLogger(__name__)

# ── Paths ──────────────────────────────────────────────────────
BASE_DIR = os.path.expanduser("~/Desktop/europrocure-intelligence")
RAW_PATH = os.path.join(BASE_DIR, "data/raw/export_CAN_2023_2018.csv")
OUT_PATH = os.path.join(BASE_DIR, "data/processed/ted_can_cleaned.parquet")

# ── Columns to drop (>70% missing — confirmed in EDA) ─────────
COLS_TO_DROP = [
    'ISO_COUNTRY_CODE_ALL',    # 100% missing
    'EU_INST_CODE',            # 99.9% missing
    'B_ACCELERATED',           # 97.8% missing
    'INFO_ON_NON_AWARD',       # 88.2% missing
    'FRA_ESTIMATED',           # 79.5% missing
    'NUMBER_TENDERS_NON_EU',   # 73.6% missing
    'NUMBER_TENDERS_OTHER_EU', # 73.2% missing
    'WIN_NATIONALID',          # 72.7% missing
]

# ── Value outlier cap (99th percentile from EDA) ───────────────
VALUE_CAP = 103_891_989  # €103M

def load_raw(path):
    """Load full CSV in chunks and concatenate."""
    log.info("Loading raw CSV in chunks...")
    chunks = []
    total = 0
    for chunk in pd.read_csv(
        path,
        chunksize=200_000,
        encoding='utf-8',
        low_memory=False
    ):
        chunks.append(chunk)
        total += len(chunk)
        log.info(f"  Loaded {total:,} rows so far...")

    df = pd.concat(chunks, ignore_index=True)
    log.info(f"Raw dataset loaded: {len(df):,} rows, {len(df.columns)} columns")
    return df


def drop_columns(df):
    """Drop columns confirmed >70% missing in EDA."""
    cols_present = [c for c in COLS_TO_DROP if c in df.columns]
    df = df.drop(columns=cols_present)
    log.info(f"Dropped {len(cols_present)} high-missingness columns")
    return df


def filter_cancelled(df):
    """Remove cancelled notices — per codebook section 3.9."""
    before = len(df)
    df = df[df['CANCELLED'] != 1].copy()
    after = len(df)
    log.info(f"Removed {before - after:,} cancelled notices")
    return df


def parse_dates(df):
    """
    Parse DT_DISPATCH and DT_AWARD from DD/MM/YY string to datetime.
    Format confirmed in EDA: '22/12/17' = 22 Dec 2017.
    """
    for col in ['DT_DISPATCH', 'DT_AWARD']:
        df[col] = pd.to_datetime(df[col], format='%d/%m/%y', errors='coerce')
    log.info("Parsed DT_DISPATCH and DT_AWARD to datetime")
    return df


def clean_values(df):
    """
    Cap VALUE_EURO_FIN_2 at 99th percentile (€103M).
    Remove zero and negative values.
    These are confirmed data entry errors from EDA.
    """
    # Remove zeros and negatives
    df['VALUE_EURO_FIN_2'] = df['VALUE_EURO_FIN_2'].where(
        df['VALUE_EURO_FIN_2'] > 0, np.nan
    )
    # Cap at 99th percentile
    df['VALUE_EURO_FIN_2'] = df['VALUE_EURO_FIN_2'].clip(upper=VALUE_CAP)

    # Apply same logic to AWARD_VALUE_EURO_FIN_1
    df['AWARD_VALUE_EURO_FIN_1'] = df['AWARD_VALUE_EURO_FIN_1'].where(
        df['AWARD_VALUE_EURO_FIN_1'] > 0, np.nan
    )
    df['AWARD_VALUE_EURO_FIN_1'] = df['AWARD_VALUE_EURO_FIN_1'].clip(upper=VALUE_CAP)

    log.info(f"Capped contract values at €{VALUE_CAP:,}")
    return df


def clean_country_codes(df):
    """
    WIN_COUNTRY_CODE contains dirty values like 'PT---PT'.
    Take only the first value before '---'.
    Confirmed in EDA notebook 01.
    """
    df['WIN_COUNTRY_CODE'] = (
        df['WIN_COUNTRY_CODE']
        .astype(str)
        .str.split('---')
        .str[0]
        .str.strip()
        .replace('nan', np.nan)
    )
    log.info("Cleaned WIN_COUNTRY_CODE — extracted first value before ---")
    return df


def standardise_column_names(df):
    """Convert all column names to lowercase snake_case."""
    df.columns = df.columns.str.lower()
    log.info("Standardised column names to lowercase")
    return df


def engineer_features(df):
    """
    Create new columns needed for business analysis.
    All decisions justified by EDA findings.
    """
    # Award year and quarter — from DT_AWARD
    df['award_year'] = df['dt_award'].dt.year
    df['award_quarter'] = df['dt_award'].dt.quarter
    df['award_month'] = df['dt_award'].dt.month

    # CPV division — first 2 digits of CPV code
    # e.g. 72300000 → '72' = IT services
    df['cpv_division'] = df['cpv'].astype(str).str[:2]

    # COVID period classification
    # pre_covid: 2018-2019, covid: 2020-2021, post_covid: 2022-2023
    def classify_covid_period(year):
        if year in [2018, 2019]:
            return 'pre_covid'
        elif year in [2020, 2021]:
            return 'covid'
        elif year in [2022, 2023]:
            return 'post_covid'
        else:
            return 'unknown'

    df['covid_period'] = df['year'].apply(classify_covid_period)

    # Contract value band
    def value_band(val):
        if pd.isna(val):
            return 'unknown'
        elif val < 50_000:
            return 'low'
        elif val < 500_000:
            return 'mid'
        elif val < 5_000_000:
            return 'high'
        else:
            return 'very_high'

    df['value_band'] = df['value_euro_fin_2'].apply(value_band)

    # SME flag — clean boolean from Y/N string
    df['is_sme'] = df['b_contractor_sme'].map({'Y': True, 'N': False})

    # Framework agreement flag
    df['is_framework'] = df['b_fra_agreement'].map({'Y': True, 'N': False})

    # Competition level — number of offers bucketed
    def competition_level(n):
        if pd.isna(n):
            return 'unknown'
        elif n == 1:
            return 'single_bidder'
        elif n <= 5:
            return 'low'
        elif n <= 10:
            return 'medium'
        else:
            return 'high'

    df['competition_level'] = df['number_offers'].apply(competition_level)

    log.info("Feature engineering complete — 8 new columns created")
    return df


def deduplicate(df):
    """
    The dataset has ~4 rows per notice due to lot/award level duplication.
    We keep all rows but flag the first occurrence of each notice.
    Deduplication for aggregations happens in dbt, not here.
    This flag allows dbt to deduplicate cleanly.
    """
    df['is_first_award'] = ~df['id_notice_can'].duplicated(keep='first')
    first_count = df['is_first_award'].sum()
    log.info(f"Flagged {first_count:,} first-occurrence notices (is_first_award=True)")
    return df


def validate(df):
    """Basic validation checks before saving."""
    log.info("Running validation checks...")

    # Check key columns exist
    required = ['id_notice_can', 'year', 'iso_country_code',
                'top_type', 'value_euro_fin_2', 'dt_award']
    for col in required:
        assert col in df.columns, f"Missing required column: {col}"

    # Check year range
    years = df['year'].dropna().unique()
    assert all(y in range(2018, 2024) for y in years), "Unexpected year values"

    # Check no negative values in value column
    neg = (df['value_euro_fin_2'] < 0).sum()
    assert neg == 0, f"Found {neg} negative values in value_euro_fin_2"

    log.info("All validation checks passed")
    return df


def save(df, path):
    """Save cleaned dataframe as parquet."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_parquet(path, index=False, engine='pyarrow')
    size_mb = os.path.getsize(path) / (1024 ** 2)
    log.info(f"Saved to {path}")
    log.info(f"File size: {size_mb:.1f} MB")
    log.info(f"Final shape: {df.shape[0]:,} rows, {df.shape[1]} columns")


def main():
    log.info("=" * 60)
    log.info("EuroProcure — Data Cleaning Pipeline")
    log.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    df = load_raw(RAW_PATH)
    df = drop_columns(df)
    df = filter_cancelled(df)
    df = parse_dates(df)
    df = clean_values(df)
    df = clean_country_codes(df)
    df = standardise_column_names(df)
    df = engineer_features(df)
    df = deduplicate(df)
    df = validate(df)
    save(df, OUT_PATH)

    log.info("=" * 60)
    log.info("Cleaning pipeline complete")
    log.info("=" * 60)


if __name__ == "__main__":
    main()