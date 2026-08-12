import time
import pandas as pd
import requests
import json
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

from finflow.config.settings import PipelineConfig
from finflow.config.logger import get_logger

from fredapi import Fred

logger = get_logger("IngestSequential")

class IngestionError(Exception):
    """Custom exception raised when an ingestion pipeline fails."""
    pass


def ingest_paysim(config: PipelineConfig) -> None:
    """
    Loads PaySim CSV, validates dtypes, renames columns to snake_case,
    and saves to data/processed/transactions.parquet.
    """
    logger.info("Starting PaySim ingestion...")
    start_time = time.perf_counter()
    
    try:
        raw_path = Path(config.raw_dir) / "paysim dataset.csv"
        processed_path = Path(config.processed_dir) / "transactions.parquet"

        df = pd.read_csv(raw_path)

        df = df.rename(columns={
            'nameOrig': 'name_orig',
            'oldbalanceOrg': 'old_balance_orig',
            'newbalanceOrig': 'new_balance_orig',
            'nameDest': 'name_dest',
            'oldbalanceDest': 'old_balance_dest',
            'newbalanceDest': 'new_balance_dest',
            'isFraud': 'is_fraud',
            'isFlaggedFraud': 'is_flagged_fraud'
        })

        df['step'] = df['step'].astype(int)
        df['type'] = df['type'].astype('category')
        df['amount'] = df['amount'].astype(float)
        df['name_orig'] = df['name_orig'].astype(str)
        df['old_balance_orig'] = df['old_balance_orig'].astype(float)
        df['new_balance_orig'] = df['new_balance_orig'].astype(float)
        df['name_dest'] = df['name_dest'].astype(str)
        df['old_balance_dest'] = df['old_balance_dest'].astype(float)
        df['new_balance_dest'] = df['new_balance_dest'].astype(float)
        df['is_fraud'] = df['is_fraud'].astype(int)
        df['is_flagged_fraud'] = df['is_flagged_fraud'].astype(int)

        is_valid_name_orig = df['name_orig'].str.match(r'^[A-Z]\d{1,15}$')
        is_valid_name_dest = df['name_dest'].str.match(r'^[A-Z]\d{1,15}$')
        is_valid = is_valid_name_orig & is_valid_name_dest

        if not is_valid.all():
            invalid_rows = len(df[~is_valid])
            logger.warning(f"Found {invalid_rows} rows with invalid name_orig or name_dest.")

        processed_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(processed_path, index=False)

        row_count = len(df)
        
        end_time = time.perf_counter()
        logger.info(f"Successfully ingested {row_count} rows in {end_time - start_time:.2f} seconds.")
        
    except Exception as e:
        logger.error(f"PaySim ingestion failed: {str(e)}")
        raise IngestionError(f"Failed to ingest PaySim data: {str(e)}") from e



def ingest_fred(config: PipelineConfig, api_key: str) -> None:
    """
    Fetches CPI (CPIAUCSL), unemployment (UNRATE), FX rate (DEXUSEU),
    saves each as CSV in data/raw/macro/
    """
    logger.info("Starting FRED API ingestion...")
    start_time = time.perf_counter()
    
    # The exact indicators required by the milestone
    indicators = ['CPIAUCSL', 'UNRATE', 'DEXUSEU']
    
    try:
        fred = Fred(api_key=api_key)
        
        # 1. Create the data/raw/macro/ directory if it doesn't exist
        macro_dir = Path(config.raw_dir) / "macro"
        macro_dir.mkdir(parents=True, exist_ok=True)
        
        # 2. Loop through each indicator, fetch it, and save as CSV
        total_fred_rows = 0

        for series_id in indicators:
            logger.info(f"Fetching {series_id}...")
            series = fred.get_series(series_id)
            
            # Convert to DataFrame
            df = pd.DataFrame({
                'date': series.index,
                'value': series.values
            })
            
            # Save as CSV
            csv_path = macro_dir / f"{series_id}.csv"
            df.to_csv(csv_path, index=False)
            logger.info(f"Saved {series_id}.csv to {csv_path}")
            total_fred_rows += len(df)
        end_time = time.perf_counter()
        logger.info(f"Finished FRED ingestion of {total_fred_rows} rows in {end_time - start_time:.2f} seconds.")

    except Exception as e:
        logger.error(f"FRED ingestion failed: {str(e)}")
        raise IngestionError(f"Failed to ingest FRED data: {str(e)}") from e



def ingest_complaints(config: PipelineConfig) -> None:
    """
    Loads CFPB complaints csv, keeps only Credit Card and Checking/Savings complaints,
    then saves them as a parquet file.
    """

    logger.info("Starting complaints ingestion...")
    start_time = time.perf_counter()

    try:

        processed_dir = Path(config.processed_dir)
        processed_dir.mkdir(parents=True, exist_ok=True)

        raw_path = Path(config.raw_dir) / "complaints.csv"
        processed_path = processed_dir / "complaints.parquet"

        products = {
            "Credit card",
            "Checking or savings account"
        }

        writer = None
        row_count = 0

        try:
            text_columns = [
                "Date received",
                "Product",
                "Sub-product",
                "Issue",
                "Sub-issue",
                "Consumer complaint narrative",
                "Company public response",
                "Company",
                "State",
                "ZIP code",
                "Tags",
                "Submitted via",
                "Date sent to company",
                "Company response to consumer",
                "Timely response?"
            ]

            for chunk in pd.read_csv(raw_path, chunksize=100000, low_memory=False):
                for column in text_columns:
                    chunk[column] = chunk[column].fillna("").astype(str)

                chunk = chunk[chunk["Product"].isin(products)]

                if chunk.empty:
                    continue

                table = pa.Table.from_pandas(chunk, preserve_index=False)

                if writer is None:
                    writer = pq.ParquetWriter(processed_path, table.schema)

                writer.write_table(table)
                row_count += len(chunk)

        finally:
            if writer is not None:
                writer.close()    

        end_time = time.perf_counter()
        logger.info(f"Successfully ingested {row_count} complaints in {end_time - start_time:.2f} seconds.")

    except Exception as e:
        logger.error(f"Complaints ingestion failed: {str(e)}")
        raise IngestionError(f"Failed to ingest complaints data: {str(e)}") from e



def run_sequential(config: PipelineConfig, fred_api_key: str) -> None:
    """
    Calls all three in order, wrapped with time.perf_counter() timing. Each ingest function must:
    log start/end times, log row counts, raise a typed custom exception (IngestionError) on failure
    """

    logger.info("Starting sequential ingestion...")
    start_time = time.perf_counter()

    try:
        ingest_paysim(config)
        ingest_fred(config, fred_api_key)
        ingest_complaints(config)

        end_time = time.perf_counter()
        logger.info(f"Finished sequential ingestion in {end_time - start_time:.2f} seconds.")

    except IngestionError as e:
        logger.error(f"Sequential ingestion failed: {str(e)}")
        raise



if __name__ == "__main__":
    config = PipelineConfig()

    run_sequential(config=config, fred_api_key=config.fred_api_key)