import duckdb
import pandas as pd

from pathlib import Path

from finflow.config.settings import PipelineConfig
from finflow.config.logger import get_logger

logger = get_logger("DataQuality")



class DataQualityError(Exception):
    """Raised when a data quality check fails."""
    pass



def check_duplicates(con) -> None:

    duplicate_count = con.execute("""
        SELECT COUNT(*)
        FROM (
            SELECT transaction_id
            FROM fact_transactions
            GROUP BY transaction_id
            HAVING COUNT(*) > 1
        );
    """).fetchone()[0]

    if duplicate_count > 0:
        raise DataQualityError(f"Found {duplicate_count} duplicated transaction IDs.")

    logger.info("Duplicate transaction ID check passed.")



def check_fraud_nulls(con) -> None:

    null_count = con.execute("""
        SELECT COUNT(*)
        FROM fact_transactions
        WHERE is_fraud IS NULL;
    """).fetchone()[0]

    if null_count > 0:
        raise DataQualityError(f"Found {null_count} NULL values in is_fraud.")

    logger.info("is_fraud NULL check passed.")



def check_foreign_keys(con) -> None:

    # transaction_type_id
    invalid_transaction_types = con.execute("""
        SELECT COUNT(*)
        FROM fact_transactions f
        LEFT JOIN dim_transaction_type tt
            ON f.transaction_type_id = tt.id
        WHERE tt.id IS NULL;
    """).fetchone()[0]

    if invalid_transaction_types > 0:
        raise DataQualityError(f"Found {invalid_transaction_types} invalid transaction_type_id values.")


    # sender
    invalid_senders = con.execute("""
        SELECT COUNT(*)
        FROM fact_transactions f
        LEFT JOIN dim_account s
            ON f.sender_account_id = s.id
        WHERE s.id IS NULL;
    """).fetchone()[0]

    if invalid_senders > 0:
        raise DataQualityError(f"Found {invalid_senders} invalid sender_account_id values.")


    # receiver
    invalid_receivers = con.execute("""
        SELECT COUNT(*)
        FROM fact_transactions f
        LEFT JOIN dim_account r
            ON f.receiver_account_id = r.id
        WHERE r.id IS NULL;
    """).fetchone()[0]

    if invalid_receivers > 0:
        raise DataQualityError(f"Found {invalid_receivers} invalid receiver_account_id values.")


    # step
    invalid_steps = con.execute("""
        SELECT COUNT(*)
        FROM fact_transactions f
        LEFT JOIN dim_time t
            ON f.step = t.step
        WHERE t.step IS NULL;
    """).fetchone()[0]

    if invalid_steps > 0:
        raise DataQualityError(f"Found {invalid_steps} invalid step values.")



def check_negative_amounts(con) -> None:

    negative_count = con.execute("""
        SELECT COUNT(*)
        FROM fact_transactions
        WHERE amount < 0;
    """).fetchone()[0]

    if negative_count > 0:
        raise DataQualityError(f"Found {negative_count} transactions with negative amounts.")

    logger.info("Negative amount check passed.")



def run_quality_checks(config: PipelineConfig) -> None:

    logger.info(f"Starting data quality checks...")
    con = duckdb.connect(config.db_path)

    try:
        check_duplicates(con)
        check_fraud_nulls(con)
        check_foreign_keys(con)
        check_negative_amounts(con)

        logger.info(f"All data quality checks passed.")

    except DataQualityError as e:
        logger.error(f"Data quality check failed: {str(e)}")
        raise

    finally:
        con.close()



if __name__ == "__main__":

    config = PipelineConfig()
    run_quality_checks(config)