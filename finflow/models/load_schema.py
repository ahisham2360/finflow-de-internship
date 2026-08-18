import time
import duckdb
import pandas as pd

from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from finflow.config.settings import PipelineConfig
from finflow.config.logger import get_logger

from multiprocessing import Manager

logger = get_logger("LoadSchema")

def create_schema(config: PipelineConfig) -> None:
    con = duckdb.connect(config.db_path)
    sql = open("finflow/models/schema.sql").read()

    for statement in sql.split(";"):
        if statement.strip():
            con.execute(statement)
    con.close()



def load_dimensions(config: PipelineConfig) -> None:
    con = duckdb.connect(config.db_path)

    try:
        transactions_path = Path(config.processed_dir) / "transactions_transformed.parquet"
        complaints_path = Path(config.processed_dir) / "complaints.parquet"

        df_transactions = pd.read_parquet(transactions_path)
        df_complaints = pd.read_parquet(complaints_path)

        # -------------------------------------------------------------------------------------
        # 1. dim_transaction_type
        # -------------------------------------------------------------------------------------

        transaction_types = (df_transactions["type"].drop_duplicates().sort_values().reset_index(drop=True))

        dim_transaction_type = pd.DataFrame({
            "id": range(1, len(transaction_types)+1),
            "type_name": transaction_types
        })

        con.register("temp_transaction_type", dim_transaction_type)
        con.execute("""
            INSERT INTO dim_transaction_type
            SELECT * FROM temp_transaction_type
        """)
        con.unregister("temp_transaction_type")

        # -------------------------------------------------------------------------------------
        # 2. dim_account
        # -------------------------------------------------------------------------------------

        accounts = pd.concat([df_transactions["name_orig"],df_transactions["name_dest"]]).drop_duplicates().sort_values().reset_index(drop=True)

        dim_account = pd.DataFrame({
            "id": range(1, len(accounts) + 1),
            "name": accounts
        })

        con.register("temp_account", dim_account)
        con.execute("""
            INSERT INTO dim_account
            SELECT * FROM temp_account
        """)
        con.unregister("temp_account")

        # -------------------------------------------------------------------------------------
        # 3. dim_time
        # -------------------------------------------------------------------------------------

        steps = (df_transactions[["step"]].drop_duplicates().sort_values("step").reset_index(drop=True))

        steps["sim_day"] = ((steps["step"] - 1) // 24) + 1
        steps["sim_week"] = ((steps["sim_day"] - 1) // 7) + 1
        steps["hour_of_day"] = (steps["step"] - 1) % 24

        dim_time = steps[
            ["step", "sim_day", "sim_week", "hour_of_day"]
        ]

        con.register("temp_time", dim_time)
        con.execute("""
            INSERT INTO dim_time
            SELECT * FROM temp_time
        """)
        con.unregister("temp_time")

        # -------------------------------------------------------------------------------------
        # 4. complaints
        # -------------------------------------------------------------------------------------

        df_complaints = df_complaints.rename(columns={
            "Date received": "date_received",
            "Product": "product",
            "Sub-product": "sub_product",
            "Issue": "issue",
            "Company": "company",
            "State": "state",
            "Company response to consumer": "resolution",
            "Complaint ID": "complaint_id"
        })

        df_complaints = df_complaints[
            [
                "complaint_id",
                "date_received",
                "product",
                "sub_product",
                "issue",
                "company",
                "state",
                "resolution"
            ]
        ]

        df_complaints["date_received"] = pd.to_datetime(
            df_complaints["date_received"],
            errors="coerce"
        )

        con.register("temp_complaints", df_complaints)
        con.execute("""
            INSERT INTO complaints
            SELECT * FROM temp_complaints
        """)
        con.unregister("temp_complaints")

    finally:
        con.close()



def load_fact_chunk(chunk: pd.DataFrame, db_path: str, lock) -> int:
    con = None

    try:
        lock.acquire()

        con = duckdb.connect(db_path)

        con.register("temp_chunk", chunk)
        con.execute("""
            INSERT INTO fact_transactions
            SELECT
                c.transaction_id,
                c.step,
                tt.id AS transaction_type_id,
                c.amount,
                c.log_amount,
                c.balance_drain,
                s.id AS sender_account_id,
                r.id AS receiver_account_id,
                c.is_fraud,
                c.is_flagged_fraud,
                c.old_balance_orig AS old_balance_sender,
                c.new_balance_orig AS new_balance_sender,
                c.old_balance_dest AS old_balance_receiver,
                c.new_balance_dest AS new_balance_receiver
            FROM temp_chunk c
            JOIN dim_transaction_type tt
                ON c.type = tt.type_name
            JOIN dim_account s
                ON c.name_orig = s.name
            JOIN dim_account r
                ON c.name_dest = r.name
        """)
        con.unregister("temp_chunk")

        return len(chunk)

    finally:
        if con is not None:
            con.close()

        lock.release()



def load_fact_transactions(config: PipelineConfig) -> None:
    path = Path(config.processed_dir) / "transactions_transformed.parquet"

    df = pd.read_parquet(path)

    df["transaction_id"] = range(1, len(df) + 1)

    chunks = [
        df.iloc[i:i + config.chunk_size]
        for i in range(0, len(df), config.chunk_size)
    ]

    with Manager() as manager:
        lock = manager.Lock()

        with ProcessPoolExecutor(max_workers=config.max_workers) as executor:
            futures = [
                executor.submit(
                    load_fact_chunk,
                    chunk,
                    config.db_path,
                    lock
                )
                for chunk in chunks
            ]

            row_counts = [future.result() for future in futures]
            total_rows = sum(row_counts)
            source_row_count = len(df)

            logger.info(f"Loaded {total_rows} fact transactions.")

            verify_row_count(config, source_row_count)



def verify_row_count(config: PipelineConfig, source_row_count: int) -> None:
    con = duckdb.connect(config.db_path)

    try:
        db_row_count = con.execute("""
            SELECT COUNT(*)
            FROM fact_transactions
        """).fetchone()[0]

        logger.info(f"Source row count: {source_row_count}")
        logger.info(f"DuckDB row count: {db_row_count}")

        if source_row_count != db_row_count:
            raise ValueError(
                f"Row count mismatch:"
                f"source={source_row_count}"
                f"DuckDB={db_row_count}"
            )

        logger.info("Fact transaction row count verified successfully.")

    finally:
        con.close()



def main(config: PipelineConfig) -> None:
    start_time = time.perf_counter()

    logger.info("Starting DuckDB load...")

    con = duckdb.connect(config.db_path)

    try:
        con.execute("DROP TABLE IF EXISTS fact_transactions")
        con.execute("DROP TABLE IF EXISTS dim_transaction_type")
        con.execute("DROP TABLE IF EXISTS dim_account")
        con.execute("DROP TABLE IF EXISTS dim_time")
        con.execute("DROP TABLE IF EXISTS complaints")

    finally:
        con.close()

    create_schema(config)
    load_dimensions(config)
    load_fact_transactions(config)

    total_time = time.perf_counter() - start_time

    logger.info(f"Total time: {total_time:.2f} seconds")



if __name__ == "__main__":

    config = PipelineConfig()
    main(config)