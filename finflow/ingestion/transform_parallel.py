import time
import pandas as pd
import numpy as np
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from finflow.config.logger import get_logger
from finflow.config.settings import PipelineConfig

logger = get_logger("IngestParallel")



def transform_chunk(df: pd.DataFrame) -> pd.DataFrame:
    
    df["amount"] = df["amount"].astype(float)
    df["old_balance_orig"] = df["old_balance_orig"].astype(float)
    df["new_balance_orig"] = df["new_balance_orig"].astype(float)

    df["balance_drain"] = (df["old_balance_orig"] - df["new_balance_orig"] - df["amount"])

    df["log_amount"] = np.log1p(df["amount"])

    return df



def transform_sequential(config: PipelineConfig, chunk_size: int) -> None:
    parquet_path = Path(config.processed_dir) / "transactions.parquet"
    output_path = Path(config.processed_dir) / "transactions_transformed.parquet"

    df = pd.read_parquet(parquet_path)

    chunks = [
        df.iloc[i:i + chunk_size]
        for i in range(0, len(df), chunk_size)
    ]

    results = [transform_chunk(chunk) for chunk in chunks]

    transformed_df = pd.concat(results, ignore_index=True)
    transformed_df.to_parquet(output_path, index=False)


def transform_parallel(config: PipelineConfig, chunk_size: int, n_workers: int) -> None:
    parquet_path = Path(config.processed_dir) / "transactions.parquet"
    output_path = Path(config.processed_dir) / "transactions_transformed.parquet"

    df = pd.read_parquet(parquet_path)

    chunks = [
        df.iloc[i:i + chunk_size]
        for i in range(0, len(df), chunk_size)
    ]
           
    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        results = list(executor.map(transform_chunk, chunks))

    transformed_df = pd.concat(results, ignore_index=True)
    transformed_df.to_parquet(output_path, index=False)



def benchmark_transformation(config: PipelineConfig) -> None:
    logger.info("Starting Benchmark Transformation...")

    chunk_size = 500000
    n_workers = 4

    try:

        # Parallel benchmark
        start_time = time.perf_counter()
        transform_parallel(config, chunk_size, n_workers)
        parallel_time = time.perf_counter() - start_time

        # Sequential benchmark
        start_time = time.perf_counter()
        transform_sequential(config, chunk_size)
        sequential_time = time.perf_counter() - start_time

        speedup = sequential_time / parallel_time

        print("\n")
        print("=" * 50)
        print("            TRANSFORMATION BENCHMARK")
        print("=" * 50)
        print(f"Chunk size:       {chunk_size:,} rows")
        print(f"Worker processes:     {n_workers}")
        print("-" * 50)
        print(f"{'Method':<20}{'Time (s)':>14}{'Speedup':>15}")
        print("-" * 50)
        print(f"{'Sequential':<20}{sequential_time:>13.2f} {'1.00x':>15}")
        print(f"{'Parallel':<20}{parallel_time:>13.2f} {speedup:>14.2f}x")
        print("=" * 50)

    except Exception as e:
        logger.error(f"Benchmark transformation failed: {str(e)}")
        raise


if __name__ == "__main__":
    config = PipelineConfig()

    benchmark_transformation(config=config)