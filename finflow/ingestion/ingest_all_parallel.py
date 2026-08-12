import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from finflow.config.settings import PipelineConfig
from finflow.config.logger import get_logger

from finflow.ingestion.ingest_all_sequential import (
    ingest_paysim,
    ingest_fred,
    ingest_complaints,
    IngestionError,
    run_sequential
)

logger = get_logger("IngestParallel")



def run_parallel(config: PipelineConfig, fred_api_key: str, max_workers: int = 3) -> None:

    logger.info("Starting parallel ingestion...")
    start_time = time.perf_counter()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:

        futures = {
            executor.submit(ingest_paysim, config): "PaySim",
            executor.submit(ingest_fred, config, fred_api_key): "FRED",
            executor.submit(ingest_complaints, config): "Complaints"
        }

        for future in as_completed(futures):
            name = futures[future]

            try:
                future.result()
                logger.info(f"{name} ingestion done.")

            except Exception as e:
                logger.error(f"{name} ingestion failed: {e}")

    end_time = time.perf_counter()
    logger.info(f"Finished parallel ingestion in {end_time - start_time:.2f} seconds.")




def benchmark_ingestion(config: PipelineConfig, fred_api_key: str) -> None:
    logger.info("Starting Benchmark Ingestion...")

    try:
        # Sequential benchmark
        start_time = time.perf_counter()
        run_sequential(config, fred_api_key)
        sequential_time = time.perf_counter() - start_time

        # Parallel benchmark
        start_time = time.perf_counter()
        run_parallel(config, fred_api_key)
        parallel_time = time.perf_counter() - start_time

        speedup = sequential_time / parallel_time

        print("\n")
        print("=" * 50)
        print("           INGESTION BENCHMARK")
        print("=" * 50)
        print(f"{'Method':<20}{'Time (s)':>15}{'Speedup':>15}")
        print("-" * 50)
        print(f"{'Sequential':<20}{sequential_time:>15.2f}{'1.00x':>15}")
        print(f"{'Parallel':<20}{parallel_time:>15.2f}{speedup:>14.2f}x")
        print("=" * 50)

    except IngestionError as e:
        logger.error(f"Benchmark ingestion failed: {str(e)}")
        raise



if __name__ == "__main__":
    config = PipelineConfig()

    benchmark_ingestion(config=config, fred_api_key=config.fred_api_key)