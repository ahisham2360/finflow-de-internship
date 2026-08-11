from dataclasses import dataclass

@dataclass
class PipelineConfig:
    raw_dir: str = "data/raw"
    processed_dir: str = "data/processed"
    db_path: str = "data/finflow.duckdb"
    fred_api_key: str = "521e2b29a2e94c0b206d669aff17aca9"
    max_workers: int = 4
    chunk_size: int = 500_000