from dataclasses import dataclass

@dataclass
class PipelineConfig:
    raw_dir: str = "data/raw"
    processed_dir: str = "data/processed"
    db_path: str = "data/finflow.duckdb"
    fred_api_key: str = ""
    max_workers: int = 4
    chunk_size: int = 500_000