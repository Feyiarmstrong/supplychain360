import logging
from datetime import datetime, timezone


def get_logger(name: str) -> logging.Logger:
    """Returns a configured logger."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    return logging.getLogger(name)


def get_ingestion_timestamp() -> str:
    """Returns current UTC timestamp as string."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def get_s3_parquet_key(source_name: str, filename: str) -> str:
    """
    Builds the S3 key for storing parquet files.
    Example: products/ingested_at=2026-03-12/products.parquet
    """
    date_partition = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return f"{source_name}/ingested_at={date_partition}/{filename}.parquet"