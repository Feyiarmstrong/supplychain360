
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from dotenv import load_dotenv
from utils import get_logger, get_ingestion_timestamp, get_s3_parquet_key

load_dotenv(dotenv_path="../.env")

logger = get_logger("writer")

DESTINATION_BUCKET = os.getenv("DESTINATION_BUCKET")
AWS_DEST_PROFILE = os.getenv("AWS_DEST_PROFILE", "default")


def get_dest_s3_client():
    """Returns an S3 client for your personal AWS account."""
    session = boto3.Session(profile_name=AWS_DEST_PROFILE)
    return session.client("s3")


def add_metadata(df: pd.DataFrame, source_name: str) -> pd.DataFrame:
    """Adds ingestion metadata columns to dataframe."""
    df["_ingested_at"] = get_ingestion_timestamp()
    df["_source"] = source_name
    return df


def write_parquet_to_s3(
    df: pd.DataFrame,
    source_name: str,
    filename: str
) -> str:
    """
    Converts dataframe to parquet and uploads to your S3 raw bucket.
    Returns the S3 key of the uploaded file.
    """
    # Add metadata
    df = add_metadata(df, source_name)

    # Convert to parquet in memory
    buffer = BytesIO()
    table = pa.Table.from_pandas(df)
    pq.write_table(table, buffer)
    buffer.seek(0)

    # Build S3 key
    s3_key = get_s3_parquet_key(source_name, filename)

    # Upload to your S3 bucket
    s3_client = get_dest_s3_client()
    s3_client.put_object(
        Bucket=DESTINATION_BUCKET,
        Key=s3_key,
        Body=buffer.getvalue()
    )

    logger.info(
        f"Successfully wrote {len(df)} rows to "
        f"s3://{DESTINATION_BUCKET}/{s3_key}"
    )

    return s3_key