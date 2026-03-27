import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import boto3
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from writer import write_parquet_to_s3
from utils import get_logger

load_dotenv(dotenv_path="../.env")

logger = get_logger(__name__)

AWS_SOURCE_PROFILE = os.getenv("AWS_SOURCE_PROFILE", "supplychain360")
AWS_DEST_PROFILE = os.getenv("AWS_DEST_PROFILE", "default")
DESTINATION_BUCKET = os.getenv("DESTINATION_BUCKET")
SSM_REGION = os.getenv("SSM_REGION", "eu-west-2")

# AWS CLIENTS

def get_ssm_client():
    """Returns SSM client using SupplyChain360 AWS profile."""
    session = boto3.Session(profile_name=AWS_SOURCE_PROFILE)
    return session.client("ssm", region_name=SSM_REGION)


def get_dest_s3_client():
    """Returns S3 client for your personal AWS account."""
    session = boto3.Session(profile_name=AWS_DEST_PROFILE)
    return session.client("s3")


# SSM + DB CONNECTION

def get_ssm_parameter(name: str) -> str:
    """Fetches a single parameter from SSM Parameter Store."""
    ssm = get_ssm_client()
    parameter_name = f"/supplychain360/db/{name}"
    logger.info(f"Fetching SSM parameter: {parameter_name}")

    response = ssm.get_parameter(
        Name=parameter_name,
        WithDecryption=True
    )
    return response["Parameter"]["Value"]


def build_engine():
    """Builds SQLAlchemy engine using credentials from SSM."""
    logger.info("Fetching database credentials from SSM...")

    host = get_ssm_parameter("host")
    port = get_ssm_parameter("port")
    dbname = get_ssm_parameter("dbname")
    user = get_ssm_parameter("user")
    password = get_ssm_parameter("password")

    connection_string = (
        f"postgresql+psycopg2://{user}:{password}"
        f"@{host}:{port}/{dbname}"
    )

    logger.info("Database engine created successfully.")
    return create_engine(
        connection_string,
        connect_args={"sslmode": "require"}
    )



# TABLE DISCOVERY

def get_sales_tables(engine) -> list:
    """
    Dynamically discovers all sales tables in public schema.
    Finds any table matching the pattern sales_YYYY_MM_DD.
    """
    query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        AND table_name LIKE 'sales_%'
        ORDER BY table_name;
    """

    df = pd.read_sql(text(query), engine)
    tables = df["table_name"].tolist()

    logger.info(f"Discovered {len(tables)} sales tables: {tables}")
    return tables


# IDEMPOTENCY CHECK

def file_exists_anywhere_in_s3(source_name: str, filename: str) -> bool:
    """
    Checks if a file exists anywhere under a source prefix.
    Returns True if exists, False if not.
    """
    s3_client = get_dest_s3_client()

    response = s3_client.list_objects_v2(
        Bucket=DESTINATION_BUCKET,
        Prefix=f"{source_name}/"
    )

    if "Contents" not in response:
        return False

    for obj in response["Contents"]:
        if filename in obj["Key"]:
            logger.info(
                f"Found existing file for {filename}: "
                f"{obj['Key']} — Skipping."
            )
            return True

    return False



# DATA CLEANING

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans dataframe before writing to Parquet.
    Converts UUID and mixed object types to strings.
    """
    import uuid

    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, uuid.UUID)).any():
            df[col] = df[col].astype(str)
            logger.info(f"Converted UUID column to string: {col}")

        elif df[col].dtype == object:
            df[col] = df[col].astype(str)

    return df


# INGEST SINGLE TABLE

def ingest_sales_table(engine, table_name: str):
    """
    Ingests a single sales table into S3 as Parquet.
    Skips if already ingested.
    Retries once on failure with fresh engine.
    """
    if file_exists_anywhere_in_s3("store_sales", table_name):
        logger.info(f"Skipping {table_name} — already ingested.")
        return

    logger.info(f"Ingesting table: public.{table_name}")
    query = f"SELECT * FROM public.{table_name}"

    # First attempt
    try:
        df = pd.read_sql(text(query), engine)
        df = clean_dataframe(df)
        write_parquet_to_s3(df, "store_sales", table_name)
        logger.info(f"Table {table_name} ingested successfully.")
        return

    except Exception as e:
        logger.error(f"First attempt failed for {table_name}: {e}")

    # Retry with fresh engine
    logger.info(f"Retrying {table_name} with fresh connection...")

    try:
        new_engine = build_engine()
        df = pd.read_sql(text(query), new_engine)
        df = clean_dataframe(df)
        write_parquet_to_s3(df, "store_sales", table_name)
        logger.info(f"Table {table_name} ingested successfully on retry.")

    except Exception as retry_error:
        logger.error(f"Retry failed for {table_name}: {retry_error}")



# INGEST ALL TABLES

def ingest_all_sales():
    """
    Ingests all sales tables dynamically discovered from database.
    Automatically picks up new tables without code changes.
    """
    logger.info("Starting store sales ingestion...")

    engine = build_engine()

    tables = get_sales_tables(engine)

    if not tables:
        logger.warning("No sales tables found. Skipping.")
        return

    for table in tables:
        ingest_sales_table(engine, table)

    logger.info("Store sales ingestion complete.")


# ENTRY POINT

if __name__ == "__main__":
    ingest_all_sales()