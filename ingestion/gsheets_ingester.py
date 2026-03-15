import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import boto3
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv
from writer import write_parquet_to_s3
from utils import get_logger

load_dotenv(dotenv_path="../.env")

logger = get_logger("gsheets_ingester")

SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "Sheet1")
GCP_CREDENTIALS_PATH = os.getenv(
    "GCP_CREDENTIALS_PATH",
    "../configs/gcp_service_account.json"
)
AWS_DEST_PROFILE = os.getenv("AWS_DEST_PROFILE", "default")
DESTINATION_BUCKET = os.getenv("DESTINATION_BUCKET")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly"
]


def get_dest_s3_client():
    """Returns S3 client for your personal AWS account."""
    session = boto3.Session(profile_name=AWS_DEST_PROFILE)
    return session.client("s3")


def file_exists_anywhere_in_s3(source_name: str, filename: str) -> bool:
    """
    Checks if a file exists anywhere under a source prefix
    regardless of date partition.
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


def get_gsheets_client():
    """Authenticates and returns a Google Sheets client."""
    logger.info("Authenticating with Google Sheets API...")
    creds = Credentials.from_service_account_file(
        GCP_CREDENTIALS_PATH,
        scopes=SCOPES
    )
    client = gspread.authorize(creds)
    logger.info("Authentication successful.")
    return client


def ingest_store_locations():
    """Static dataset — skips if already ingested."""
    logger.info("Starting store locations ingestion...")

    if file_exists_anywhere_in_s3("store_locations", "store_locations"):
        logger.info("Store locations already ingested. Skipping.")
        return

    client = get_gsheets_client()
    spreadsheet = client.open_by_key(SHEET_ID)
    worksheet = spreadsheet.worksheet(SHEET_NAME)
    records = worksheet.get_all_records()

    if not records:
        logger.warning("No data found in Google Sheet!")
        return

    df = pd.DataFrame(records)
    logger.info(f"Retrieved {len(df)} rows from Google Sheet.")
    write_parquet_to_s3(df, "store_locations", "store_locations")
    logger.info("Store locations ingestion complete.")