
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
from utils import get_logger

from s3_ingester import (
    ingest_products,
    ingest_warehouses,
    ingest_suppliers,
    ingest_inventory,
    ingest_shipments
)
from postgres_ingester import ingest_all_sales
from gsheets_ingester import ingest_store_locations

load_dotenv(dotenv_path="../.env")

logger = get_logger("main")


def run_all_ingestions():
    """Runs all ingestion pipelines in sequence."""

    logger.info("=" * 50)
    logger.info("Starting SupplyChain360 Data Ingestion")
    logger.info("=" * 50)

    # S3 Sources
    logger.info("--- S3 Sources ---")
    ingest_products()
    ingest_warehouses()
    ingest_suppliers()
    ingest_inventory()
    ingest_shipments()

    # PostgreSQL Source
    logger.info("--- PostgreSQL Source ---")
    ingest_all_sales()

    # Google Sheets Source
    logger.info("--- Google Sheets Source ---")
    ingest_store_locations()

    logger.info("=" * 50)
    logger.info("All ingestions completed successfully!")
    logger.info("=" * 50)



if __name__ == "__main__":
    run_all_ingestions()