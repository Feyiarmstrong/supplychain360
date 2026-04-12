# SupplyChain360 — A Unified Supply Chain Data Platform
 
Stack: Python • PostgreSQL • Google Sheets • AWS S3 • Snowflake • dbt • Airflow • Terraform • Docker • GitHub Actions  

-----

## What is SupplyChain360?

SupplyChain360 is a production-grade, end-to-end supply chain data platform that consolidates fragmented operational data from multiple heterogeneous sources into a single, reliable, analytics-ready data warehouse. It automates the entire data lifecycle from raw ingestion and transformation to modelled, queryable business intelligence using modern cloud infrastructure and data engineering tooling.

### The Problem it Solves

In a real supply chain operation, data lives in silos. Inventory is in one system. Shipments are in another. Product and supplier data live in flat files and spreadsheets. Store locations are maintained manually. Nobody has a clear, unified view of what’s moving, what’s delayed, what’s overstocked, or which supplier is underperforming.

### Without a unified platform, teams can’t answer the questions that matter:

- Which warehouses are approaching critical inventory levels right now?
- Which suppliers have the highest delivery failure rate this quarter?
- How many shipments are currently delayed, and in which regions?
- Are we stocking the right products across the right stores?

SupplyChain360 solves this by building a fully automated pipeline that pulls from every data source, lands raw data in cloud object storage, loads it into a cloud data warehouse, and transforms it into clean, business-ready dimensional models — on a daily schedule, with no manual intervention.

-----

-----

## Tech Stack

|Component             |Technology             |
|----------------------|-----------------------|
|Cloud Infrastructure  |AWS (S3, SSM, DynamoDB)|
|Data Warehouse        |Snowflake              |
|Transformation        |dbt Core               |
|Orchestration         |Apache Airflow 2.8.1   |
|Containerization      |Docker                 |
|Infrastructure as Code|Terraform              |
|CI/CD                 |GitHub Actions         |
|Language              |Python 3.11            |
|Storage Format        |Apache Parquet         |
|Source Database       |PostgreSQL via Supabase|

-----

## Project Structure

supplychain360/

├── .github/

│   └── workflows/

│       └── ci_cd.yml                  # GitHub Actions pipeline

├── airflow/

│   ├── Dockerfile                     # Custom Airflow image

│   ├── docker-compose.yml             # Airflow service definitions

│   ├── requirements.txt               # Airflow Python dependencies

│   └── dags/

│       └── supplychain360_dag.py      # Main orchestration DAG

├── configs/                           # GCP service account credentials (gitignored)

├── dbt/

│   └── supplychain360_dbt/

│       ├── models/

│       │   ├── staging/               # 7 staging models (views)

│       │   └── analytics/             # 4 dimension + 3 fact tables

│       ├── tests/                     # Custom dbt data tests

│       └── dbt_project.yml


├── ingestion/

│   ├── main.py                        # Entry point — runs all ingesters

│   ├── s3_ingester.py                 # Reads CSVs/JSON from source S3 bucket

│   ├── postgres_ingester.py           # Reads store_sales from Supabase

│   ├── gsheets_ingester.py            # Reads store_locations from Google Sheets

│   ├── writer.py                      # Writes Parquet to destination S3 bucket

│   └── utils.py                       # Shared helpers (logging, config loading)

├── terraform/

│   ├── main.tf                        # Provider configuration + remote state backend

│   ├── variables.tf                   # Input variable declarations

│   ├── s3.tf                          # S3 bucket definitions

│   ├── dynamodb.tf                    # DynamoDB table for state locking

│   └── outputs.tf                     # Output values (bucket ARNs, table name)

├── .env.example                       # Template for local environment variables

├── .gitignore

├── docker-compose.yml                 # Root compose file for ingestion container

└── requirements.txt

-----

## Data Sources

|Source         |Type  |Format       |Rows     |Update Frequency|
|---------------|------|-------------|---------|----------------|
|Products       |Static|CSV (S3)     |63       |One-time        |
|Warehouses     |Static|CSV (S3)     |10       |One-time        |
|Suppliers      |Static|CSV (S3)     |12       |One-time        |
|Store Locations|Static|Google Sheets|800      |One-time        |
|Inventory      |Daily |CSV (S3)     |56,100   |Daily           |
|Shipments      |Daily |JSON (S3)    |350,000  |Daily           |
|Store Sales    |Daily |PostgreSQL   |1,400,000|Daily           |

Total: 1,806,985 rows across 7 entities

-----

## How Data Flows

Every day at 6:00 AM UTC, Airflow triggers the pipeline. Here is what happens to a single sales transaction from the moment it exists in Supabase to the moment it is queryable in a dashboard:

1. Ingestion — postgres_ingester.py connects to Supabase via the Transaction Pooler on port 6543 with sslmode=require. It reads the store_sales table and passes the data to writer.py.

2. Landing — writer.py serialises the data to Parquet format and writes it to s3://supplychain360-raw-feyisayo/store_sales/ in the personal AWS account. If the file already exists for today, it is skipped — ingestion is fully idempotent.

3. Loading — Snowflake’s COPY INTO command pulls the Parquet file from S3 into the RAW.STORE_SALES table in SUPPLYCHAIN360_DB.

4. Staging — dbt runs stg_store_sales, a view that casts data types, parses timestamps, renames columns to a consistent naming convention, and filters out any rows with null primary keys.

5. Analytics — dbt runs fct_sales, a table model that joins stg_store_sales to dim_stores and dim_products, enriching each transaction with store region and full product metadata.

6. Testing — dbt tests validate that fct_sales has no null order IDs, no duplicate records, and that all foreign key references resolve correctly. If any test fails, the DAG task fails and the bad data is visible in Airflow logs before it ever reaches a dashboard.

-----

## Snowflake Setup

- Account
- Database  
- Warehouse (X-Small, auto-suspend 60s)  
- Schemas: RAW, STAGING, ANALYTICS

### RAW Tables (loaded by Python)

|Table                |Rows     |Source       |
|---------------------|---------|-------------|
|`RAW_PRODUCTS`       |63       |S3 CSV       |
|`RAW_WAREHOUSES`     |10       |S3 CSV       |
|`RAW_SUPPLIERS`      |12       |S3 CSV       |
|`RAW_STORE_LOCATIONS`|800      |Google Sheets|
|`RAW_INVENTORY`      |56,100   |S3 CSV       |
|`RAW_SHIPMENTS`      |350,000  |S3 JSON      |
|`RAW_STORE_SALES`    |1,400,000|PostgreSQL   |

### Staging Views (dbt)

stg_products · stg_warehouses · stg_suppliers · stg_store_locations · stg_inventory · stg_shipments · stg_store_sales

All staging models are views. They cast types, standardise column names, and filter bad rows. No data is duplicated at this layer — they are lightweight lenses over the raw tables.

### Analytics Tables (dbt)

|Model           |Type     |Description                                                          |
|----------------|---------|---------------------------------------------------------------------|
|`dim_products`  |Dimension|Product catalog enriched with supplier name and category             |
|`dim_suppliers` |Dimension|Supplier reference data with contact and location info               |
|`dim_warehouses`|Dimension|Warehouse locations with region and capacity info                    |
|`dim_stores`    |Dimension|Store locations enriched with region metadata from Sheets            |
|`fct_sales`     |Fact     |1.4M sales transactions joined to store and product dimensions       |
|`fct_shipments` |Fact     |350K shipment records with delivery delay calculation and status flag|
|`fct_inventory` |Fact     |Daily inventory snapshots with reorder flag per product/warehouse    |

-----

## dbt Data Models — Design Notes

### Why views for staging, tables for analytics?  

Staging models are views because they are cheap to compute and always reflect the latest raw data without storing redundant copies. Analytics models are materialised as tables because they are large (1M+ rows), joined, and queried frequently — materialising them prevents repeated expensive joins on every dashboard load.

### Delivery delay calculation in `fct_shipments`  

Each shipment record includes scheduled_delivery_date and actual_delivery_date. The staging model calculates delay_days = actual - scheduled. The fact model adds a boolean is_delayed flag (delay_days > 0) and a delay_bucket column (on_time / 1-3 days / 4-7 days / 7+ days) to make reporting simple without requiring dashboard-level logic.



### Foreign key integrity  

Every fact model has dbt relationship tests validating that all product IDs, warehouse IDs, store IDs, and supplier IDs resolve to a valid dimension record. These tests run at the end of every DAG run.

-----

## Infrastructure (Terraform)

All AWS resources are provisioned with Terraform. State is stored remotely in S3 with DynamoDB locking to prevent concurrent modifications.

|Resource                         |Type          |Purpose                        |
|---------------------------------|--------------|-------------------------------|
|`supplychain360_bucket`          |S3 Bucket     |Raw data storage (Bronze layer)|
|`supplychain360_tfstate`         |S3 Bucket     |Terraform remote state         |

-----

## Credentials Management

Database credentials are never hardcoded or committed to version control. All sensitive values are stored in AWS SSM Parameter Store and fetched at runtime.

|SSM Path                     |Value                           |
|-----------------------------|--------------------------------|
|`/supplychain360/db/host`    |Supabase Transaction Pooler host|
|`/supplychain360/db/port`    |6543                            |
|`/supplychain360/db/dbname`  |postgres                        |
|`/supplychain360/db/user`    |postgres.project_id        |
|`/supplychain360/db/password`|Supabase password               |

At runtime, the ingestion code fetches these via boto3.client('ssm').get_parameter() before establishing any database connection. If a parameter is missing, the script fails loudly with a descriptive error before attempting any connection.

-----

## Airflow DAG

The supplychain360_pipeline DAG runs daily at 06:00 UTC.

-----

## Docker Services

|Service  |Image               |Port|Purpose                      |
|---------|--------------------|----|-----------------------------|
|ingestion|Dockerfile.ingestion|—   |Runs Python ingestion scripts|
|airflow  |apache/airflow:2.8.1|8081|Airflow webserver + scheduler|
|postgres |postgres:15         |5432|Airflow metadata database    |

-----

## CI/CD Pipeline (GitHub Actions)

Every push to main automatically triggers the pipeline. Three jobs run in sequence:

1. Environment Setup — Python 3.11 is configured, all dependencies installed from requirements.txt

2. dbt Validation — dbt profiles are constructed from GitHub Secrets, and dbt runs staging models, analytics models, and all tests against the live Snowflake warehouse

3. Docker Build and Push — The ingestion image is built and pushed to Docker Hub with two tags:
- feyiarmstrong/supplychain360-ingestion:latest
- feyiarmstrong/supplychain360-ingestion:<commit-sha>

The commit SHA tag ensures every image is traceable to the exact code version that produced it. Rolling back is as simple as pulling a previous SHA tag.

-----

## Local Setup

### Prerequisites

- Python 3.11+
- Docker Desktop
- Terraform
- AWS CLI with two profiles configured:
  - supplychain360 — source data account (where raw CSV/JSON files live)
  - default — personal AWS account (where Parquet files are written)
- Snowflake account
- Supabase account (PostgreSQL)
- GCP service account with Google Sheets API and Drive API enabled

### 1. Clone the repository
- git clone https://github.com/Feyiarmstrong/supplychain360.git
- cd supplychain360

### 2. Create virtual environment and install dependencies
- python -m venv venv
- source venv/Scripts/activate  # Windows Git Bash
- pip install -r requirements.txt

### 3. Configure AWS profiles
# Source account — where raw data lives
- aws configure --profile supplychain360

# Personal account — where Parquet will be written
- aws configure

### 4. Set up environment variables

- cp .env.example .env


# AWS
- SOURCE_BUCKET
- DESTINATION_BUCKET
- AWS_SOURCE_PROFILE
- AWS_DEST_PROFILE
- SSM_REGION

# Google Sheets

- GOOGLE_SHEET_ID
- GOOGLE_SHEET_NAME
- GCP_CREDENTIALS_PATH=configs/gcp_service_account.json

# Snowflake

- Credentials for snowflake

### 5. Add GCP credentials

Place GCP service account JSON file at configs/gcp_service_account.json.

### 6. Provision AWS infrastructure
- cd terraform
- aws s3 mb s3://supplychain360-tfstate-feyisayo --region us-east-1
- terraform init
- terraform plan
- terraform apply
cd ..

### 7. Run ingestion
- cd ingestion
- python main.py

This reads from all three sources and writes Parquet files to S3. Static sources (products, warehouses, suppliers, store_locations) are only written once — subsequent runs skip them if the file already exists.

### 8. Run ingestion via Docker
- docker compose build
- docker compose run ingestion

### 9. Run dbt models

- cd dbt/supplychain360_dbt
- dbt run --select staging
- dbt run --select analytics
- dbt test

### 10. Start Airflow

cd airflow

docker compose -p supplychain360 up --build -d

Access Airflow UI at http://localhost:8081 

Username: | Password:

-----

## Environment Variables Reference

|Variable              |Description                                          |
|----------------------|-----------------------------------------------------|
|`SOURCE_BUCKET`       |S3 bucket name in the source AWS account             |
|`DESTINATION_BUCKET`  |S3 bucket name in the personal AWS account           |
|`AWS_SOURCE_PROFILE`  |AWS CLI profile name for the source account          |
|`AWS_DEST_PROFILE`    |AWS CLI profile name for the personal account        |
|`SSM_REGION`          |AWS region where SSM parameters are stored           |
|`GOOGLE_SHEET_ID`     |Google Sheet ID from the URL                         |
|`GOOGLE_SHEET_NAME`   |Sheet tab name containing store location data        |
|`GCP_CREDENTIALS_PATH`|Path to GCP service account JSON file                |
|`SNOWFLAKE_ACCOUNT`   |Snowflake account identifier (format: `acct.region`) |
|`SNOWFLAKE_USER`      |Snowflake username                                   |
|`SNOWFLAKE_PASSWORD`  |Snowflake password                                   |
|`SNOWFLAKE_DATABASE`  |Target database               |
|`SNOWFLAKE_WAREHOUSE` |Compute warehouse              |
|`SNOWFLAKE_ROLE`      |Snowflake role with USAGE and CREATE TABLE privileges|

-----

## Key Engineering Decisions

### Why the `SOURCE_CONFIG` dictionary pattern in ingestion scripts?

- Each ingester originally had hardcoded table names, S3 paths, and column definitions scattered across the script. Refactoring to a SOURCE_CONFIG dictionary centralises all source-specific configuration in one place at the top of the file. Adding a new data source means adding a new dictionary entry — no logic changes required. This makes the code significantly easier to maintain and extend.

### Why Supabase Transaction Pooler (port 6543) instead of direct connection (port 5432)?  

- Supabase’s direct connection port (5432) supports only a limited number of concurrent connections — it is designed for persistent, long-lived connections from application servers. The Transaction Pooler (6543) is designed for short-lived, high-frequency connections like ETL scripts. Using the wrong port causes connection exhaustion errors under load. Additionally, the Transaction Pooler requires sslmode=require — without this, the connection is silently rejected.

### Why two AWS boto3 sessions instead of one?  

- The source data lives in a separate AWS account owned by the data provider. A single boto3 session can only authenticate against one account at a time. Two sessions run simultaneously — one authenticated against the supplychain360 profile for reading, and one against the default profile for writing. This cleanly separates the read and write credential scopes and mirrors how cross-account data pipelines work in real production environments.

### Why idempotent ingestion?  

- If the pipeline fails midway and is rerun, a non-idempotent ingester would write duplicate data. All ingesters check whether the target file already exists in S3 before writing. Static sources (products, warehouses, suppliers) are only ever written once — they do not change. Daily sources check for an existing file keyed to today’s date before writing. This means the pipeline can be safely retried at any point without producing duplicates.

### Why Parquet instead of CSV for S3 storage?  

- Parquet is columnar, compressed, and schema-aware. A CSV file with 1.4M rows of sales data might be 400MB uncompressed. The equivalent Parquet file is typically 40–80MB. Snowflake’s COPY INTO command reads Parquet significantly faster than CSV because it can skip columns it doesn’t need. Parquet also preserves data types — there is no ambiguity about whether order_date is a string or a timestamp.

### Why Snowflake auto-suspend at 60 seconds?  

- The warehouse only needs to be running during active query execution. With auto-suspend at 60 seconds, the warehouse shuts down automatically when idle, eliminating charges for sitting unused between pipeline runs. For a daily pipeline, this means the warehouse runs for minutes per day rather than 24 hours. The cost impact is significant — an X-Small warehouse running 24/7 costs approximately $14/month. With auto-suspend, the actual cost is under $1/month.

### Why dbt views for staging and tables for analytics?  

- Staging models are inexpensive transformations that run on top of already-loaded raw data. Materialising them as views means no additional storage cost and no data duplication. Analytics models aggregate and join millions of rows across multiple tables — running these as views would mean recalculating the entire join on every dashboard query. Materialising them as tables means the expensive computation runs once per day during the dbt run, and dashboards query pre-built results instantly.

-----

## Docker Hub
- docker pull feyiarmstrong/supplychain360-ingestion:latest

-----

## Author

Feyisayo Ajiboye  

Data Engineer  

GitHub: [@Feyiarmstrong](https://github.com/Feyiarmstrong)
