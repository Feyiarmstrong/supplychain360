# Production-Grade Unified Supply Chain Data Platform

## Overview
SupplyChain360 is a production-grade unified supply chain data platform that integrates multiple heterogeneous data sources into a single, reliable and scalable data warehouse. The platform automates the entire data lifecycle from raw ingestion to analytics-ready models using modern data engineering tools and cloud infrastructure.

Supply chain companies often store data in multiple systems:
 • transactional databases
 • external APIs
 • spreadsheets
 • cloud storage

This fragmentation makes it difficult to produce reliable analytics.

SupplyChain360 solves this by building a centralized data platform that:
 • ingests data from multiple sources
 • standardizes and transforms datasets
 • loads clean data into a warehouse
 • supports analytics and reporting


## Tech Stack

| Component | Technology |
|---|---|
| Cloud Infrastructure | AWS (S3, SSM, DynamoDB, ECR) |
| Data Warehouse | Snowflake |
| Transformation | dbt Core |
| Orchestration | Apache Airflow 2.8.1 |
| Containerization | Docker |
| IaC | Terraform |
| CI/CD | GitHub Actions |
| Language | Python 3.11 |
| Storage Format | Apache Parquet |


## Project Structure

supplychain360/

├── .github/

│   └── workflows/

│       └── ci_cd.yml

├── airflow/

│   ├── Dockerfile

│   ├── docker-compose.yml

│   ├── requirements.txt

│   └── dags/

│       └── supplychain360_dag.py

├── configs/

├── dbt/

│   └── supplychain360_dbt/

│       ├── models/

│       │   ├── staging/

│       │   └── analytics/

│       └── dbt_project.yml

├── docker/

│   ├── Dockerfile.ingestion

│   └── requirements.txt

├── ingestion/

│   ├── main.py

│   ├── s3_ingester.py

│   ├── postgres_ingester.py

│   ├── gsheets_ingester.py

│   ├── writer.py

│   └── utils.py

├── terraform/

│   ├── main.tf

│   ├── variables.tf

│   ├── s3.tf

│   ├── dynamodb.tf

│   └── outputs.tf

├── .env.example

├── .gitignore

├── docker-compose.yml

└── requirements.txt

---

## Data Sources

| Source | Type | Format | Rows |
|---|---|---|---|
| Products | Static | CSV | 63 |
| Warehouses | Static | CSV | 10 |
| Suppliers | Static | CSV | 12 |
| Store Locations | Static | Google Sheets | 800 |
| Inventory | Daily | CSV | 56,100 |
| Shipments | Daily | JSON | 350,000 |
| Store Sales | Daily | PostgreSQL | 1,400,000 |



## Data Sources architecture

   │

   │
   
   ├── PostgreSQL Database
   
   ├── Google Sheets API
   
   ├── External API
   
   └── CSV files in S3
   
        │
        ▼

Python Ingestion Layer
        │
        ▼

Amazon S3 Data Lake
        │
        ▼

Apache Airflow Orchestration
        │
        ▼

dbt Transformations
        │
        ▼

Analytics Warehouse Tables


---

## Data Models

### Staging Layer
| Model | Description |
|---|---|
| stg_products | Cleaned product catalog |
| stg_warehouses | Standardized warehouse data |
| stg_suppliers | Normalized supplier info |
| stg_inventory | Daily inventory snapshots |
| stg_shipments | Shipment records with delay calc |
| stg_store_locations | Store location data |
| stg_store_sales | Daily sales transactions |

### Analytics Layer
| Model | Type | Description |
|---|---|---|
| dim_products | Dimension | Product catalog with supplier info |
| dim_suppliers | Dimension | Supplier details |
| dim_warehouses | Dimension | Warehouse locations |
| dim_stores | Dimension | Store locations and regions |
| fct_sales | Fact | 1.4M sales transactions |
| fct_shipments | Fact | 350K shipment records |
| fct_inventory | Fact | Daily inventory with reorder flags |

---

## Infrastructure (Terraform)

Resources provisioned in AWS:

- supplychain360-raw-feyisayo — Raw data S3 bucket
- supplychain360-tfstate-feyisayo — Terraform remote state bucket
- supplychain360-tf-locks — DynamoDB table for state locking

---

## CI/CD Pipeline

Every push to main automatically:

1.  Sets up Python environment
2.  Installs dbt
3.  Creates dbt profiles from GitHub secrets
4.  Runs dbt tests against Snowflake
5.  Logs into Docker Hub
6.  Builds and pushes Docker image with two tags:
   - feyiarmstrong/supplychain360-ingestion:latest
   - feyiarmstrong/supplychain360-ingestion:<commit-sha>

---

## Local Setup

### Prerequisites

- Python 3.11+
- Docker Desktop
- Terraform
- AWS CLI configured with two profiles:
  - supplychain360 — source data account
  - default — personal AWS account
- Snowflake account
- GCP service account with Google Sheets API enabled

### Environment Variables

Copy .env.example to .env and fill in your values:

### Run Ingestion Locally

cd ingestion

python main.py

### Run With Docker

docker compose build

docker compose run ingestion

### Run dbt Models

cd dbt/supplychain360_dbt

dbt run --select staging

dbt run --select analytics

dbt test

### Start Airflow

cd airflow

docker compose -p supplychain360 up --build -d

Access Airflow UI at http://localhost:8081

Username:

Password:


### Airflow DAG

The supplychain360_pipeline DAG runs daily at 6:00 AM UTC:

ingest_data → dbt_staging → dbt_analytics → dbt_test

## Task Description

ingest_data -> Pulls data from all sources into S3 as Parquet

dbt_staging -> Runs 7 staging models in Snowflake

dbt_analytics -> Runs 4 dimension + 3 fact models

dbt_test -> Validates data quality across all models

---

## Key Design Decisions

### Idempotent Ingestion

All ingesters check if data already exists before writing.

Static sources are only ingested once.

Daily sources check for existing files before writing new ones.

### Dual AWS Accounts

Source data lives in a separate AWS account.

Two boto3 sessions run simultaneously, one for reading from the source account and one for writing to the personal account.

### Data Lakehouse Pattern

All raw data lands in S3 as Parquet (Bronze layer) before loading into Snowflake (Silver/Gold layers).

This ensures replayability and cost efficiency.

### Remote Terraform State

Terraform state is stored in S3 with DynamoDB locking to prevent concurrent modifications.

### Credentials Management

Database credentials are stored in AWS SSM Parameter Store and fetched at runtime.

No credentials are hardcoded or committed to version control.

## Docker Hubdocker pull

feyiarmstrong/supplychain360-ingestion:latest

---

## Author

Feyisayo Ajiboye

Data Engineer

GitHub: @Feyiarmstrong
