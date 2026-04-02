# Spark Sales Pipeline

A production style Dockerized PySpark ETL project that reads raw retail sales data from CSV, validates and cleans the dataset, standardizes selected fields, derives business metrics, and writes curated output as Parquet.

## Project Overview

This project simulates a simple real world retail data engineering workflow using PySpark inside Docker. It is structured like a small production style ETL job with separated configuration, logging, validation logic, automated tests, containerization, and CI.

## Features

- Reads raw retail sales data from CSV
- Validates required input columns
- Removes duplicate records
- Removes invalid rows
- Standardizes `category` to lowercase
- Standardizes `payment_method` to lowercase
- Standardizes `store_city` to title case
- Creates derived `total_amount`
- Logs raw schema, raw row count, cleaned row count, preview, and output path
- Writes cleaned output in Parquet format
- Uses Docker and Docker Compose for reproducible execution
- Uses environment variables for configurable paths and write mode
- Includes automated tests with `pytest`
- Includes end to end ETL pipeline validation
- Includes GitHub Actions CI workflow

## Tech Stack

- Python 3.11
- PySpark 3.5.1
- Docker
- Docker Compose
- Pytest
- GitHub Actions

## Project Structure

```text
spark_sales_pipeline/
├── .github/
│   └── workflows/
│       └── ci.yml
├── app/
│   ├── __init__.py
│   ├── jobs/
│   │   └── sales_etl.py
│   └── utils/
│       ├── config.py
│       ├── logger.py
│       └── validators.py
├── data/
│   ├── raw/
│   │   └── sales.csv
│   └── processed/
│       └── sales_cleaned.parquet
├── tests/
│   ├── test_config.py
│   ├── test_e2e_pipeline.py
│   ├── test_sales_etl.py
│   └── test_validators.py
├── Dockerfile
├── docker-compose.yml
├── README.md
├── requirements.txt
├── pytest.ini
└── .gitignore