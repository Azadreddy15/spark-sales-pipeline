# Spark Sales Pipeline

A production style Dockerized PySpark ETL project that reads raw retail sales data from CSV, validates and cleans the dataset, standardizes selected fields, derives business metrics, and writes curated output as Parquet.

## Project overview

This project simulates a simple real world retail data engineering workflow using PySpark inside Docker. It is structured like a small production ETL job with separated configuration, logging, validation logic, tests, and data folders.

## Features

- Reads raw retail sales data from CSV
- Validates required input columns
- Removes duplicate records
- Removes invalid rows
- Standardizes selected text fields
- Creates a derived `total_amount` column
- Writes cleaned output in Parquet format
- Uses Docker and Docker Compose for reproducible execution
- Includes modular utility files for config, logging, and validation
- Includes pytest based validation test coverage

## Tech stack

- Python 3.11
- PySpark 3.5.1
- Docker
- Docker Compose
- pytest

## Project structure

```text
spark_sales_pipeline/
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
│   └── test_validators.py
├── Dockerfile
├── docker-compose.yml
├── README.md
├── requirements.txt
├── pytest.ini
└── .gitignore