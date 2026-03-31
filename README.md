# Spark Sales Pipeline

A Dockerized PySpark ETL pipeline that reads raw retail sales data from CSV, cleans and transforms it, and writes the curated output as Parquet.

## Current status

Version 1 is working.

It currently does the following:
- reads raw sales data from `data/raw/sales.csv`
- removes bad rows and duplicates
- standardizes text fields
- creates `total_amount`
- writes cleaned output to `data/processed/sales_cleaned.parquet`

## Tech stack

- Python
- PySpark
- Docker
- Docker Compose

## Run the project

```bash
docker compose up --build