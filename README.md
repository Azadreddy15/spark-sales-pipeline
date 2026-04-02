# Spark Sales Pipeline

A production style Dockerized PySpark ETL project that reads raw retail sales data from CSV, validates and cleans the dataset, standardizes selected fields, derives business metrics, and writes curated output as Parquet.

## Project Overview

This project simulates a simple real world retail data engineering workflow using PySpark inside Docker. It is structured like a small production style ETL job with separated configuration, logging, validation logic, automated tests, containerization, and CI.

## Features

- Reads raw retail sales data from CSV
- Checks that the input file exists before Spark reads it
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
- Includes missing input file handling test
- Includes GitHub Actions CI workflow
- Uses an explicit Spark schema for predictable input data types

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
├── .dockerignore
├── Dockerfile
├── docker-compose.yml
├── README.md
├── requirements.txt
├── pytest.ini
└── .gitignore
```

## Input Data

The raw input file is:

```bash
data/raw/sales.csv
```

The dataset includes these columns:

- `order_id`
- `order_date`
- `customer_id`
- `product_id`
- `category`
- `product_name`
- `quantity`
- `unit_price`
- `payment_method`
- `store_city`

The sample data intentionally includes dirty records such as:

- duplicate rows
- missing `order_id`
- missing `order_date`
- missing `customer_id`
- missing `product_id`
- invalid `quantity` values
- invalid `unit_price` values
- inconsistent city casing
- inconsistent text casing

## ETL Transformations

The pipeline performs the following steps:

### 1. Check input file and read raw CSV data

Before Spark reads the dataset, the ETL job checks whether the configured input file exists. If the file is missing, it raises a `FileNotFoundError`.

The raw sales CSV is then read into a Spark DataFrame using an explicit schema for predictable input types.

### 2. Validate required columns

Before transformation, the pipeline verifies that the input dataset contains all required columns.

Required columns:

- `order_id`
- `order_date`
- `customer_id`
- `product_id`
- `category`
- `product_name`
- `quantity`
- `unit_price`
- `payment_method`
- `store_city`

### 3. Remove invalid rows

The pipeline removes rows that:

- are duplicates
- have missing `order_id`
- have missing `order_date`
- have missing `customer_id`
- have missing `product_id`
- have `quantity <= 0`
- have `unit_price <= 0`

### 4. Standardize text fields

The pipeline standardizes these fields:

- `category` to lowercase
- `payment_method` to lowercase
- `store_city` to title case

### 5. Derive business metric

The pipeline creates:

```python
total_amount = quantity * unit_price
```

### 6. Write curated output

The cleaned dataset is written as Parquet.

## Configuration

The project uses environment variables through `app/utils/config.py`.

Supported configuration values:

- `INPUT_PATH`
- `OUTPUT_PATH`
- `APP_NAME`
- `WRITE_MODE`

Default values:

```python
INPUT_PATH = "/opt/spark-apps/data/raw/sales.csv"
OUTPUT_PATH = "/opt/spark-apps/data/processed/sales_cleaned.parquet"
APP_NAME = "RetailSalesETL"
WRITE_MODE = "overwrite"
```

## Docker Setup

### Dockerfile

The Dockerfile currently:

- uses `python:3.11-slim`
- installs `default-jdk` and `procps`
- sets `JAVA_HOME=/usr/lib/jvm/default-java`
- sets `PYSPARK_PYTHON=python3`
- sets the working directory to `/opt/spark-apps`
- installs dependencies from `requirements.txt`
- copies the full project into the container
- uses `.dockerignore` to keep unnecessary files out of the Docker build context

### Docker Compose

The `docker-compose.yml` file defines the `spark-job` service and passes runtime configuration using environment variables.

Configured environment variables:

- `INPUT_PATH=/opt/spark-apps/data/raw/sales.csv`
- `OUTPUT_PATH=/opt/spark-apps/data/processed/sales_cleaned.parquet`
- `APP_NAME=RetailSalesETL`
- `WRITE_MODE=overwrite`

The container runs:

```bash
python -m app.jobs.sales_etl
```

## Run the Pipeline

From the project root, run:

```bash
docker compose up --build
```

This will:

- build the Docker image
- run the Spark ETL job
- process the raw CSV
- write cleaned Parquet output

## Run Tests

Run the full test suite:

```bash
docker compose run --rm --build spark-job pytest
```

Run only the end to end pipeline test:

```bash
docker compose run --rm --build spark-job pytest tests/test_e2e_pipeline.py
```

## Output

The cleaned Parquet output is written to:

```bash
data/processed/sales_cleaned.parquet
```

The final output contains cleaned, validated, and standardized records along with the derived `total_amount` column.

## Logging

The project uses centralized logging through `app/utils/logger.py`.

The ETL job currently logs:

- raw schema
- raw row count
- cleaned row count
- cleaned preview
- output path

This makes the pipeline easier to debug and monitor.

## Test Coverage

The project currently includes:

- **Config tests** for default values and environment variable overrides
- **Validator tests** for required column validation and invalid row filtering
- **Transformation tests** for duplicate removal, text standardization, `total_amount` calculation, and missing input file handling
- **End to end ETL test** that runs the full pipeline and validates the Parquet output schema and cleaned values

Latest verified local test result:

```text
7 passed
```

## CI Integration

GitHub Actions CI is configured in:

```bash
.github/workflows/ci.yml
```

The workflow runs automatically on:

- `push` to `main`
- `pull_request` to `main`

CI verifies that the test suite passes successfully on GitHub.

## Current Status

This project currently supports:

- Dockerized execution
- modular PySpark ETL code
- explicit schema based CSV ingestion
- input file existence validation
- required column validation
- invalid row filtering
- text normalization
- derived metrics
- Parquet output
- automated testing
- end to end pipeline validation
- GitHub Actions CI

This makes it a strong production style beginner to intermediate data engineering portfolio project.

## Example Commands

Run the pipeline:

```bash
docker compose up --build
```

Run all tests:

```bash
docker compose run --rm --build spark-job pytest
```

Push latest changes:

```bash
git add .
git commit -m "Update README to reflect latest ETL state"
git push origin main
```

## Why This Project Matters

This project demonstrates practical data engineering skills used in real workflows:

- ETL pipeline design
- data cleaning and validation
- Spark based data processing
- containerized development
- automated testing
- CI integration
- maintainable project structure

It is intentionally simple enough to understand clearly while still reflecting production style practices.

## Author

**Azad Reddy**

GitHub: `Azadreddy15`