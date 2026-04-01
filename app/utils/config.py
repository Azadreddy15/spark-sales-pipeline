import os


class Config:
    INPUT_PATH = os.getenv("INPUT_PATH", "/opt/spark-apps/data/raw/sales.csv")
    OUTPUT_PATH = os.getenv("OUTPUT_PATH", "/opt/spark-apps/data/processed/sales_cleaned.parquet")
    APP_NAME = os.getenv("APP_NAME", "RetailSalesETL")