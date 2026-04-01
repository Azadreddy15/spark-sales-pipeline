from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def validate_required_columns(df: DataFrame, required_columns: list[str]) -> None:
    missing_columns = [column for column in required_columns if column not in df.columns]

    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")


def filter_valid_sales_rows(df: DataFrame) -> DataFrame:
    return (
        df.dropDuplicates()
        .dropna(subset=["order_id", "order_date"])
        .filter(col("quantity") > 0)
        .filter(col("unit_price") > 0)
    )