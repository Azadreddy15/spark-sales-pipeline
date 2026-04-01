import pytest
from pyspark.sql import SparkSession
from app.utils.validators import filter_valid_sales_rows, validate_required_columns


def test_filter_valid_sales_rows():
    spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

    data = [
        (1001, "2026-03-01", 2, 25.5),
        (1002, None, 1, 10.0),
        (1003, "2026-03-02", 0, 20.0),
        (1004, "2026-03-03", 1, -5.0),
    ]
    columns = ["order_id", "order_date", "quantity", "unit_price"]

    df = spark.createDataFrame(data, columns)
    result_df = filter_valid_sales_rows(df)

    assert result_df.count() == 1
    spark.stop()


def test_validate_required_columns_raises_for_missing_columns():
    spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

    df = spark.createDataFrame(
        [(1001, "2026-03-01")],
        ["order_id", "order_date"],
    )

    with pytest.raises(ValueError, match="Missing required columns"):
        validate_required_columns(df, ["order_id", "order_date", "quantity"])

    spark.stop()