from pyspark.sql import SparkSession

from app.utils.validators import filter_valid_sales_rows


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