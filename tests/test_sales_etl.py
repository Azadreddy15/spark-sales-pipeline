import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, initcap, lower

from app.utils.validators import filter_valid_sales_rows


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.master("local[*]").appName("sales-etl-tests").getOrCreate()
    yield spark
    spark.stop()


def test_sales_etl_transformations(spark):
    data = [
        (1001, "2026-03-01", "C001", "P101", "ELECTRONICS", "Wireless Mouse", 2, 25.5, "CARD", "dallas"),
        (1001, "2026-03-01", "C001", "P101", "ELECTRONICS", "Wireless Mouse", 2, 25.5, "CARD", "dallas"),
        (1002, None, "C002", "P102", "HOME", "Bottle", 1, 15.0, "CASH", "FORT WORTH"),
        (1003, "2026-03-02", "C003", "P103", "FITNESS", "Yoga Mat", 3, 20.0, "UPI", "arlington"),
        (1004, "2026-03-03", "C004", "P104", "HOME", "Lamp", 0, 30.0, "CARD", "dallas"),
        (1005, "2026-03-04", "C005", "P105", "GROCERY", "Apples", 1, -5.0, "CASH", "irving"),
    ]

    columns = [
        "order_id",
        "order_date",
        "customer_id",
        "product_id",
        "category",
        "product_name",
        "quantity",
        "unit_price",
        "payment_method",
        "store_city",
    ]

    df = spark.createDataFrame(data, columns)

    cleaned_df = filter_valid_sales_rows(df)

    cleaned_df = (
        cleaned_df.withColumn("category", lower(col("category")))
        .withColumn("payment_method", lower(col("payment_method")))
        .withColumn("store_city", initcap(col("store_city")))
        .withColumn("total_amount", col("quantity") * col("unit_price"))
    )

    rows = cleaned_df.orderBy("order_id").collect()

    assert cleaned_df.count() == 2

    assert rows[0]["order_id"] == 1001
    assert rows[0]["category"] == "electronics"
    assert rows[0]["payment_method"] == "card"
    assert rows[0]["store_city"] == "Dallas"
    assert rows[0]["total_amount"] == 51.0

    assert rows[1]["order_id"] == 1003
    assert rows[1]["category"] == "fitness"
    assert rows[1]["payment_method"] == "upi"
    assert rows[1]["store_city"] == "Arlington"
    assert rows[1]["total_amount"] == 60.0


def test_main_raises_file_not_found_for_missing_input(monkeypatch):
    import app.jobs.sales_etl as sales_etl

    monkeypatch.setattr(
        sales_etl.Config,
        "INPUT_PATH",
        "/tmp/definitely_missing_sales_input_123456.csv",
    )

    with pytest.raises(FileNotFoundError, match="Input file not found"):
        sales_etl.main()