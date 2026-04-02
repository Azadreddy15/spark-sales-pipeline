from pyspark.sql import SparkSession
from pyspark.sql.functions import col, initcap, lower

from app.utils.validators import filter_valid_sales_rows


def test_sales_etl_core_transformations():
    spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

    data = [
        (1001, "2026-03-01", "C001", "P101", "ELECTRONICS", "Wireless Mouse", 2, 25.5, "CARD", "dallas"),
        (1001, "2026-03-01", "C001", "P101", "ELECTRONICS", "Wireless Mouse", 2, 25.5, "CARD", "dallas"),
        (1002, None, "C002", "P102", "HOME", "Bottle", 1, 15.0, "CASH", "FORT WORTH"),
        (1003, "2026-03-02", "C003", "P103", "FITNESS", "Yoga Mat", 3, 20.0, "UPI", "arlington"),
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

    cleaned_df = (
        filter_valid_sales_rows(df)
        .withColumn("category", lower(col("category")))
        .withColumn("payment_method", lower(col("payment_method")))
        .withColumn("store_city", initcap(col("store_city")))
        .withColumn("total_amount", col("quantity") * col("unit_price"))
    )

    rows = cleaned_df.orderBy("order_id").collect()

    assert len(rows) == 2
    assert rows[0]["category"] == "electronics"
    assert rows[0]["payment_method"] == "card"
    assert rows[0]["store_city"] == "Dallas"
    assert rows[0]["total_amount"] == 51.0
    assert rows[1]["category"] == "fitness"
    assert rows[1]["payment_method"] == "upi"
    assert rows[1]["store_city"] == "Arlington"
    assert rows[1]["total_amount"] == 60.0

    spark.stop()