from pyspark.sql import SparkSession
from app.jobs.sales_etl import main
from app.utils.config import Config


def test_sales_etl_end_to_end(tmp_path, monkeypatch):
    input_file = tmp_path / "sales.csv"
    output_dir = tmp_path / "sales_cleaned.parquet"

    input_file.write_text(
        """order_id,order_date,customer_id,product_id,category,product_name,quantity,unit_price,payment_method,store_city
1001,2026-03-01,C001,P101,ELECTRONICS,Wireless Mouse,2,25.5,CARD,dallas
1001,2026-03-01,C001,P101,ELECTRONICS,Wireless Mouse,2,25.5,CARD,dallas
1002,,C002,P102,HOME,Bottle,1,15.0,CASH,FORT WORTH
1003,2026-03-02,C003,P103,FITNESS,Yoga Mat,3,20.0,UPI,arlington
1004,2026-03-03,C004,P104,HOME,Lamp,0,30.0,CARD,dallas
1005,2026-03-04,C005,P105,GROCERY,Apples,1,-5.0,CASH,irving
""",
        encoding="utf-8",
    )

    monkeypatch.setattr(Config, "INPUT_PATH", str(input_file))
    monkeypatch.setattr(Config, "OUTPUT_PATH", str(output_dir))
    monkeypatch.setattr(Config, "APP_NAME", "RetailSalesETLTest")
    monkeypatch.setattr(Config, "WRITE_MODE", "overwrite")

    main()

    spark = SparkSession.builder.master("local[*]").appName("test-read-output").getOrCreate()

    result_df = spark.read.parquet(str(output_dir))
    rows = result_df.orderBy("order_id").collect()

    assert output_dir.exists()
    assert result_df.count() == 2

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

    spark.stop()