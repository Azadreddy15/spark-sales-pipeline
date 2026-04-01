from pyspark.sql import SparkSession
from pyspark.sql.functions import col, initcap, lower

from app.utils.config import Config
from app.utils.logger import get_logger
from app.utils.validators import filter_valid_sales_rows, validate_required_columns


logger = get_logger(__name__)


def main():
    spark = (
        SparkSession.builder
        .appName(Config.APP_NAME)
        .getOrCreate()
    )

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(Config.INPUT_PATH)
    )

    logger.info("Raw schema:")
    df.printSchema()

    raw_count = df.count()
    logger.info(f"Raw row count: {raw_count}")

    required_columns = [
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
    validate_required_columns(df, required_columns)

    cleaned_df = (
        filter_valid_sales_rows(df)
        .withColumn("category", lower(col("category")))
        .withColumn("store_city", initcap(col("store_city")))
        .withColumn("total_amount", col("quantity") * col("unit_price"))
    )

    cleaned_count = cleaned_df.count()
    logger.info(f"Cleaned row count: {cleaned_count}")

    logger.info("Cleaned data preview:")
    cleaned_df.show(truncate=False)

    cleaned_df.write.mode("overwrite").parquet(Config.OUTPUT_PATH)

    logger.info(f"Cleaned parquet written to: {Config.OUTPUT_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()