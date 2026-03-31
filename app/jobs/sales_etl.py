from pyspark.sql import SparkSession
from pyspark.sql.functions import col, initcap, lower


def main():
    spark = (
        SparkSession.builder
        .appName("RetailSalesETL")
        .getOrCreate()
    )

    input_path = "/opt/spark-apps/data/raw/sales.csv"
    output_path = "/opt/spark-apps/data/processed/sales_cleaned.parquet"

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(input_path)
    )

    print("Raw schema:")
    df.printSchema()

    cleaned_df = (
        df.dropDuplicates()
        .dropna(subset=["order_id", "order_date"])
        .withColumn("category", lower(col("category")))
        .withColumn("store_city", initcap(col("store_city")))
        .filter(col("quantity") > 0)
        .filter(col("unit_price") > 0)
        .withColumn("total_amount", col("quantity") * col("unit_price"))
    )

    print("Cleaned data:")
    cleaned_df.show(truncate=False)

    cleaned_df.write.mode("overwrite").parquet(output_path)

    print(f"Cleaned parquet written to: {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()