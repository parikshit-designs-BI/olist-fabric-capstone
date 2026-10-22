# 01_bronze_ingest
# Reads raw Olist CSVs from Files/bronze/olist/ and writes them as-is
# into Delta tables under the `bronze` schema. No cleaning here on purpose.

from pyspark.sql import functions as F

SOURCE_PATH = "Files/bronze/olist/"

# csv file  ->  bronze table name
FILES = {
    "olist_customers_dataset.csv":          "customers",
    "olist_geolocation_dataset.csv":        "geolocation",
    "olist_order_items_dataset.csv":        "order_items",
    "olist_order_payments_dataset.csv":     "order_payments",
    "olist_order_reviews_dataset.csv":      "order_reviews",
    "olist_orders_dataset.csv":             "orders",
    "olist_products_dataset.csv":           "products",
    "olist_sellers_dataset.csv":            "sellers",
    "product_category_name_translation.csv": "category_translation",
}

# make sure the three layer schemas exist
for schema in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

for file_name, table_name in FILES.items():
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")   # bronze keeps everything as string
        .option("multiLine", "true")      # review comments contain line breaks
        .option("escape", '"')
        .load(SOURCE_PATH + file_name)
    )

    # audit columns – the only thing bronze adds
    df = (
        df.withColumn("_source_file", F.lit(file_name))
          .withColumn("_load_ts", F.current_timestamp())
    )

    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(f"bronze.{table_name}"))

    print(f"bronze.{table_name:<22} {df.count():>8} rows")

print("\nBronze load complete.")
