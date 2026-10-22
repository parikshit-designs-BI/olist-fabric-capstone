# 02_silver_clean
# Reads bronze tables, applies typing / trimming / dedup / standardisation,
# writes one silver table per source table, and logs data-quality checks.

from pyspark.sql import functions as F

def bronze(name):
    return spark.table(f"bronze.{name}")

def write_silver(df, name):
    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(f"silver.{name}"))
    print(f"silver.{name:<20} {df.count():>8} rows")

ts = lambda c: F.to_timestamp(F.col(c), "yyyy-MM-dd HH:mm:ss")

# ---------------------------------------------------------------- customers
customers = (
    bronze("customers")
    .dropDuplicates(["customer_id"])
    .select(
        F.trim("customer_id").alias("customer_id"),
        F.trim("customer_unique_id").alias("customer_unique_id"),
        F.col("customer_zip_code_prefix").cast("int").alias("zip_code_prefix"),
        F.initcap(F.trim("customer_city")).alias("city"),
        F.upper(F.trim("customer_state")).alias("state"),
        "_load_ts",
    )
)
write_silver(customers, "customers")

# ---------------------------------------------------------------- sellers
sellers = (
    bronze("sellers")
    .dropDuplicates(["seller_id"])
    .select(
        F.trim("seller_id").alias("seller_id"),
        F.col("seller_zip_code_prefix").cast("int").alias("zip_code_prefix"),
        F.initcap(F.trim("seller_city")).alias("city"),
        F.upper(F.trim("seller_state")).alias("state"),
        "_load_ts",
    )
)
write_silver(sellers, "sellers")

# ---------------------------------------------------------------- products
# source has typo columns (product_name_lenght) and Portuguese category names
translation = bronze("category_translation").select(
    F.col("product_category_name").alias("cat_pt"),
    F.col("product_category_name_english").alias("cat_en"),
)
products = (
    bronze("products")
    .dropDuplicates(["product_id"])
    .join(translation, F.col("product_category_name") == F.col("cat_pt"), "left")
    .select(
        F.trim("product_id").alias("product_id"),
        F.coalesce("cat_en", "product_category_name", F.lit("unknown")).alias("category"),
        F.col("product_name_lenght").cast("int").alias("name_length"),
        F.col("product_description_lenght").cast("int").alias("description_length"),
        F.col("product_photos_qty").cast("int").alias("photos_qty"),
        F.col("product_weight_g").cast("int").alias("weight_g"),
        F.col("product_length_cm").cast("int").alias("length_cm"),
        F.col("product_height_cm").cast("int").alias("height_cm"),
        F.col("product_width_cm").cast("int").alias("width_cm"),
        "_load_ts",
    )
)
write_silver(products, "products")

# ---------------------------------------------------------------- orders
orders = (
    bronze("orders")
    .dropDuplicates(["order_id"])
    .select(
        F.trim("order_id").alias("order_id"),
        F.trim("customer_id").alias("customer_id"),
        F.lower(F.trim("order_status")).alias("order_status"),
        ts("order_purchase_timestamp").alias("purchase_ts"),
        ts("order_approved_at").alias("approved_ts"),
        ts("order_delivered_carrier_date").alias("delivered_carrier_ts"),
        ts("order_delivered_customer_date").alias("delivered_customer_ts"),
        ts("order_estimated_delivery_date").alias("estimated_delivery_ts"),
        "_load_ts",
    )
)
write_silver(orders, "orders")

# ---------------------------------------------------------------- order_items
order_items = (
    bronze("order_items")
    .dropDuplicates(["order_id", "order_item_id"])
    .select(
        F.trim("order_id").alias("order_id"),
        F.col("order_item_id").cast("int").alias("order_item_id"),
        F.trim("product_id").alias("product_id"),
        F.trim("seller_id").alias("seller_id"),
        ts("shipping_limit_date").alias("shipping_limit_ts"),
        F.col("price").cast("decimal(12,2)").alias("price"),
        F.col("freight_value").cast("decimal(12,2)").alias("freight_value"),
        "_load_ts",
    )
)
write_silver(order_items, "order_items")

# ---------------------------------------------------------------- payments
payments = (
    bronze("order_payments")
    .dropDuplicates(["order_id", "payment_sequential"])
    .select(
        F.trim("order_id").alias("order_id"),
        F.col("payment_sequential").cast("int").alias("payment_sequential"),
        F.lower(F.trim("payment_type")).alias("payment_type"),
        F.col("payment_installments").cast("int").alias("installments"),
        F.col("payment_value").cast("decimal(12,2)").alias("payment_value"),
        "_load_ts",
    )
)
write_silver(payments, "order_payments")

# ---------------------------------------------------------------- reviews
# same review_id appears for multiple orders -> key is (review_id, order_id)
reviews = (
    bronze("order_reviews")
    .dropDuplicates(["review_id", "order_id"])
    .select(
        F.trim("review_id").alias("review_id"),
        F.trim("order_id").alias("order_id"),
        F.col("review_score").cast("int").alias("review_score"),
        F.col("review_comment_title").alias("comment_title"),
        F.col("review_comment_message").alias("comment_message"),
        ts("review_creation_date").alias("review_created_ts"),
        ts("review_answer_timestamp").alias("review_answered_ts"),
        "_load_ts",
    )
)
write_silver(reviews, "order_reviews")

# ---------------------------------------------------------------- geolocation
# 1M rows but only ~19k zip prefixes -> collapse to one lat/lng per prefix
geolocation = (
    bronze("geolocation")
    .select(
        F.col("geolocation_zip_code_prefix").cast("int").alias("zip_code_prefix"),
        F.col("geolocation_lat").cast("double").alias("lat"),
        F.col("geolocation_lng").cast("double").alias("lng"),
        F.initcap(F.trim("geolocation_city")).alias("city"),
        F.upper(F.trim("geolocation_state")).alias("state"),
    )
    .groupBy("zip_code_prefix")
    .agg(
        F.avg("lat").alias("lat"),
        F.avg("lng").alias("lng"),
        F.first("city").alias("city"),
        F.first("state").alias("state"),
    )
    .withColumn("_load_ts", F.current_timestamp())
)
write_silver(geolocation, "geolocation")

# ---------------------------------------------------------------- data quality log
# each check returns a failing-row count; 0 = pass
checks = {
    "orders.order_id_null":          orders.filter("order_id IS NULL").count(),
    "orders.purchase_ts_null":       orders.filter("purchase_ts IS NULL").count(),
    "orders.bad_status":             orders.filter(~F.col("order_status").isin(
                                        "delivered","shipped","canceled","invoiced","processing",
                                        "unavailable","approved","created")).count(),
    "order_items.orphan_order":      order_items.join(orders, "order_id", "left_anti").count(),
    "order_items.orphan_product":    order_items.join(products, "product_id", "left_anti").count(),
    "order_items.negative_price":    order_items.filter("price < 0").count(),
    "reviews.score_out_of_range":    reviews.filter("review_score NOT BETWEEN 1 AND 5").count(),
    "customers.zip_null":            customers.filter("zip_code_prefix IS NULL").count(),
}

dq = spark.createDataFrame(
    [(k, v, "PASS" if v == 0 else "FAIL") for k, v in checks.items()],
    ["check_name", "failing_rows", "status"],
).withColumn("run_ts", F.current_timestamp())

dq.write.format("delta").mode("append").saveAsTable("silver.dq_log")

print("\nData quality results:")
dq.show(truncate=False)

failed = [k for k, v in checks.items() if v > 0]
if failed:
    print(f"WARNING: {len(failed)} check(s) failed -> {failed}")
else:
    print("All checks passed.")
