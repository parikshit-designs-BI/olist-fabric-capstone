# 03_gold_build
# Builds the star schema in the `gold` schema from silver tables.
#   dim_date                  generated calendar
#   dim_product, dim_seller   SCD Type 1 (overwrite, hash surrogate key)
#   dim_customer              SCD Type 2 (history kept, sequential surrogate key)
#   fact_order_items, fact_payments, fact_reviews

from datetime import datetime
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

FAR_FUTURE = "9999-12-31 00:00:00"
RUN_TS     = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def silver(name):
    return spark.table(f"silver.{name}")

def write_gold(df, name):
    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(f"gold.{name}"))
    print(f"gold.{name:<18} {df.count():>8} rows")

date_key = lambda c: F.date_format(F.col(c), "yyyyMMdd").cast("int")

# ================================================================ dim_date
dim_date = (
    spark.sql("SELECT explode(sequence(to_date('2016-01-01'), to_date('2019-12-31'), interval 1 day)) AS date")
    .select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        F.col("date"),
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.date_format("date", "yyyy-MM").alias("year_month"),
        F.dayofmonth("date").alias("day"),
        F.dayofweek("date").alias("day_of_week"),
        F.date_format("date", "EEEE").alias("day_name"),
        F.weekofyear("date").alias("week_of_year"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
    )
)
write_gold(dim_date, "dim_date")

# ================================================================ dim_product  (Type 1)
dim_product = (
    silver("products")
    .select(
        F.xxhash64("product_id").alias("product_key"),
        "product_id", "category",
        "weight_g", "length_cm", "height_cm", "width_cm", "photos_qty",
        (F.col("length_cm") * F.col("height_cm") * F.col("width_cm")).alias("volume_cm3"),
    )
)
write_gold(dim_product, "dim_product")

# ================================================================ dim_seller  (Type 1)
geo = silver("geolocation").select("zip_code_prefix", "lat", "lng")
dim_seller = (
    silver("sellers")
    .join(geo, "zip_code_prefix", "left")
    .select(
        F.xxhash64("seller_id").alias("seller_key"),
        "seller_id", "zip_code_prefix", "city", "state", "lat", "lng",
    )
)
write_gold(dim_seller, "dim_seller")

# ================================================================ dim_customer  (Type 2)
TRACKED = ["zip_code_prefix", "city", "state"]          # changes here create a new version
DIM_COLS = ["customer_key", "customer_id", "customer_unique_id",
            *TRACKED, "row_hash", "valid_from", "valid_to", "is_current"]

src = (
    silver("customers")
    .select("customer_id", "customer_unique_id", *TRACKED)
    .withColumn("row_hash", F.sha2(F.concat_ws("||",
        *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in TRACKED]), 256))
)

if not spark.catalog.tableExists("gold.dim_customer"):
    # ---- initial load: every row is version 1, valid since forever
    initial = (
        src.withColumn("customer_key", F.row_number().over(Window.orderBy("customer_id")))
           .withColumn("valid_from", F.to_timestamp(F.lit("1900-01-01 00:00:00")))
           .withColumn("valid_to",   F.to_timestamp(F.lit(FAR_FUTURE)))
           .withColumn("is_current", F.lit(True))
           .select(DIM_COLS)
    )
    write_gold(initial, "dim_customer")
    print("dim_customer: initial load")

else:
    # ---- incremental: expire changed rows, insert new versions
    tgt = DeltaTable.forName(spark, "gold.dim_customer")
    cur = tgt.toDF().filter("is_current").select("customer_id", F.col("row_hash").alias("cur_hash"))

    changed = (src.join(cur, "customer_id")
                  .filter(F.col("row_hash") != F.col("cur_hash"))
                  .drop("cur_hash"))
    new     = src.join(cur, "customer_id", "left_anti")
    n_changed, n_new = changed.count(), new.count()

    # 1) close the old version of every changed customer
    (tgt.alias("t")
        .merge(changed.select("customer_id").alias("s"),
               "t.customer_id = s.customer_id AND t.is_current = true")
        .whenMatchedUpdate(set={
            "valid_to":   f"to_timestamp('{RUN_TS}')",
            "is_current": "false",
        })
        .execute())

    # 2) insert new versions (changed) + brand-new customers
    max_key = tgt.toDF().agg(F.max("customer_key")).first()[0] or 0
    inserts = (
        changed.unionByName(new)
        .withColumn("customer_key", F.lit(max_key) + F.row_number().over(Window.orderBy("customer_id")))
        .withColumn("valid_from", F.to_timestamp(F.lit(RUN_TS)))
        .withColumn("valid_to",   F.to_timestamp(F.lit(FAR_FUTURE)))
        .withColumn("is_current", F.lit(True))
        .select(DIM_COLS)
    )
    inserts.write.format("delta").mode("append").saveAsTable("gold.dim_customer")
    print(f"dim_customer: SCD2 run -> {n_changed} changed, {n_new} new, "
          f"{spark.table('gold.dim_customer').count()} total rows")

# ================================================================ facts
orders = silver("orders")

# customer version that was valid at purchase time (point-in-time join)
cust_ver = spark.table("gold.dim_customer").select("customer_key", "customer_id", "valid_from", "valid_to")
orders_k = (
    orders.alias("o")
    .join(cust_ver.alias("c"),
          (F.col("o.customer_id") == F.col("c.customer_id")) &
          (F.col("o.purchase_ts") >= F.col("c.valid_from")) &
          (F.col("o.purchase_ts") <  F.col("c.valid_to")), "left")
    .select("o.*", "c.customer_key")
)

# ---- fact_order_items  (grain: one row per order line)
fact_order_items = (
    silver("order_items").alias("i")
    .join(orders_k.alias("o"), "order_id")
    .join(spark.table("gold.dim_product").select("product_id", "product_key"), "product_id", "left")
    .join(spark.table("gold.dim_seller").select("seller_id", "seller_key"), "seller_id", "left")
    .select(
        "order_id", "order_item_id",
        "customer_key", "product_key", "seller_key",
        date_key("purchase_ts").alias("purchase_date_key"),
        date_key("delivered_customer_ts").alias("delivered_date_key"),
        date_key("estimated_delivery_ts").alias("estimated_date_key"),
        "order_status", "purchase_ts",
        "price", "freight_value",
        (F.col("price") + F.col("freight_value")).alias("total_value"),
        F.datediff("delivered_customer_ts", "purchase_ts").alias("delivery_days"),
        F.datediff("delivered_customer_ts", "estimated_delivery_ts").alias("days_vs_estimate"),
        (F.col("delivered_customer_ts") > F.col("estimated_delivery_ts")).alias("is_late"),
    )
)
write_gold(fact_order_items, "fact_order_items")

# ---- fact_payments  (grain: one row per payment)
fact_payments = (
    silver("order_payments")
    .join(orders_k.select("order_id", "customer_key", "purchase_ts"), "order_id")
    .select(
        "order_id", "payment_sequential", "customer_key",
        date_key("purchase_ts").alias("purchase_date_key"),
        "payment_type", "installments", "payment_value",
    )
)
write_gold(fact_payments, "fact_payments")

# ---- fact_reviews  (grain: one row per review)
fact_reviews = (
    silver("order_reviews")
    .join(orders_k.select("order_id", "customer_key", "purchase_ts"), "order_id")
    .select(
        "review_id", "order_id", "customer_key",
        date_key("purchase_ts").alias("purchase_date_key"),
        date_key("review_created_ts").alias("review_date_key"),
        "review_score",
        F.col("comment_message").isNotNull().alias("has_comment"),
        F.datediff("review_answered_ts", "review_created_ts").alias("answer_days"),
    )
)
write_gold(fact_reviews, "fact_reviews")

# ================================================================ sanity check
orphans = fact_order_items.filter("customer_key IS NULL").count()
print(f"\nfact_order_items rows without customer_key: {orphans}  (expect 0)")
print("Gold build complete.")
