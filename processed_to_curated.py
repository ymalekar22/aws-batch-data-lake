import sys
import logging
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── Logging ───────────────────────────────────────────────────────────────────
import sys
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
logger.addHandler(handler)

# ── Job arguments ─────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'processed_bucket',
    'curated_bucket',
])

# ── Glue context ──────────────────────────────────────────────────────────────
sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ── Performance tuning ────────────────────────────────────────────────────────
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "400")

PROCESSED = f"s3://{args['processed_bucket']}/processed"
CURATED   = f"s3://{args['curated_bucket']}/curated"

# ── Load processed tables ─────────────────────────────────────────────────────
logger.info("Loading processed tables...")

customers = spark.read.parquet(f"{PROCESSED}/customers/")
orders    = spark.read.parquet(f"{PROCESSED}/orders/")
products  = spark.read.parquet(f"{PROCESSED}/products/")

logger.info(f"Customers: {customers.count()}")
logger.info(f"Products:  {products.count()}")

# ── Broadcast small tables ────────────────────────────────────────────────────
# Products is small (360K) — broadcast to avoid shuffle in join
products_broadcast = F.broadcast(
    products.select(
        "i_item_sk", "i_category", "i_product_name",
        "i_current_price", "i_wholesale_cost",
        "i_margin", "i_margin_pct", "is_active"
    )
)

# ── Enrich orders with product info ──────────────────────────────────────────
logger.info("Joining orders with products...")
orders_enriched = orders.join(
    products_broadcast,
    orders.ss_item_sk == products_broadcast.i_item_sk,
    how="left"
).withColumn(
    "ss_gross_profit",
    F.col("ss_net_paid") - (F.col("ss_quantity") * F.col("i_wholesale_cost"))
)

# ── Customer-level aggregations ───────────────────────────────────────────────
logger.info("Building customer aggregations...")
customer_stats = orders_enriched.groupBy("ss_customer_sk").agg(
    F.count("ss_ticket_number").alias("total_orders"),
    F.sum("ss_net_paid").alias("total_spend"),
    F.avg("ss_net_paid").alias("avg_order_value"),
    F.sum("ss_quantity").alias("total_items_bought"),
    F.sum("ss_gross_profit").alias("total_gross_profit"),
    F.countDistinct("i_category").alias("unique_categories"),
    F.max("ss_sold_date_sk").alias("last_purchase_date_sk"),
    F.min("ss_sold_date_sk").alias("first_purchase_date_sk"),
    F.sum(F.when(F.col("is_profitable") == False, 1).otherwise(0)).alias("unprofitable_orders"),
    F.sum(F.when(F.col("has_missing_customer") == True, 1).otherwise(0)).alias("anonymous_orders"),
)

# ── RFM Scoring ───────────────────────────────────────────────────────────────
logger.info("Calculating RFM scores...")

# Frequency score
w_freq = Window.orderBy(F.col("total_orders").asc())
w_monetary = Window.orderBy(F.col("total_spend").asc())
w_recency = Window.orderBy(F.col("last_purchase_date_sk").desc())

customer_rfm = customer_stats \
    .withColumn("f_score", F.ntile(5).over(w_freq)) \
    .withColumn("m_score", F.ntile(5).over(w_monetary)) \
    .withColumn("r_score", F.ntile(5).over(w_recency)) \
    .withColumn("rfm_score",
        F.col("r_score") + F.col("f_score") + F.col("m_score")) \
    .withColumn("rfm_segment",
        F.when(F.col("rfm_score") >= 13, "champions")
         .when(F.col("rfm_score") >= 10, "loyal_customers")
         .when(F.col("rfm_score") >= 7,  "potential_loyalists")
         .when(F.col("rfm_score") >= 5,  "at_risk")
         .otherwise("lost"))

# ── Revenue ranking per customer ──────────────────────────────────────────────
w_revenue_rank = Window.orderBy(F.col("total_spend").desc())
customer_rfm = customer_rfm \
    .withColumn("revenue_rank", F.rank().over(w_revenue_rank)) \
    .withColumn("revenue_percentile", F.percent_rank().over(w_revenue_rank))

# ── Category preferences ──────────────────────────────────────────────────────
logger.info("Building category preferences...")
category_spend = orders_enriched.groupBy("ss_customer_sk", "i_category").agg(
    F.sum("ss_net_paid").alias("category_spend")
)

# Top category per customer
w_cat = Window.partitionBy("ss_customer_sk").orderBy(F.col("category_spend").desc())
top_category = category_spend \
    .withColumn("cat_rank", F.rank().over(w_cat)) \
    .filter(F.col("cat_rank") == 1) \
    .select(
        F.col("ss_customer_sk"),
        F.col("i_category").alias("preferred_category"),
        F.col("category_spend").alias("preferred_category_spend")
    )

# ── Join with customer profile ────────────────────────────────────────────────
logger.info("Building customer 360...")
customer_360 = customers.join(
    customer_rfm,
    customers.c_customer_sk == customer_rfm.ss_customer_sk,
    how="left"
).join(
    top_category,
    customers.c_customer_sk == top_category.ss_customer_sk,
    how="left"
).select(
    # Customer profile
    "c_customer_sk", "c_customer_id", "c_full_name",
    "c_email_address", "c_birth_country", "c_age",
    "c_birth_date", "c_preferred_cust_flag",
    # Order stats
    "total_orders", "total_spend", "avg_order_value",
    "total_items_bought", "total_gross_profit",
    "unique_categories", "unprofitable_orders",
    # RFM
    "r_score", "f_score", "m_score", "rfm_score", "rfm_segment",
    # Rankings
    "revenue_rank", "revenue_percentile",
    # Category preference
    "preferred_category", "preferred_category_spend",
    # Audit
    F.current_timestamp().alias("curated_at")
)

# ── Write curated customer 360 ────────────────────────────────────────────────
DST_360 = f"{CURATED}/customer_360/"
logger.info(f"Writing customer_360 to {DST_360}")

customer_360.write \
    .mode("overwrite") \
    .partitionBy("rfm_segment") \
    .parquet(DST_360)

logger.info(f"customer_360 written successfully")

# ── Product performance metrics ───────────────────────────────────────────────
logger.info("Building product metrics...")
product_metrics = orders_enriched.groupBy(
    "ss_item_sk", "i_product_name", "i_category",
    "i_current_price", "i_wholesale_cost"
).agg(
    F.count("ss_ticket_number").alias("total_sales"),
    F.sum("ss_quantity").alias("total_units_sold"),
    F.sum("ss_net_paid").alias("total_revenue"),
    F.sum("ss_gross_profit").alias("total_profit"),
    F.avg("ss_sales_price").alias("avg_selling_price"),
    F.avg("ss_discount_pct").alias("avg_discount_pct"),
    F.countDistinct("ss_customer_sk").alias("unique_customers"),
).withColumn(
    "profit_margin_pct",
    F.round(F.col("total_profit") / F.col("total_revenue") * 100, 2)
).withColumn(
    "curated_at", F.current_timestamp()
)

# Rank products within category
w_product = Window.partitionBy("i_category").orderBy(F.col("total_revenue").desc())
product_metrics = product_metrics \
    .withColumn("category_rank", F.rank().over(w_product)) \
    .withColumn("is_top_10_in_category", F.col("category_rank") <= 10)

DST_PRODUCTS = f"{CURATED}/product_metrics/"
logger.info(f"Writing product_metrics to {DST_PRODUCTS}")

product_metrics.write \
    .mode("overwrite") \
    .partitionBy("i_category") \
    .parquet(DST_PRODUCTS)

logger.info("product_metrics written successfully")

# ── Commit ────────────────────────────────────────────────────────────────────
job.commit()
logger.info("Job completed successfully")