import sys
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
import logging

# ── Logging setup ─────────────────────────────────────────────────────────────
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── Job arguments ─────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'source_database',
    'source_table',
    'target_bucket',
    'target_prefix',
    'year',
    'month'
])

# ── Glue context ──────────────────────────────────────────────────────────────
sc        = SparkContext()
glueContext = GlueContext(sc)
spark     = glueContext.spark_session
job       = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ── Performance tuning ────────────────────────────────────────────────────────
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# ── Column mapping ────────────────────────────────────────────────────────────
COLUMN_MAPPING = {
    "col0":  "c_customer_sk",
    "col1":  "c_customer_id",
    "col2":  "c_current_cdemo_sk",
    "col3":  "c_current_hdemo_sk",
    "col4":  "c_current_addr_sk",
    "col5":  "c_first_shipto_date_sk",
    "col6":  "c_first_sales_date_sk",
    "col7":  "c_salutation",
    "col8":  "c_first_name",
    "col9":  "c_last_name",
    "col10": "c_preferred_cust_flag",
    "col11": "c_birth_day",
    "col12": "c_birth_month",
    "col13": "c_birth_year",
    "col14": "c_birth_country",
    "col15": "c_login",
    "col16": "c_email_address",
    "col17": "c_last_review_date_sk",
}

# ── Read raw data ─────────────────────────────────────────────────────────────
logger.info(f"Reading raw data from {args['source_database']}.{args['source_table']}")

df = spark.sql(f"""
    SELECT * FROM {args['source_database']}.{args['source_table']}
    WHERE year = '{args['year']}' AND month = '{args['month']}'
""")

raw_count = df.count()
logger.info(f"Raw record count: {raw_count}")

# ── Apply column mapping ──────────────────────────────────────────────────────
for old_name, new_name in COLUMN_MAPPING.items():
    df = df.withColumnRenamed(old_name, new_name)

# ── Transformations ───────────────────────────────────────────────────────────
def transform_customers(df):
    return df \
        .withColumn("c_full_name",
            F.concat(F.col("c_first_name"), F.lit(" "), F.col("c_last_name"))) \
        .withColumn("c_birth_date",
            F.to_date(
                F.concat_ws("-", F.col("c_birth_year"), F.col("c_birth_month"), F.col("c_birth_day")),
                "yyyy-M-d"
            )) \
        .withColumn("c_age",
            F.floor(F.datediff(F.current_date(), F.col("c_birth_date")) / 365)) \
        .withColumn("c_email_address",
            F.lower(F.trim(F.col("c_email_address")))) \
        .withColumn("c_birth_country",
            F.initcap(F.col("c_birth_country"))) \
        .withColumn("has_missing_email",
            F.col("c_email_address").isNull() | (F.col("c_email_address") == "")) \
        .withColumn("processed_at", F.current_timestamp()) \
        .drop("c_login")

df_processed = transform_customers(df)

# ── Data quality checks ───────────────────────────────────────────────────────
processed_count = df_processed.count()
logger.info(f"Processed record count: {processed_count}")

# Fail if record count drops more than 5%
if processed_count < raw_count * 0.95:
    raise ValueError(f"Data loss detected! Raw: {raw_count}, Processed: {processed_count}")

# Fail if no records
assert processed_count > 0, "Empty dataframe — transformation produced no records"

logger.info("Data quality checks passed")

# ── Write to processed layer ──────────────────────────────────────────────────
DST = f"s3://{args['target_bucket']}/{args['target_prefix']}"
logger.info(f"Writing to {DST}")

df_processed.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet(DST)

logger.info(f"Successfully written {processed_count} records to {DST}")

# ── Commit job bookmark ───────────────────────────────────────────────────────
job.commit()