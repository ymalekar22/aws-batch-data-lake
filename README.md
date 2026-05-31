# Project 1: Batch Data Lake on AWS (Three-Layer Architecture)

A three-layer batch data lake pipeline built on AWS Glue and S3. Raw customer data from RDS MySQL is ingested via CDC, transformed through a processed layer, and aggregated into a curated analytics layer with RFM customer segmentation.

---

## Architecture

```
  RDS MySQL                S3 (raw)               S3 (processed)         S3 (curated)
  ─────────                ────────               ──────────────         ────────────
  customers ──CDC──►  cdc/customers/         customers/             customer_360/
  table         │    (Parquet, partitioned   orders/                  (partitioned by
                │     year/month)            products/                 rfm_segment)
                │
                │    DynamoDB                                        product_metrics/
                └──► cdc-control-table                                (partitioned by
                     (watermark: last_run_ts)                          i_category)
```

**Data flow:**

```
customers_cdc_job.py  →  customers_raw_to_processed.py  →  processed_to_curated.py
     (Layer 1)                   (Layer 2)                       (Layer 3)
   CDC from RDS              Rename + clean               RFM scoring + customer 360
   DynamoDB watermark         DQ checks                   Product metrics + ranking
   Parquet to S3 raw          Parquet to S3 processed     Parquet to S3 curated
```

---

## Files

| File | Purpose |
|------|---------|
| `customers_cdc_job.py` | AWS Glue Python Shell — reads rows changed since last run from RDS MySQL, merges with existing S3 data, writes Parquet (partitioned year/month) |
| `customers_raw_to_processed.py` | AWS Glue PySpark — renames raw columns (col0…col17 → readable names), derives full_name/age/birth_date, normalises emails, data quality gate (fails if >5% record loss) |
| `processed_to_curated.py` | AWS Glue PySpark — joins customers + orders + products, builds customer 360 with RFM scoring (5-tier: champions → lost), product metrics with category ranking |

---

## Prerequisites

- AWS account with permissions to create: Glue jobs, S3 buckets, RDS (MySQL), DynamoDB
- Python 3.11+ with `boto3` installed (`pip install boto3`)
- AWS credentials configured: `aws configure`
- Region: `us-east-1` (CDC job) / `ap-south-1` (Glue jobs) — update region constants in each file to match your setup
- S3 buckets already created: `datalake-raw`, `datalake-processed`, `datalake-curated`
- Glue database already created: `datalake_processed` (with tables `customers`, `orders`, `products` crawled from processed layer)

---

## Setup & Deployment

### Step 1 — Ingest (CDC from RDS)

Upload `customers_cdc_job.py` as a **Glue Python Shell job** and run with these parameters:

```bash
aws glue create-job \
  --name customers-cdc \
  --role AWSGlueServiceRole \
  --command '{"Name":"pythonshell","ScriptLocation":"s3://your-bucket/scripts/customers_cdc_job.py","PythonVersion":"3"}' \
  --default-arguments '{
    "--rds_endpoint": "your-rds-host.rds.amazonaws.com",
    "--rds_database": "your_database",
    "--rds_username": "your_user",
    "--rds_password": "your_password",
    "--target_bucket": "datalake-raw",
    "--dynamodb_table": "cdc-control-table"
  }'

aws glue start-job-run --job-name customers-cdc
```

The DynamoDB control table is created automatically on first run. On each subsequent run it reads only rows with `updated_at` after the last successful run timestamp.

### Step 2 — Raw → Processed

Upload `customers_raw_to_processed.py` as a **Glue PySpark job** (Glue 4.0, G.1X):

```bash
aws glue create-job \
  --name customers-raw-to-processed \
  --role AWSGlueServiceRole \
  --command '{"Name":"glueetl","ScriptLocation":"s3://your-bucket/scripts/customers_raw_to_processed.py","PythonVersion":"3"}' \
  --glue-version "4.0" \
  --worker-type G.1X --number-of-workers 2 \
  --default-arguments '{
    "--source_database": "datalake_raw",
    "--source_table": "customers",
    "--target_bucket": "datalake-processed",
    "--target_prefix": "customers",
    "--year": "2024",
    "--month": "1"
  }'

aws glue start-job-run --job-name customers-raw-to-processed
```

Data quality check: job fails if processed record count drops more than 5% vs raw count.

### Step 3 — Processed → Curated

Upload `processed_to_curated.py` as a **Glue PySpark job** (Glue 4.0, G.2X recommended for joins):

```bash
aws glue create-job \
  --name processed-to-curated \
  --role AWSGlueServiceRole \
  --command '{"Name":"glueetl","ScriptLocation":"s3://your-bucket/scripts/processed_to_curated.py","PythonVersion":"3"}' \
  --glue-version "4.0" \
  --worker-type G.2X --number-of-workers 4 \
  --default-arguments '{
    "--processed_bucket": "datalake-processed",
    "--curated_bucket": "datalake-curated"
  }'

aws glue start-job-run --job-name processed-to-curated
```

---

## Outputs

### `customer_360/` (partitioned by `rfm_segment`)

One row per customer. RFM segments:

| Segment | RFM Score |
|---------|-----------|
| `champions` | ≥ 13 |
| `loyal_customers` | 10–12 |
| `potential_loyalists` | 7–9 |
| `at_risk` | 5–6 |
| `lost` | < 5 |

Key columns: `c_customer_sk`, `c_full_name`, `rfm_score`, `rfm_segment`, `revenue_rank`, `preferred_category`, `total_spend`.

### `product_metrics/` (partitioned by `i_category`)

One row per product. Key columns: `i_product_name`, `total_revenue`, `profit_margin_pct`, `category_rank`, `is_top_10_in_category`.

---

## Cleanup

To delete all AWS resources created by this project:

```bash
# Delete Glue jobs
aws glue delete-job --job-name customers-cdc
aws glue delete-job --job-name customers-raw-to-processed
aws glue delete-job --job-name processed-to-curated

# Empty and delete S3 buckets
aws s3 rm s3://datalake-raw --recursive
aws s3 rm s3://datalake-processed --recursive
aws s3 rm s3://datalake-curated --recursive

# Delete DynamoDB control table
aws dynamodb delete-table --table-name cdc-control-table
```
