import sys
import boto3
import pandas as pd
import pymysql
import awswrangler as wr
from awsglue.utils import getResolvedOptions
from datetime import datetime
import logging

# ── Logging ───────────────────────────────────────────────────────────────────
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
logger.addHandler(handler)

# ── Job arguments ─────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'rds_endpoint',
    'rds_database',
    'rds_username',
    'rds_password',
    'target_bucket',
    'dynamodb_table',
])

# ── DynamoDB control table ────────────────────────────────────────────────────
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
control_table = dynamodb.Table(args['dynamodb_table'])

JOB_NAME = 'customers_cdc'
run_id   = datetime.utcnow().strftime('%Y-%m-%d-%H:%M:%S')

def get_last_run_timestamp():
    response = control_table.query(
        KeyConditionExpression=boto3.dynamodb.conditions.Key('job_name').eq(JOB_NAME),
        ScanIndexForward=False,
        Limit=1
    )
    items = response.get('Items', [])
    if items:
        ts = items[0]['last_run_timestamp']
        logger.info(f"Last run timestamp: {ts}")
        return ts
    return '1900-01-01 00:00:00'

def save_control_record(status, records_processed, error=None):
    item = {
        'job_name':           JOB_NAME,
        'run_id':             run_id,
        'last_run_timestamp': run_id,
        'status':             status,
        'records_processed':  records_processed,
    }
    if error:
        item['error_message'] = str(error)
    control_table.put_item(Item=item)
    logger.info(f"Control record saved: {status}, {records_processed} records")

try:
    # ── Get last run timestamp ────────────────────────────────────────────────
    last_run = get_last_run_timestamp()
    logger.info(f"Reading changes since: {last_run}")

    # ── Read from MySQL using pymysql ─────────────────────────────────────────
    conn = pymysql.connect(
        host=args['rds_endpoint'],
        user=args['rds_username'],
        password=args['rds_password'],
        database=args['rds_database'],
        port=3306
    )

    query = f"""
        SELECT 
            customer_id, first_name, last_name,
            email, phone, country,
            customer_segment, lifetime_value,
            created_at, updated_at
        FROM customers
        WHERE updated_at > '{last_run}'
        ORDER BY updated_at ASC
    """

    df_new = pd.read_sql(query, conn)
    conn.close()

    record_count = len(df_new)
    logger.info(f"New/changed records found: {record_count}")

    if record_count == 0:
        logger.info("No new data. Exiting.")
        save_control_record('SUCCESS', 0)
        sys.exit(0)

    # ── Transformations ───────────────────────────────────────────────────────
    df_new['full_name']    = df_new['first_name'] + ' ' + df_new['last_name']
    df_new['is_premium']   = df_new['customer_segment'] == 'premium'
    df_new['processed_at'] = datetime.utcnow().isoformat()
    df_new['year']         = df_new['updated_at'].dt.year.astype(str)
    df_new['month']        = df_new['updated_at'].dt.month.astype(str)

    # ── Read existing target ──────────────────────────────────────────────────
    TARGET = f"s3://{args['target_bucket']}/cdc/customers/"

    try:
        df_existing = wr.s3.read_parquet(path=TARGET)
        logger.info(f"Existing records: {len(df_existing)}")

        # Remove records that will be updated
        df_existing = df_existing[
            ~df_existing['customer_id'].isin(df_new['customer_id'])
        ]

        # Merge existing + new
        df_final = pd.concat([df_existing, df_new], ignore_index=True)

    except Exception:
        logger.info("First run — no existing target data")
        df_final = df_new

    # ── Write to S3 ───────────────────────────────────────────────────────────
    logger.info(f"Writing {len(df_final)} records to {TARGET}")

    wr.s3.to_parquet(
        df=df_final,
        path=TARGET,
        dataset=True,
        partition_cols=['year', 'month'],
        mode='overwrite_partitions'
    )

    save_control_record('SUCCESS', record_count)
    logger.info("CDC job completed successfully")

except Exception as e:
    logger.error(f"Job failed: {str(e)}")
    save_control_record('FAILED', 0, error=str(e))
    raise e