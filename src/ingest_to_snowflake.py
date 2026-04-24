import snowflake.connector
import os
import sys
import logging
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

load_dotenv()

def run_snowflake_ingestion():

    # FAIL FAST
    required_vars = [
        'SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD', 'SNOWFLAKE_ACCOUNT',
        'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_S3_BUCKET_NAME'
    ]
    for var in required_vars:
        if not os.getenv(var):
            logging.error(f"CRITICAL: Missing required environment variable: {var}")
            sys.exit(1)

    logging.info("❄️ Connecting to Snowflake...")
   
    try:
        # 1. Connect using the role Terraform just built
        conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            role='PIPELINE_ROLE',
            warehouse='FULFILLMENT_WH',
            database='FULFILLMENT_DB',
            schema='RAW'
        )
        cursor = conn.cursor()
        logging.info("✅ Connected Successfully!")
    except snowflake.connector.errors.ProgrammingError as e:
        logging.error(f"CRITICAL: Snowflake Auth Error: {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"CRITICAL: Failed to connect to Snowflake: {e}")
        sys.exit(1)

    try:
        logging.info("🏗️ Verifying pipeline objects...")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS fulfillment_events (
            order_number VARCHAR, tracking_number VARCHAR, order_status VARCHAR,
            expected_weight FLOAT, box_size VARCHAR, expected_packing_date DATE,
            expected_delivery_date DATE, year INT, week_number INT,
            expected_item_qty INT, site_name VARCHAR, fulfillment_line_id FLOAT,
            approved_timestamp TIMESTAMP_NTZ, released_timestamp TIMESTAMP_NTZ,
            pick_start_timestamp TIMESTAMP_NTZ, picked_item_qty FLOAT,
            pick_complete_timestamp TIMESTAMP_NTZ, actual_weight FLOAT,
            _extracted_at TIMESTAMP_NTZ, _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        );
        """)

        cursor.execute("""
        CREATE FILE FORMAT IF NOT EXISTS my_csv_format
            TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"' NULL_IF = ('', 'NULL', 'None');
        """)

        aws_key = os.getenv('AWS_ACCESS_KEY_ID')
        aws_secret = os.getenv('AWS_SECRET_ACCESS_KEY')
        s3_bucket = os.getenv('AWS_S3_BUCKET_NAME')
       
        cursor.execute(f"""
        CREATE STAGE IF NOT EXISTS my_s3_stage
            URL = 's3://{s3_bucket}/raw/'
            CREDENTIALS = (AWS_KEY_ID = '{aws_key}' AWS_SECRET_KEY = '{aws_secret}')
            FILE_FORMAT = my_csv_format;
        """)

        logging.info("🚀 Executing COPY INTO command from S3 to Snowflake...")
        cursor.execute("""
        COPY INTO fulfillment_events (
            order_number, tracking_number, order_status,
            expected_weight, box_size, expected_packing_date,
            expected_delivery_date, year, week_number,
            expected_item_qty, site_name, fulfillment_line_id,
            approved_timestamp, released_timestamp,
            pick_start_timestamp, picked_item_qty,
            pick_complete_timestamp, actual_weight,
            _extracted_at
        )
        FROM @my_s3_stage/
        PATTERN = '.*fulfillment_events_.*\\.csv'
        ON_ERROR = 'CONTINUE';
        """)
       
        cursor.execute("SELECT COUNT(*) FROM fulfillment_events;")
        row_count = cursor.fetchone()[0]
        logging.info(f"🎉 Success! There are now {row_count} rows in the fulfillment_events table.")

    except snowflake.connector.errors.ProgrammingError as e:
        logging.error(f"CRITICAL: Snowflake SQL Execution Error: {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"CRITICAL: Error during ingestion SQL execution: {e}")
        sys.exit(1)
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals(): conn.close()
        logging.info("🔌 Connection closed.")

if __name__ == "__main__":
    run_snowflake_ingestion()