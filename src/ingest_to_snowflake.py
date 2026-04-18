import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()

def run_snowflake_ingestion():
    print("❄️ Connecting to Snowflake...")
   
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
        print("✅ Connected Successfully!")

        print("🏗️ Verifying pipeline objects...")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS fulfillment_events (
            order_number VARCHAR, tracking_number VARCHAR, order_status VARCHAR,
            expected_weight FLOAT, box_size VARCHAR, expected_packing_date DATE,
            expected_delivery_date DATE, year INT, week_number INT,
            expected_item_qty INT, site_name VARCHAR, fulfillment_line_id FLOAT,
            approved_timestamp TIMESTAMP_NTZ, released_timestamp TIMESTAMP_NTZ,
            pick_start_timestamp TIMESTAMP_NTZ, picked_item_qty FLOAT,
            pick_complete_timestamp TIMESTAMP_NTZ, actual_weight FLOAT
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

        print("🚀 Executing COPY INTO command from S3 to Snowflake...")
        cursor.execute("""
        COPY INTO fulfillment_events
        FROM @my_s3_stage/fulfillment_event_data.csv
        ON_ERROR = 'CONTINUE';
        """)
       
        cursor.execute("SELECT COUNT(*) FROM fulfillment_events;")
        row_count = cursor.fetchone()[0]
        print(f"🎉 Success! There are now {row_count} rows in the fulfillment_events table.")

    except Exception as e:
        print(f"❌ Error during ingestion: {e}")
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals(): conn.close()
        print("🔌 Connection closed.")

if __name__ == "__main__":
    run_snowflake_ingestion()