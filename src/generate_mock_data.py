import pandas as pd
import logging
from faker import Faker
import random
import os
import sys
from datetime import datetime, timedelta
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Load variables from the .env file
load_dotenv()

# Initialize Faker
fake = Faker('en_IN')

def generate_state_machine_fulfillment_data(num_records=200):
    logging.info(f"🏭 Generating {num_records} lifecycle-accurate fulfillment records...")
   
    data = []
    sites = ['Bengaluru (Karnataka)', 'Ahmedabad (Gujarat)', 'Mumbai (Maharashtra)', 'Pune (Maharashtra)']
    statuses = ['planned', 'approved', 'released', 'picking', 'picked complete']
    box_sizes = ['Small', 'Medium', 'Large', 'Extra Large']
   
    for _ in range(num_records):
        # 1. Determine the current state of this specific order
        status_idx = random.randint(0, len(statuses) - 1)
        current_status = statuses[status_idx]
       
        # --- BASE STATE: Applies to ALL orders (even 'planned') ---
        # Generate base creation time
        base_time = datetime.now() - timedelta(minutes=random.randint(0, 1440))
       
        expected_packing_date = (base_time + timedelta(days=random.randint(0, 2))).date()
        expected_delivery_date = (expected_packing_date + timedelta(days=random.randint(2, 5)))
       
        expected_item_qty = random.randint(1, 40)
        expected_weight = round(random.uniform(0.5, 25.0), 2)
       
        record = {
            'order_number': f"ORD-{fake.unique.random_number(digits=9, fix_len=True)}",
            'tracking_number': f"AWB{fake.unique.random_number(digits=11, fix_len=True)}",
            'order_status': current_status,
            'expected_weight': expected_weight,
            'box_size': random.choice(box_sizes),
            'expected_packing_date': expected_packing_date.isoformat(),
            'expected_delivery_date': expected_delivery_date.isoformat(),
            'year': base_time.year,
            'week_number': base_time.isocalendar()[1],
            'expected_item_qty': expected_item_qty,
            'site_name': random.choice(sites),
           
            # Initialize downstream fields as None (NULL)
            'fulfillment_line_id': None,
            'approved_timestamp': None,
            'released_timestamp': None,
            'pick_start_timestamp': None,
            'picked_item_qty': None,
            'pick_complete_timestamp': None,
            'actual_weight': None
        }

        # --- PROGRESSIVE STATE LOGIC ---
       
        # 2. APPROVED LOGIC
        if status_idx >= 1:
            record['fulfillment_line_id'] = random.choice([1, 2, 3])
            t_approved = base_time + timedelta(minutes=random.randint(5, 60))
            record['approved_timestamp'] = t_approved.isoformat()

        # 3. RELEASED LOGIC
        if status_idx >= 2:
            t_released = t_approved + timedelta(minutes=random.randint(10, 120))
            record['released_timestamp'] = t_released.isoformat()

        # 4. PICKING LOGIC
        if status_idx >= 3:
            t_pick_start = t_released + timedelta(minutes=random.randint(5, 45))
            record['pick_start_timestamp'] = t_pick_start.isoformat()
           
            # If it's CURRENTLY picking, it hasn't picked everything yet.
            if current_status == 'picking':
                # Pick a random number between 0 and expected_qty - 1
                record['picked_item_qty'] = random.randint(0, max(0, expected_item_qty - 1))

        # 5. PICKED COMPLETE LOGIC
        if status_idx >= 4:
            t_pick_complete = t_pick_start + timedelta(minutes=random.randint(10, 90))
            record['pick_complete_timestamp'] = t_pick_complete.isoformat()
           
            # Actual weight gets measured at the end
            record['actual_weight'] = max(0.1, round(expected_weight + random.uniform(-0.5, 0.5), 2))
           
            # 95% of the time picked matches expected. 5% short pick.
            if random.random() > 0.05:
                record['picked_item_qty'] = expected_item_qty
            else:
                record['picked_item_qty'] = max(0, expected_item_qty - random.randint(1, 3))

        data.append(record)
       
    return pd.DataFrame(data)

def upload_to_s3(local_file_path, bucket_name, s3_file_key):
    logging.info(f"☁️ Uploading {local_file_path} to s3://{bucket_name}/{s3_file_key}...")
    
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
        )
    
        s3_client.upload_file(local_file_path, bucket_name, s3_file_key)
        logging.info("✅ Upload successful!")
    except FileNotFoundError:
        logging.error(f"CRITICAL: The local file {local_file_path} was not found")
        sys.exit(1)
    except ClientError as e:
        error_code = e.response['Error']['Code']
        logging.error(f"CRITICAL: AWS S3 Upload Failed! Error Code: {error_code}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"An unexpected error occurred during S3 upload: {e}")
        sys.exit(1)

if __name__ == "__main__":

    # Fail fast check
    bucket = os.getenv('AWS_S3_BUCKET_NAME')
    aws_key = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret = os.getenv('AWS_SECRET_ACCESS_KEY')

    if not bucket or not aws_key or not aws_secret:
        logging.error("CRITICAL: Missing AWS environment variables (AWS_S3_BUCKET_NAME or AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY)")
        sys.exit(1)

    os.makedirs("data/raw", exist_ok=True)
   
    # 1. Generate Data (Defaulting to 200 for the daily pipeline batch)
    try:
        df = generate_state_machine_fulfillment_data(200)

        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        local_path = f"data/raw/fulfillment_events_{timestamp_str}.csv"
    
        # Ensure correct column order matching your exact specifications
        ordered_columns = [
            'order_number', 'tracking_number', 'order_status', 'expected_weight', 'box_size',
            'expected_packing_date', 'expected_delivery_date', 'year', 'week_number',
            'expected_item_qty', 'site_name', 'fulfillment_line_id', 'approved_timestamp',
            'released_timestamp', 'pick_start_timestamp', 'picked_item_qty',
            'pick_complete_timestamp', 'actual_weight'
        ]
        df = df[ordered_columns]

        df['_extracted_at'] = datetime.now().isoformat()
    
        df.to_csv(local_path, index=False)
    
        logging.info(f"💾 Local file saved to {local_path}")
    
        # Show a quick preview of rows in different states to verify the logic worked
        logging.info("\n--- Logic Verification Preview ---")
        logging.info(df[df['order_status'] == 'planned'][['order_status', 'approved_timestamp', 'fulfillment_line_id']].head(1))
        logging.info(df[df['order_status'] == 'picking'][['order_status', 'expected_item_qty', 'picked_item_qty', 'pick_complete_timestamp']].head(1))
        logging.info(df[df['order_status'] == 'picked complete'][['order_status', 'expected_weight', 'actual_weight', 'pick_complete_timestamp']].head(1))
    
        # 2. Upload to AWS S3
        s3_key = f"raw/fulfillment_events_{timestamp_str}.csv"
    
        upload_to_s3(local_path, bucket, s3_key)
    except Exception as e:
        logging.error(f"Pipeline failed during data generation: {e}")
        sys.exit(1)