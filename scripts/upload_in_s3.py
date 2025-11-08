import boto3
import os
from dotenv import load_dotenv
from botocore.exceptions import ClientError

load_dotenv()

AWS_REGION = os.getenv("AWS_REGION")
BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
RAW_DATA_PATH = os.getenv("S3_RAW_DATA_PATH")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

s3 = boto3.client(
    "s3",
    aws_access_key_id = AWS_ACCESS_KEY_ID,
    aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
    region_name = AWS_REGION
)

def upload_raw_files(bucket_name):
    """Upload CSVs from data/raw to S3."""
    import glob
    files = glob.glob(f"{RAW_DATA_PATH}/*.csv")
    if not files:
        print(f"⚠️ No raw CSV files found in {RAW_DATA_PATH}")
        return
    for file_path in files:
        key = f"data/raw/{os.path.basename(file_path)}"
        s3.upload_file(file_path, bucket_name, key)
        print(f"📤 Uploaded {file_path} → s3://{bucket_name}/{key}")

def list_bucket_objects(bucket_name):
    """List objects inside the bucket."""
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix="data/raw/")
    print("\n📁 Objects in S3 bucket:")
    for obj in response.get("Contents", []):
        print(" -", obj["Key"])

if __name__ == "__main__":
    upload_raw_files(BUCKET_NAME)
    list_bucket_objects(BUCKET_NAME)