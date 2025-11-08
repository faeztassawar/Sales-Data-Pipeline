import os
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError, NoCredentialsError

load_dotenv()

AWS_REGION = os.getenv("AWS_REGION")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

def validate():
    if not (AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY):
        print("❌ AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY missing in .env")
        return False
    try:
        sts = boto3.client(
            "sts",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
        identity = sts.get_caller_identity()
        print("✅ Credentials valid. Caller identity:")
        print("  Account:", identity.get("Account"))
        print("  ARN:", identity.get("Arn"))
    except NoCredentialsError:
        print("❌ No credentials found.")
        return False
    except ClientError as e:
        print("❌ AWS ClientError:", e.response.get("Error", {}).get("Message"))
        return False
    except Exception as e:
        print("❌ Unexpected error:", str(e))
        return False

    # Try listing S3 buckets (permission might be restricted; this is only a check)
    try:
        s3 = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
        resp = s3.list_buckets()
        buckets = [b["Name"] for b in resp.get("Buckets", [])]
        print("ℹ️ Buckets in account (if list permission allowed):", buckets)
    except ClientError as e:
        # Might be AccessDenied for list_buckets; still okay if STS succeeded
        print("⚠️ Could not list buckets:", e.response.get("Error", {}).get("Message"))
    return True

if __name__ == "__main__":
    validate()
