import boto3
import pandas as pd
from io import BytesIO
from botocore.exceptions import ClientError
from config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION
from utils.logger import get_logger

# Initialize Logger
logger = get_logger(__name__)

_client = None  # Global cache. everytime below is called, check if one already exists


def initialize_s3_client():
    """
    Initialize and return a boto3 S3 client instance.
    """
    global _client

    if _client is not None:
        # Already initialized — reuse existing client
        return _client

    try:
        _client = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION,
        )
        logger.info("Successfully initialized S3 client.")
        return _client

    except ClientError as e:
        logger.error(f"AWS ClientError during S3 initialization: {e}")
        raise
    except Exception as e:
        logger.error(f"Failed to initialize S3 client: {str(e)}")
        raise


def create_bucket_if_not_exists(client, bucket_name):
    """
    Create an S3 bucket if it doesn't exist already.
    """
    try:
        # Check if bucket exists
        existing_buckets = client.list_buckets()
        if not any(b["Name"] == bucket_name for b in existing_buckets["Buckets"]):
            client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": AWS_REGION},
            )
            logger.info(f"Bucket '{bucket_name}' created successfully.")
        else:
            logger.info(f"Bucket '{bucket_name}' already exists.")

    except ClientError as e:
        logger.error(f"Bucket creation error: {str(e)}")
        raise


def upload_file(client, bucket_name, object_name, file_path):
    """
    Upload a file to a specified S3 bucket.
    """
    try:
        client.upload_file(file_path, bucket_name, object_name)
        logger.info(
            f"File '{file_path}' uploaded to bucket '{bucket_name}' as '{object_name}'."
        )

    except ClientError as e:
        logger.error(f"Upload error: {str(e)}")
        raise


def download_file(client, bucket_name, object_name, file_path):
    """
    Download a file from S3 bucket to local path.
    """
    try:
        client.download_file(bucket_name, object_name, file_path)
        logger.info(
            f"File '{object_name}' downloaded from bucket '{bucket_name}' to '{file_path}'."
        )

    except ClientError as e:
        logger.error(f"Download error: {str(e)}")
        raise


def read_file(client, bucket_name, object_name):
    """
    Read a file (CSV/Parquet) from S3 bucket into a DataFrame.
    """
    try:
        response = client.get_object(Bucket=bucket_name, Key=object_name)
        logger.info(f"Reading file '{object_name}' from bucket '{bucket_name}'.")

        # Guess file type from extension
        if object_name.endswith(".parquet"):
            df = pd.read_parquet(BytesIO(response["Body"].read()))
        else:
            df = pd.read_csv(BytesIO(response["Body"].read()))

        logger.info(f"File '{object_name}' successfully read into DataFrame.")
        return df

    except ClientError as e:
        logger.error(f"Read error: {str(e)}")
        raise


if __name__ == "__main__":
    # Only runs if you execute s3_client.py directly
    initialize_s3_client
