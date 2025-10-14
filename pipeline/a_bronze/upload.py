# importing libraries/modules
import io
import boto3
from utils.logger import get_logger
from pipeline.s3_client import initialize_s3_client
from config import S3_BUCKET_BRONZE

# initialize logger
logger = get_logger(__name__)

def upload_to_bronze(s3_client, dataframes: dict):
    """
    Uploads dataframes to a specified S3 Bronze bucket.

    Parameters:
        dataframes: dictionary of file names and their dataframes.
    """
    # s3 client initialized in orchestration script passed
    client = s3_client

    for file, df in dataframes.items():
        try:
            logger.info(f"Starting {file} upload to bronze bucket")

            # Convert DataFrame to in-memory buffer
            csv_buffer = io.BytesIO()
            df.to_csv(csv_buffer, index=False) # puts content inside buffer
            csv_buffer.seek(0)

            object_name = f"{file}.csv"  # prefix ensures Bronze folder in bucket
            logger.info(f"Uploading {object_name} to S3 bucket {S3_BUCKET_BRONZE}")

            # Upload the file to S3
            client.put_object(
                Bucket=S3_BUCKET_BRONZE,
                Key=object_name,
                Body=csv_buffer.getvalue()
            )

            logger.info(f"Successfully uploaded {object_name} to S3 bucket {S3_BUCKET_BRONZE}.")

        except Exception as e:
            logger.error(f"Failed to upload {file}. Error: {e}")

