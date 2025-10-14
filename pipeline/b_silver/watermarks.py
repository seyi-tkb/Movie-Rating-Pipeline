import pandas as pd
import io
from pipeline.s3_client import initialize_s3_client
from config import S3_BUCKET_BRONZE, WATERMARKS_PATH
from utils.logger import get_logger
from datetime import datetime
from botocore.exceptions import ClientError

# initialize logger
logger = get_logger(__name__)

# function to read watermarks
def read_watermarks() -> pd.DataFrame:
    """
    Reads the existing watermarks from S3.

    Return:
       dataframe (pd.DataFrame): watermark file content as a dataframe. A structured but empty DataFrame if no watermark file exists.
    """

    # Initialize AWS S3 client
    client = initialize_s3_client()

    try:
        logger.info("Reading watermarks from S3 Bronze bucket...")
        response = client.get_object(Bucket=S3_BUCKET_BRONZE, Key=WATERMARKS_PATH)

        # Read parquet file directly from S3 response
        watermarks_df = pd.read_csv(io.BytesIO(response["Body"].read()))
        logger.info("Successfully read existing watermarks.")
        return watermarks_df

    except ClientError as e:
        # If the file doesn't exist, return empty DataFrame
        logger.warning(f"Watermarks not found in S3. Initializing new one. Details: {e}")
        columns = ["dataset_name", "max_value", "records_loaded", "processing_time"]
        return pd.DataFrame(columns=columns)


# function to update watermarks
def update_watermarks(new_watermark: dict):
    """
    Updates the watermarks table after an incremental load.

    Parameters:
         new_watermark (dict): {field_name: watermark_value}
    """
    logger.info("Updating watermarks in S3...")

    # Initialize AWS S3 client
    client = initialize_s3_client()

    try:
        # read old watermarks if available
        try:
            old_watermarks = read_watermarks()
        except Exception:
            logger.warning("No existing watermark file. Creating a new one.")
            old_watermarks = pd.DataFrame(columns=["dataset_name", "max_value", "records_loaded", "processing_time"])

        # add the new row
        new_row = pd.DataFrame([new_watermark])
        updated_watermark = pd.concat([old_watermarks, new_row], ignore_index=True)

        # write back to S3
        buffer = io.BytesIO()
        updated_watermark.to_csv(buffer, index=False)
        buffer.seek(0)

        client.put_object(
            Bucket=S3_BUCKET_BRONZE,
            Key=WATERMARKS_PATH,
            Body=buffer.getvalue(),
            ContentType="application/csv"
        )
        logger.info("Watermarks updated successfully in S3.")

    except Exception as e:
        logger.error(f"Failed to update watermarks: {e}")
        raise

if __name__ == "__main__":
    pass