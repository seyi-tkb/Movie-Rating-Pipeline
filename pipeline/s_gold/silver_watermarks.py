import pandas as pd
from io import BytesIO
from pipeline.s3_client import initialize_s3_client
from config import S3_BUCKET_SILVER, WATERMARKS_PATH
from utils.logger import get_logger
from datetime import datetime


# initialize logger
logger = get_logger(__name__)


# function to read watermarks
def read_silver_watermarks():
    """
    Reads the existing watermarks from S3.

    Return:
       dataframe (pd.DataFrame): watermark file content as a dataframe. A structured but empty DataFrame if no watermark file exists.
    """

    # initialize s3 Client
    client = initialize_s3_client()

    try:
        logger.info("Reading watermarks from s3..")
        response = client.get_object(Bucket=S3_BUCKET_SILVER, Key=WATERMARKS_PATH)
        watermarks_df = pd.read_csv(BytesIO(response["Body"].read()))
        return watermarks_df

    except Exception as e:
        logger.warning(f"Watermarks not found. Initializing One. Details: {e}")
        columns = ["dataset_name", "max_value", "records_loaded", "processing_time"]
        return pd.DataFrame(columns=columns)


# function to write to watermarks
def update_silver_watermarks(new_watermark: dict):
    """
    Updates the watermarks table after an incremental load.

    Parameters:
         new_watermark (dict): {field_name: watermark_value}
    """

    # initialize s3 Client
    client = initialize_s3_client()

    try:
        # read old watermarks if available
        try:
            old_watermarks = read_silver_watermarks()
        except Exception:
            logger.warning("No existing watermark file. Creating a new one.")
            old_watermarks = pd.DataFrame(
                columns=[
                    "dataset_name",
                    "max_value",
                    "records_loaded",
                    "processing_time",
                ]
            )

        logger.info("Updating watermarks in S3...")

        # add the new row
        new_row = pd.DataFrame([new_watermark])
        # helps with not appending empty dfs
        if old_watermarks.empty:
            updated_watermark = new_row
        else:
            updated_watermark = pd.concat([old_watermarks, new_row], ignore_index=True)

        # write back to S3
        buffer = BytesIO()
        updated_watermark.to_csv(buffer, index=False)
        buffer.seek(0)

        client.put_object(
            Bucket=S3_BUCKET_SILVER,
            Key=WATERMARKS_PATH,
            Body=buffer.getvalue(),
            ContentType="application/csv",
        )
        logger.info("Watermarks updated successfully in S3.")

    except Exception as e:
        logger.error(f"Failed to update watermarks: {e}")
        raise


if __name__ == "__main__":
    pass
