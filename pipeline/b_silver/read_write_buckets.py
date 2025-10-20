import pandas as pd
from io import BytesIO
from pipeline.s3_client import initialize_s3_client
from utils.logger import get_logger
from config import S3_BUCKET_BRONZE, S3_BUCKET_SILVER

# Initialize Logger
logger = get_logger(__name__)


#
def read_file(bucket_name: str, object_name: str) -> pd.DataFrame:
    """
    Reads a CSV file from the Bronze bucket into a DataFrame.

    Parameters:
        bucket_name (str): bucket name
        object_name (str): file name

    Return:
        dataframe (pd.Dataframe): structured content of file
    """
    logger.info(f"Reading {object_name} from {bucket_name} bucket.")

    # Initialize AWS S3 client
    client = initialize_s3_client()

    try:
        response = client.get_object(Bucket=bucket_name, Key=object_name)
        data = response["Body"].read()
        # bytesIO makes it so i dont have to download the file.
        # get to read the content from memory
        # masks the binary content and tricks pd.r_csv to think it is a real file
        df = pd.read_csv(BytesIO(data))
        logger.info(
            f"Successfully read {object_name} into DataFrame. Shape: {df.shape}"
        )
        return df

    except Exception as e:
        logger.error(f"Error reading {object_name} from S3 Bronze bucket: {str(e)}")
        raise


def write_to_silver(df: pd.DataFrame, object_name: str):
    """
    Uploads a DataFrame as a CSV file into the Silver bucket.

    Parameters:
        df (pd.Dataframe): structured content of file.
        object_name (str): file name to write to
    """
    logger.info(f"Writing {object_name} to {S3_BUCKET_SILVER} bucket.")

    # Initialize AWS S3 client
    client = initialize_s3_client()

    try:
        csv_buffer = BytesIO()
        df.to_csv(csv_buffer, index=False)  # puts content inside buffer
        csv_buffer.seek(0)  # resets buffer pointer to line 1 (beginning)

        client.put_object(
            Bucket=S3_BUCKET_SILVER,
            Key=object_name,
            Body=csv_buffer.getvalue(),
            ContentType="application/csv",
        )
        logger.info(f"Successfully wrote {object_name} to Silver bucket.")

    except Exception as e:
        logger.error(
            f"Error writing {object_name} to Silver bucket: {str(e)}", exc_info=True
        )
        raise


if __name__ == "__main__":
    pass
