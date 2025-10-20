import pandas as pd
from io import BytesIO
from pipeline.s3_client import initialize_s3_client
from utils.logger import get_logger
from config import S3_BUCKET_SILVER


# initialize Logger
logger = get_logger(__name__)


# function to read from silver bucket
def read_silver_file(object_name: str) -> pd.DataFrame:
    """
    Reads a CSV file from the Silver S3 bucket and loads it into a pandas DataFrame.

    Parameters:
        object_name (str): the name of the object (file) to read from the Silver bucket.

    Returns:
        pd.DataFrame: a DataFrame containing the contents of the CSV file.
    """

    # initialize s3 Client
    client = initialize_s3_client()

    logger.info(f"Reading {object_name} from {S3_BUCKET_SILVER} bucket.")

    try:
        response = client.get_object(Bucket=S3_BUCKET_SILVER, Key=object_name)
        data = response["Body"].read()
        df = pd.read_csv(BytesIO(data))
        logger.info(
            f"Successfully read {object_name} into DataFrame. Shape: {df.shape}"
        )
        return df

    except Exception as e:
        logger.error(f"Error reading {object_name} from {S3_BUCKET_SILVER}: {str(e)}")
        raise


if __name__ == "__main__":
    pass
