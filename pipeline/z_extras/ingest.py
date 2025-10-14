# importing libraries/modules
import pandas as pd
import io
from utils.logger import get_logger
from pipeline.s3_client import initialize_minio_client
from config import MINIO_BRONZE_BUCKET


# initialize logger
logger = get_logger(__name__)


# function to read files from source (drive)
def read_files(urls: dict) -> dict: 
    """
    Reads a list of the 3 file paths into dataframes

    Parameters:
        urls: dicyionary of file names and their urls

    Returns:
        dict: dictionary of file names and created dataframes
    """
    dataframes = {}
    client = initialize_minio_client()

    for file, url in urls:
        try:
            file_id = url.split('/')[-2]
            direct_url = f"https://drive.usercontent.google.com/download?id={file_id}&export=download&authuser=0&confirm=t"
            
            logger.info(f"Reading {file}...")
            
            df = pd.read_csv(direct_url)
            dataframes[file] = df
            
            logger.info(f"Successfully read {file}.")

        except Exception as e:
            logger.info(f"Failed to read {file}. Error: {e}")

    return dataframes

# list of file urls
# url_list = {"user_movie_lens" : "https://drive.google.com/file/d/1_wAww5beF2K7dpx-SU_gUUddNWeaeZqv/view?usp=drive_link", 
#            "item_movie_lens" : "https://drive.google.com/file/d/188tIKLJKek62rGmzj1Ylc03fe4Pgb5co/view?usp=drive_link", 
#            "data_movie_lens" : "https://drive.google.com/file/d/1-3S-XOgZyo9D3sVoXtjPvmFdsihjfQhN/view?usp=drive_link"
#            }

# read files
# items_df = dataframes["items"]
# data_df = dataframes["data"]
# users_df = dataframes["users"]


def upload_to_bronze(dataframes: dict):
    """
    Uploads dataframes to a specified MinIO bucket.

    Parameters:
        dataframes: dictionary of file names and their dataframes.
        bucket_name: name of the MinIO bucket to upload files to.
    """
    client = initialize_minio_client()

    for file, df in dataframes.items():
        try:
            logger.info(f"Starting {file} upload to bronze bucket")

            # Convert DataFrame to in-memory bytes (buffer)
            csv_buffer = io.BytesIO()
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)

            
            object_name = f"{file}.csv"
            logger.info(f"Uploading {object_name} to MinIO {MINIO_BRONZE_BUCKET} bucket")

            # Upload the  file to the MinIO bucket
            client.put_object(
                bucket_name = MINIO_BRONZE_BUCKET,
                object_name = object_name,
                data = csv_buffer,
                length = csv_buffer.getbuffer().nbytes
            )

            logger.info(f"Successfully uploaded {object_name} to {MINIO_BRONZE_BUCKET} bucket.")

        except Exception as e:
            logger.info(f"Failed to upload {file}. Error: {e}")




# ----------------------------
# VALIDATION
# ----------------------------

# src/preliminary_validation.py


# function to check columns
def validate_columns(dataframes: dict, expected_cols: dict):
    """
    Validates that each dataframe contains the expected columns.
    Args:
        dataframes (dict): {name: dataframe}
        expected_columns (dict): {name: list of expected columns}
    """
    for file, df in dataframes.items():
        logger.info(f"Validating columns for {file}..")
        expected_columns = set(expected_cols[file])
        actual_columns = set(df.columns)
        missing = expected_columns - actual_columns
        extra = actual_columns - expected_columns

        # if missing set is not empty
        if missing: 
            logger.error(f"Missing columns in {file}: {missing}")
            raise ValueError(f"Missing columns in {file}: {missing}")
        
        # if extra columns are present
        if extra:
            logger.warning(f"Extra columns found in {file}: {extra}")

        logger.info(f"successfully validated {file}'s columns.")

# function to check null values
def validate_nulls(dataframes: dict):
    """
    Logs if there are any null values in the dataframes.
    Args:
        dataframes (dict): {name: dataframe}
    """
    for name, df in dataframes.items():
        null_counts = df.isnull().sum()
        total_nulls = null_counts.sum()
        if total_nulls > 0:
            logger.warning(f"{name} has {total_nulls} missing values:\n{null_counts[null_counts > 0]}")
        else:
            logger.info(f"No missing values found in {name}.")

