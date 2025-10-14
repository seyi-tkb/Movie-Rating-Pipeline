# importing libraries/modules
import pandas as pd
from utils.logger import get_logger

# initialize logger
logger = get_logger()

# function to clean user df
def clean_users(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the users dataframe

    Parameters:
        urls: raw user dataframe

    Returns:
        pd.dataframe: cleaned user dataframe
    """
    logger.info("Cleaning users dataframe...")

    df = df.copy()

    # drop duplicates
    df = df.drop_duplicates()

    # clean string fields: whitespaces and case
    df['gender'] = df['gender'].str.strip().str.upper()
    df['occupation'] = df['occupation'].str.strip().str.title()
    df['zip_code'] = df['zip_code'].astype(str).str.strip()

    logger.info(f"Users dataframe cleaned: {df.shape[0]} records")
    return df


# function to clean item df
def clean_items(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the movie dataframe

    Parameters:
        urls: raw items dataframe

    Returns:
        pd.dataframe: cleaned items dataframe
    """
    logger.info("Cleaning items dataframe...")

    df = df.copy()

    # drop duplicates
    df = df.drop_duplicates()

    # Parse release_date to datetime
    df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')

    # Trim and clean string columns
    df['movie_title'] = df['movie_title'].str.strip()
    df['primary_genre'] = df['primary_genre'].str.strip().str.title()
    df['IMDb_URL'] = df['IMDb_URL'].str.strip()

    logger.info(f"Items dataframe cleaned: {df.shape[0]} records")
    return df


# function to clean data df
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the data dataframe

    Parameters:
        urls: raw data dataframe

    Returns:
        pd.dataframe: cleaned data dataframe
    """
    logger.info("Cleaning data dataframe...")

    df = df.copy()

    # Drop exact duplicates
    df = df.drop_duplicates()

    # Parse timestamp to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')

    logger.info(f"Data dataframe cleaned: {df.shape[0]} records")
    return df
