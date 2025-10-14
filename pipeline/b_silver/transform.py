import pandas as pd
from io import BytesIO
from botocore.exceptions import ClientError
from pipeline.s3_client import initialize_s3_client
from utils.logger import get_logger
from config import S3_BUCKET_BRONZE, S3_BUCKET_SILVER, expected_columns
from pipeline.b_silver.watermarks import read_watermarks, update_watermarks
from pipeline.b_silver.read_write_buckets import read_file, write_to_silver

# initialize Logger
logger = get_logger(__name__)

# function to tranform movie df
def prepare_movie_df(pipeline_start, **kwargs: dict):
    """
    Transforms and ingests movie data from the bronze layer to the silver layer.

    Parameters:
        pipeline_start (datetime.datetime): The cutoff date for initial ingestion. Data beyond this date is excluded until the DAG progresses past it.

        **kwargs (dict):
            Airflow context dictionary containing:
            - execution_date (datetime): The logical start of the DAG run.
    """
    
    logger.info("Starting transformations for movie data..")
    
    # Get execution window
    exec_date = kwargs['data_interval_start'] # beginning of DAG run window

    # read from bronze 
    df = read_file(S3_BUCKET_BRONZE, 'movies.csv')
    # print("Executuion date is:", exec_date)

    # standardize column names
    df.columns = df.columns.str.strip().str.lower()

    # ensure release_date is datetime
    df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
    df['release_date'] = df['release_date'].dt.tz_localize('UTC')

    # drop duplicates
    df = df.drop_duplicates()

    # Filter: full load up to execution_date (bounded by resumption_date if needed)
    cutoff = min(exec_date, pipeline_start)
    df = df[df['release_date'] <= cutoff]
    df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
    
    logger.info(f"Full load of {df.shape[0]} records up to {cutoff.date()}")

    # drop rows with missing item_id or movie_title
    df = df.dropna(subset=['item_id', 'movie_title'])

    # trim and clean string columns
    df['movie_title'] = df['movie_title'].str.strip()
    df['primary_genre'] = df['primary_genre'].str.strip().str.title()
    df['imdb_url'] = df['imdb_url'].str.strip()

    # final shape logging
    logger.info(f"Movies data cleaned with {df.shape[0]} records")
    
    # upload to silver
    write_to_silver(df, 'movies.csv')

    # update the watermark with the latest processing record
    if not df.empty:
        latest_max_value = df['release_date'].max()
        data = {'dataset_name': 'movies',
                'max_value': latest_max_value,
                'records_loaded': df.shape[0],
                'processing_time': pd.Timestamp.now()}
        
        update_watermarks(data)


# function to transform ratings df
def prepare_ratings_df(pipeline_start, **kwargs: dict):
    """
    Transforms and ingests ratings data from the bronze layer to the silver layer.

    Parameters:
        pipeline_start (datetime.datetime): The cutoff date for initial ingestion. Data beyond this date is excluded until the DAG progresses past it.

        **kwargs (dict):
            Airflow context dictionary containing:
            - execution_date (datetime): The logical start of the DAG run.
            - next_execution_date (datetime): The logical end of the DAG run (not used in full load).
    """
    
    logger.info("Starting transformations for ratings..")

    # Get execution window
    int_start = kwargs['data_interval_start']
    int_end = kwargs['data_interval_end']
    logical_date = kwargs['logical_date']
    # execution_date/logical_date means start of period not actual time task is running
    # execution date + schedule interval


    # Read and Clean Bronze
    df = read_file(S3_BUCKET_BRONZE,'ratings.csv')
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')   # standardize column names
    df = df.drop_duplicates()        # drop exact duplicates
    df = df.dropna(subset=['user_id', 'item_id', 'rating'])     # Drop rows with missing user_id, item_id or rating
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')     # Convert timestamp into proper datetime
    df['timestamp'] = df['timestamp'].dt.tz_localize('UTC')     # localize to ensure parity with **kwargs

    # my_test
    logger.info(f"Interval Start: {int_start.date()}, interval End: {int_end.date()},\
                Logical start: {logical_date.date()}, Pipeline start: {pipeline_start.date()}")
    
    # Filter by Execution Window
    if int_end <= pipeline_start:
        # First run: ingest all data up to resumption_date
        df = df[df['timestamp'] <= pipeline_start]
        logger.info(f"Initial run: filtering {df.shape[0]} rating records up to {pipeline_start.date()}")
        target_month = pipeline_start.strftime('%Y-%m')      # get write partition
        
    else:
        # Weekly run: ingest only that week's data
        df = df[(df['timestamp'] >= int_start) & (df['timestamp'] < int_end)]
        logger.info(f"Weekly run: filtering {df.shape[0]} rating records from {int_start.date()} to {int_end.date()}")
    
    df['timestamp'] = df['timestamp'].dt.tz_localize(None)
    
    # incremental filter based on watermark
    watermarks = read_watermarks()
    if not watermarks.empty:
        logger.info("Applying watermark filter for ratings..")

        # Filter only 'ratings' dataset
        ratings_watermark = watermarks[watermarks["dataset_name"] == "ratings"]

        if not ratings_watermark.empty:
            # Safely convert to datetime (handles strings like "1998-04-22 23:10:38")
            ratings_watermark['max_value'] = pd.to_datetime(ratings_watermark['max_value'], errors='coerce')

            # Drop invalid or NaT entries
            ratings_watermark = ratings_watermark.dropna(subset=['max_value'])

            if not ratings_watermark.empty:
                latest_watermark = ratings_watermark['max_value'].max()
                df = df[df['timestamp'] > latest_watermark]
                logger.info(f"Filtered ratings to {df.shape[0]} new records after watermark {latest_watermark}.")
            else:
                logger.warning("No valid datetime values in ratings watermark. Proceeding without filter.")
        else:
            logger.warning("No existing watermark for ratings. Proceeding without filter.")
    else:
        logger.warning("No watermark file found. Proceeding without filter.")

    if df.empty:
        logger.warning("No new ratings data to upload after watermark filtering. End")
        return

    # write initial load into one partition
    if int_end <= pipeline_start:
        try:   
            existing_df = read_file(S3_BUCKET_SILVER, f'ratings/{target_month}.csv')
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                existing_df = pd.DataFrame(columns=expected_columns["ratings"])
            else:
                raise

        combined_df = pd.concat([existing_df, df], ignore_index=True)
        combined_df = combined_df.drop_duplicates()
        write_to_silver(combined_df,f'ratings/{target_month}.csv')

    # write subsequent loads into actual monthly partitions (if_exists i.e when a week spans 2 months.)
    else:    
    # group into monthly partitions 
        monthly_groups = df.groupby(df['timestamp'].dt.to_period('M'))  # pandas groupby obj (acts like a dataframe)

        for period, split_df in monthly_groups:

            month_str = period.strftime('%Y-%m')
            target_key = f'ratings/{month_str}.csv'

            try:
                existing_df = read_file(S3_BUCKET_SILVER, target_key)
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchKey':
                    existing_df = pd.DataFrame(columns=expected_columns["ratings"])
                else:
                    raise

            # write to partition
            combined_df = pd.concat([existing_df, split_df], ignore_index=True)
            combined_df = combined_df.drop_duplicates()
            write_to_silver(combined_df, target_key)
            logger.info(f"Wrote {split_df.shape[0]} records to {target_key}")


    # update the watermark with the new details
    if not df.empty:
        latest_max_value = df['timestamp'].max()
        data = {'dataset_name': 'ratings',
                'max_value': latest_max_value,
                'records_loaded': df.shape[0],
                'processing_time': pd.Timestamp.now()}
        
        update_watermarks(data)


# function to transform user df
def prepare_users_df():
    """
    Transforms and ingests ratings data from the bronze layer to the silver layer.
    """
    
    # read from bronze 
    df = read_file(S3_BUCKET_BRONZE, 'users.csv')

    logger.info("Starting transformations for users..")

    # standardize column names
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
    
    # drop users without user_id
    df = df.dropna(subset=['user_id'])

    # drop duplicates
    df = df.drop_duplicates()

    # clean string fields: whitespaces and case
    df['gender'] = df['gender'].str.strip().str.upper()
    df['occupation'] = df['occupation'].str.strip().str.title()
    df['zip_code'] = df['zip_code'].astype(str).str.strip()

    # final shape logging
    logger.info(f"Users data cleaned with {df.shape[0]} records")
    
    # upload to silver
    write_to_silver(df, 'users.csv')

    # update the watermark with the new details
    if not df.empty:
        latest_max_value = df['user_id'].max()
        data = {'dataset_name': 'users',
                'max_value': latest_max_value,
                'records_loaded': df.shape[0],
                'processing_time': pd.Timestamp.now()}
        
        update_watermarks(data)


if __name__ == "__main__":
    pass