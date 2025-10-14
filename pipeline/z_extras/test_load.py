import pandas as pd
from io import BytesIO
from sqlalchemy import text
from utils.logger import get_logger
from config import S3_BUCKET_SILVER, pipeline_start
from pipeline.b_silver.read_write_buckets import read_file
from pipeline.s_gold.read_bucket import read_silver_file
from pipeline.s_gold.connection import write_to_postgres, get_db_connection, execute_sql
from pipeline.s_gold.silver_watermarks import read_silver_watermarks, update_silver_watermarks
from pipeline.s_gold.sql.schema import *
from pipeline.s_gold.sql.ratings_partition import partition_functions


# initialize Logger
logger = get_logger(__name__)

# creating checking tables
def check_tables():
    """
    Ensures that all required PostgreSQL tables exist by executing their CREATE TABLE statements.
    """
    
    try:
        execute_sql(staging_schema_sql, "stg")
        execute_sql(production_schema_sql, "prod")
        logger.info(f"Schemas ensured.")
    except Exception as e:
        logger.error(f"Schema ensured: {str(e)}", exc_info=True)
    
    try:
        table_dict = {"stg_users": staging_users_sql,
                      "users": users_sql,
                      "stg_movies": staging_movies_sql,
                      "movies": movies_sql,
                      "stg_ratings": staging_ratings_sql,
                      "ratings": ratings_sql}
        
        for table_name, sql_str in table_dict.items():
            execute_sql(sql_str, table_name)
        logger.info(f"Tables ensured.")
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}", exc_info=True)

    try:
        execute_sql(partition_functions, "ratings_partitions")
        logger.info(f"Created partition functions.")
    except Exception as e:
        logger.error(f"{str(e)}", exc_info=True)

# function to load movie df
def load_movie_df():
    """
    Loads and processes movie data from the Silver layer into the Gold PostgreSQL database.
    """

    # read from silver 
    df = read_silver_file('movies.csv')

    # Convert release_date to datetime
    df['release_date'] = pd.to_datetime(df['release_date']).dt.tz_localize(None)

    # upload to staging
    try:
        write_to_postgres(df, 'stg_movies')
    except Exception as e:
        logger.error(f"WrITE TO STAGING failed: {str(e)}", exc_info=True)
        raise
    
    # upsert from staging
    try:
        engine = get_db_connection()

        with engine.begin() as conn:
            conn.execute(text(upsert_movies))
            logger.info(f'Upsert complete. New records successfully loaded') # need to put overwrite (SCD Type 1) methodology for this one from beginning

    except Exception as e:
        logger.error(f"Upsert failed: {str(e)}", exc_info=True)
        raise

    # update the watermark with the latest processing record
    if not df.empty:
        latest_max_value = df['release_date'].max()
        data = {'dataset_name': 'movies',
                'max_value': latest_max_value,
                'records_loaded': df.shape[0],
                'processing_time': pd.Timestamp.now()}
        
        update_silver_watermarks(data)


# function to transform user df
def load_users_df():
    """
    Loads and processes user data from the Silver layer into the Gold PostgreSQL database.
    """
    
    # read from silver 
    df = read_silver_file('users.csv')

    logger.info("Starting load for users..")

    # upload to postgres    
    try:
        write_to_postgres(df, 'stg_users')
    except Exception as e:
        logger.error(f"Write to stg_users failed: {str(e)}")
        raise

    # SCD check and upsert
    try:
        engine = get_db_connection()
        with engine.begin() as conn:
            conn.execute(text(upsert_users))
            logger.info("User upsert complete. New and changed user records successfully processed.")

    except Exception as e:
        logger.error(f"CDC upsert failed: {str(e)}", exc_info=True)
        raise
        
    # update the watermark with the new details
    if not df.empty:
        latest_max_value = df['user_id'].max()
        data = {'dataset_name': 'users',
                'max_value': latest_max_value,
                'records_loaded': df.shape[0],
                'processing_time': pd.Timestamp.now()
                                }
        
        update_silver_watermarks(data)


# function to load ratings df
def load_ratings_df(pipeline_start, **kwargs: dict):
    """
    Loads and processes ratings data from the Silver layer into the Gold PostgreSQL database.
    
    Parameters:
        pipeline_start : The cutoff date for initial ingestion. Data beyond this date is excluded until the DAG progresses past it.

        **kwargs (dict) : 
            Airflow context dictionary containing:
                - data_interval_start (datetime): Logical start of the DAG run.
                - data_interval_end (datetime): Logical end of the DAG run.
    """
    
    # Get execution window
    from datetime import datetime
    import pytz
    int_start = datetime(1997, 9, 1, tzinfo=pytz.UTC)
    int_end = datetime(1997, 10, 5, tzinfo=pytz.UTC)

    # determine read scope
    if int_end <= pipeline_start:
        start_month = (pipeline_start - pd.DateOffset(months=2)).replace(day=1) # first day of that month
        end_month = pipeline_start
    else:
        start_month = int_start
        end_month = int_end

    target_months = pd.date_range(start=start_month, end=end_month, freq='MS').strftime('%Y-%m').tolist()

    
    # read monthly partitions
    dfs = []
    for month in target_months:
        path = f'ratings/{month}.csv'
        try:
            df_month = read_silver_file(path)
            dfs.append(df_month)
        except Exception as e:
            if "NoSuchKey" in str(e):
                logger.warning(f"Partition {path} not found. Skipping.")
                continue
            else:
                logger.error(f"Unexpected error reading {path}: {e}")
                raise

    if not dfs:
        logger.warning("No ratings data found in selected partitions. Skipping load.")
        return


    df = pd.concat(dfs, ignore_index=True)

    # convert time col to datetime - ensure datetype compatibility with **kwargs
    df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize('UTC')

    # Filter by Execution Window
    if int_end <= pipeline_start:
        # First run: ingest all data up to resumption_date
        df = df[df['timestamp'] <= pipeline_start]
        logger.info(f"Initial run: filtering {df.shape[0]} rating records up to {pipeline_start.date()}")
        
    else:
        # Weekly run: ingest only that week's data
        df = df[(df['timestamp'] >= int_start) & (df['timestamp'] < int_end)]
        logger.info(f"Weekly run: filtering {df.shape[0]} rating records from {int_start.date()} to {int_end.date()}")
    
    # remove timezone for watermark compatibility
    df['timestamp'] = df['timestamp'].dt.tz_localize(None)
    
    # incremental filter based on watermark
    watermarks = read_silver_watermarks()

    if not watermarks.empty:
        logger.info("Applying watermark filter for ratings..")

        # Filter only 'ratings' dataset
        ratings_watermark = watermarks[watermarks["dataset_name"] == "ratings"]

        if not ratings_watermark.empty:
            # Safely convert to datetime
            ratings_watermark['max_value'] = pd.to_datetime(ratings_watermark['max_value'], errors='coerce')
            latest_watermark = ratings_watermark['max_value'].max()
            df = df[df['timestamp'] > latest_watermark]
            logger.info(f"Filtered ratings to {df.shape[0]} new records after watermark {latest_watermark}.")
        else:
            logger.warning("No valid/existing watermark for ratings. Proceeding without filter.")
    else:
        logger.warning("No watermark file found. Proceeding without filter.")

    if df.empty:
        logger.warning("No new ratings data to upload after watermark filtering. End")
        return
    
    # upload to staging table
    write_to_postgres(df, 'stg_ratings')

    # ensure partition exists in prod.ratings
    try:
        execute_sql(create_ratings_partition, "ratings_partitions")
        logger.info(f"Ratings table monthly partition ensured.")
    except Exception as e:
        logger.error(f"{str(e)}", exc_info=True)

    # upsert into gold
    try:
        execute_sql(upsert_ratings, "upsert ratings")
        logger.info("Ratings upsert complete. New records successfully loaded.")

    except Exception as e:
        logger.error(f"Upsert failed: {str(e)}", exc_info=True)
        raise

    # update the watermark with the new details
    if not df.empty:
        latest_max_value = df['timestamp'].max()
        data = {'dataset_name': 'ratings',
                'max_value': latest_max_value,
                'records_loaded': df.shape[0],
                'processing_time': pd.Timestamp.now()}
        
        update_silver_watermarks(data)


if __name__ == "__main__":
    pass