import logging
import pandas as pd
from config import POSTGRES_CONFIG
from sqlalchemy import create_engine, text

# initializing logger
logger = logging.getLogger(__name__)

#function to connect to database
def get_db_connection():
    """
    Establishes a SQLAlchemy engine connection to a PostgreSQL database using credentials from config.

    Returns:
        sqlalchemy.engine.Engine: a SQLAlchemy engine instance connected to the specified database.
    """
    try:
        engine = create_engine(
            f"postgresql+psycopg2://{POSTGRES_CONFIG['user']}:{POSTGRES_CONFIG['password']}@"
            f"{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}"
        )
        logger.info("Successfully connected to Postgres.")
        return engine
    
    except Exception as e:
        logger.error(f"PostgreSQL connection error: {str(e)}")
        raise


# function to create table
# i need to turn this into EXECUTE STATEMENT function
# use it for upserts and all that as well
def execute_sql(sql: str, text_name: str):
    """
    Executes SQL code.

    Parameters:
        sql (str): SQL statement defining the table schema.
        table (str): Name of the table to be created.
    """
    try:
        engine = get_db_connection()
        with engine.begin() as conn: # begin ensures commit. connect has to be closed to commit
            conn.execute(text(sql))
        logger.info(f"Ensured '{text_name}' in Postgres.")

    except Exception as e:
        logger.error(f"Error creating table '{text_name}': {str(e)}")
        raise


# function to upload to postgres
def write_to_postgres(df: pd.DataFrame, table_name: str, mode: str = "append", schema: str = "stg"): #append but table is always truncated so its clean. "replace" and to_sql cause type issues
    """
    Writes a pandas DataFrame to a PostgreSQL table.

    Parameters:
        df (pd.DataFrame): the DataFrame to be written.
        table_name (str): target table name in the database.
        mode (str): write mode for SQLAlchemy's to_sql. Options:("append", "replace", "fail")
        schema (str): schema to insert into. default is staging.
    """

    logger.info(f"Writing {df.shape[0]} {table_name} records to Postgres..")
    
    try:
        engine = get_db_connection()

        df.to_sql(name=table_name, schema= schema, con=engine, if_exists=mode, index=False, method='multi', chunksize=1000)

        logger.info(f"Successfully inserted {len(df)} records into {table_name}.")
    except Exception as e:
        logger.exception(f"Error during df.to_sql for {table_name}: {str(e)}")
        raise

if __name__ == "__main__":
    pass