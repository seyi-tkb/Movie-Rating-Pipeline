# Importing required modules
from pipeline.a_bronze.ingest import read_files
from pipeline.a_bronze.validation import validate_columns, validate_nulls
from pipeline.a_bronze.upload import upload_to_bronze
from utils.logger import get_logger
from config import urls, expected_columns, S3_BUCKET_BRONZE
from pipeline.s3_client import initialize_s3_client, create_bucket_if_not_exists

# Initialize logger
logger = get_logger(__name__)


def source_to_bronze():
    """Main function to orchestrate the data pipeline with error handling."""
    
    logger.info("Starting Bronze layer data pipeline...")
    
    # Initialize AWS S3 client
    client = initialize_s3_client()

    try:
        # Step 1: Read raw files
        dataframes = read_files(urls)
        logger.info("Successfully ingested raw data.")

        # Step 2: Validate schema & nulls
        validate_columns(dataframes, expected_columns)
        validate_nulls(dataframes)
        logger.info("Data validation completed successfully.")

        # Step 3: Ensure Bronze bucket exists
        create_bucket_if_not_exists(client, S3_BUCKET_BRONZE)

        # Step 4: Upload to Bronze
        upload_to_bronze(client, dataframes)
        logger.info("Data successfully uploaded to Bronze S3 bucket.")

        logger.info("Bronze pipeline execution completed successfully!")
    
    except Exception as e:
        logger.error(f"Pipeline execution failed. Error: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    source_to_bronze()
