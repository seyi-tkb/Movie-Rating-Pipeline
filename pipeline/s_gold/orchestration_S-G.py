from pipeline.s_gold.load import (
    load_users_df,
    load_ratings_df,
    load_movie_df,
    check_tables,
)
from utils.logger import get_logger

logger = get_logger(__name__)


def silver_to_gold():
    """
    Orchestrates silver to gold pipe
    """
    logger.info("Starting silver-gold orchestration...")

    try:
        logger.info("Creating tables if not exist..")
        check_tables()

        logger.info("Processing users data..")
        load_users_df()

        logger.info("Processing movies data..")
        load_movie_df()

        logger.info("Processing ratings data..")
        load_ratings_df()

        logger.info("All datasets processed successfully.")

    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}", exc_info=True)


if __name__ == "__main__":
    silver_to_gold()
