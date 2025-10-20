from utils.logger import get_logger
from pipeline.b_silver.transform import (
    prepare_movie_df,
    prepare_ratings_df,
    prepare_users_df,
)

# need

# initializing logger
logger = get_logger(__name__)


def orchestrate():
    logger.info("Starting bronze-silver orchestration...")

    try:
        prepare_movie_df()
    except Exception as e:
        logger.error(f"Movie pipeline failed: {e}")

    try:
        prepare_ratings_df()
    except Exception as e:
        logger.error(f"Ratings pipeline failed: {e}")

    try:
        prepare_users_df()
    except Exception as e:
        logger.error(f"Users pipeline failed: {e}")

    logger.info("MovieLens ETL Orchestration Complete.")


if __name__ == "__main__":
    orchestrate()
