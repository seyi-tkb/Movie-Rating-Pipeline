from datetime import datetime
from datetime import datetime, timedelta
import pytz
from pipeline.s3_client import initialize_s3_client
from config import S3_BUCKET_BRONZE, S3_BUCKET_SILVER, S3_BUCKET_GOLD


# parent check function
def check_s3_file_freshness(bucket_name, key, fresh_period=7):
    client = initialize_s3_client()
    response = client.head_object(Bucket=bucket_name, Key=key)
    last_modified = response["LastModified"]

    # Convert to local timezone if needed
    now = datetime.now(pytz.utc)
    freshness_threshold = now - timedelta(days=fresh_period)

    if last_modified >= freshness_threshold:
        print(f"{key} is fresh: {last_modified}")
        return True
    else:
        print(f"{key} is stale: {last_modified}")
        return False


def check_ratings_file():
    return check_s3_file_freshness(bucket_name=S3_BUCKET_BRONZE, key="ratings.csv")


def check_movies_file():
    return check_s3_file_freshness(bucket_name=S3_BUCKET_BRONZE, key="movies.csv")


def check_users_file():
    return check_s3_file_freshness(bucket_name=S3_BUCKET_BRONZE, key="users.csv")
