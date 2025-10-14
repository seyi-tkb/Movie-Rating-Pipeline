# import needed library
import os
from dotenv import load_dotenv
from datetime import datetime
import pytz

# Load the .env file
load_dotenv()

# dictionary of urls to read data
urls = {
    "movies": "https://drive.google.com/file/d/188tIKLJKek62rGmzj1Ylc03fe4Pgb5co/view?usp=drive_link",
    "ratings": "https://drive.google.com/file/d/1-3S-XOgZyo9D3sVoXtjPvmFdsihjfQhN/view?usp=drive_link",
    "users": "https://drive.google.com/file/d/1_wAww5beF2K7dpx-SU_gUUddNWeaeZqv/view?usp=drive_link"
}

# dictionary of file columns
expected_columns = {
    "movies": ['item_id', 'movie_title', 'release_date', 'IMDb_URL', 'primary_genre'],
    "ratings": ['user_id', 'item_id', 'rating', 'timestamp'],
    "users": ['user_id', 'age', 'gender', 'occupation', 'zip_code']
}

# date columns to process
date_columns = {
    "movies": "release_date",
    "ratings": "timestamp"
}

# AWS S3 configuration keys
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

S3_BUCKET_BRONZE = os.getenv("S3_BUCKET_BRONZE")
S3_BUCKET_SILVER = os.getenv("S3_BUCKET_SILVER")
S3_BUCKET_GOLD = os.getenv("S3_BUCKET_GOLD")
WATERMARKS_PATH = "watermarks/watermarks.csv"  # although JSON preferred over parquet for metadata

# Postgres configuration keys
POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "database": os.getenv("POSTGRES_DB", "movie_rating_database"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD")
}

# orchestration
pipeline_start = datetime(1997, 9, 28, tzinfo=pytz.UTC)

pipeline_schedule = {
    "frequency": "weekly",  # can be "daily", "weekly", etc.
    "day_of_week": "sunday"  # if weekly
}
