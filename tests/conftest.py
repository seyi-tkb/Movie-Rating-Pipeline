import pytest
import boto3
from moto import mock_aws
import pandas as pd

# -------------
# Silver Fixtures
# ------------

@pytest.fixture
def movie_data():
    """Simulated Bronze CSV movie dataset."""
    return (
        "item_id,movie_title,release_date,IMDb_URL,primary_genre\n"
        "1,The Matrix,1999-03-31,https://www.imdb.com/title/tt0133093/,Sci-Fi\n"
        "2,Titanic,1997-12-19,https://www.imdb.com/title/tt0120338/,Romance\n"
        "3,Inception,2010-07-16,https://www.imdb.com/title/tt1375666/,Thriller\n"
        "4,The Godfather,1972-03-24,https://www.imdb.com/title/tt0068646/,Crime\n"
        "5,Toy Story,1995-11-22,https://www.imdb.com/title/tt0114709/,Animation\n"
        "6,NaN,2018-02-16,https://www.imdb.com/title/tt1825683/,Action\n"
    )

@pytest.fixture
def user_data():
    """Simulated Bronze CSV user data."""
    return (
        "user_id,age,gender,occupation,zip_code\n"
        "1,24,M,student,10001\n"
        "2,35,F,engineer,94105\n"
        "3,42,M,artist,60614\n"
        "4,29,F,doctor,30303\n"
        "5,51,M,retired,75201\n"
        ",38,F,teacher,02139\n"
    )

@pytest.fixture
def rating_data():
    return (
        "user_id,item_id,rating,timestamp\n"
        "1,101,4.0,946598400\n"  # 1999-12-25 00:00:00
        "2,102,5.0,946624800\n"  # 1999-12-25 07:00:00
        "3,103,3.0,946651200\n"  # 1999-12-25 14:00:00
        "4,104,2.0,946677600\n"  # 1999-12-25 21:00:00
        "5,105,4.5,946660800\n"  # 1999-12-25 16:00:00
        "6,,3.5,946670000\n"    # Null data
    )


@pytest.fixture
def s3_mock_env():
    """Creates a fake S3 environment with both Bronze and Silver buckets."""
    with mock_aws(): #instad of patching so as not to produce a generator
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="movie-pipeline-silver")
        s3.create_bucket(Bucket="movie-pipeline-bronze")
        yield s3


# -------------
# Gold Fixtures
# ------------

@pytest.fixture
def sample_bytes_data():
    csv_data = (
        "user_id,rating,movie_id\n"
        "1,5,42\n"
        "2,4,37\n"
    )
    return csv_data.encode("utf-8") # returns bytes


@pytest.fixture
def sample_df():
    df = pd.DataFrame({
        "user_id": [1, 2],
        "rating": [5.0, 4.0],
        "movie_id": [42, 37]
    })
    return df


from datetime import datetime

import pytest

@pytest.fixture
def sample_watermarks():
    """Fixture: 
    """
    return (
        "dataset_name,max_value,records_loaded,processing_time\n"
        "ratings,2000-11-01T00:00:00,2000,2025-11-04T06:30:00\n"
        "users,10023,1500,2025-11-04T06:31:00\n"
        "movies,2003-10-31T00:00:00,1800,2025-11-04T06:32:00\n"
    )


@pytest.fixture
def sample_watermarks_n():
    """Fixture: Plain CSV string for watermark data with historical ratings max_value."""
    return (
        "dataset_name,max_value,records_loaded,processing_time\n"
        "ratings,1999-12-16T12:00:00,100,1999-12-25T12:30:00\n"
        "users,10023,1500,2025-11-04T06:31:00\n"
        "movies,2003-10-31T00:00:00,1800,2025-11-04T06:32:00\n"
    )