import pytest
import boto3
from moto import mock_aws

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
@mock_aws
def s3_mock_env():
    """Creates a fake S3 environment with both Bronze and Silver buckets."""
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="movie-pipeline-silver")
    s3.create_bucket(Bucket="movie-pipeline-bronze")
    yield s3  # provides it to the test
