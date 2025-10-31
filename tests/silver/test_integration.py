import pytest
from moto import mock_aws
import boto3
import pandas as pd
from unittest.mock import patch
from pipeline.b_silver.transform import prepare_movie_df
from config import S3_BUCKET_SILVER, WATERMARKS_PATH



movie_data = (
    "item_id,movie_title,release_date,IMDb_URL,primary_genre\n"
    "1,The Matrix,1999-03-31,https://www.imdb.com/title/tt0133093/,Sci-Fi\n"
    "2,Titanic,1997-12-19,https://www.imdb.com/title/tt0120338/,Romance\n"
    "3,Inception,2010-07-16,https://www.imdb.com/title/tt1375666/,Thriller\n"
    "4,The Godfather,1972-03-24,https://www.imdb.com/title/tt0068646/,Crime\n"
    "5,Toy Story,1995-11-22,https://www.imdb.com/title/tt0114709/,Animation\n"
    "6,NaN,2018-02-16,https://www.imdb.com/title/tt1825683/,Action\n"       # has missing title
)

# test_movie_flow
@pytest.mark.integration
@mock_aws
@patch("pipeline.b_silver.transform.initialize_s3_client")
def test_prepare_movie_df_integration(mock_init_client, movie_data):
    """
    Integration test for prepare_movie_df() that reads movie data from the bronze bucket,
    transforms it, and writes the result to the silver bucket.

    Parameters:
        mock_init_client (MagicMock): Mocked S3 client initializer.
    """
    
    # setup fake s3
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="movie-pipeline-silver")
    s3.create_bucket(Bucket="movie-pipeline-bronze")

    s3.put_object(Bucket="movie-pipeline-bronze",
                  Key= "movies.csv",
                  Body= movie_data)
    
    mock_init_client.return_value = s3
    
    with patch("config.S3_BUCKET_BRONZE", "movie-pipeline-bronze"), \
         patch("config.S3_BUCKET_SILVER", "movie-pipeline-silver"), \
         patch("config.WATERMARKS_PATH", "watermarks/watermarks.csv"):

        # Run
        pipeline_start = pd.Timestamp("2011-01-01", tz="UTC")
        kwargs = {"data_interval_start": pd.Timestamp("2020-12-31", tz="UTC")}
        prepare_movie_df(pipeline_start, **kwargs)

        # Verify
        response = s3.get_object(Bucket="movie-pipeline-silver", Key="movies.csv")
        content = response["Body"].read().decode("utf-8")
        assert "Inception" in content
        assert "Animation" in content
        assert "Action" not in content

