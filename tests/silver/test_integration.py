import pytest
from moto import mock_aws
import boto3
import pandas as pd
from unittest.mock import patch
from pipeline.b_silver.transform import prepare_movie_df


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
        pipeline_start = pd.Timestamp("2020-01-01", tz="UTC")
        kwargs = {"data_interval_start": pd.Timestamp("2020-12-31", tz="UTC")}
        prepare_movie_df(pipeline_start, **kwargs)

        # Verify
        response = s3.get_object(Bucket="movie-pipeline-silver", Key="movies.csv")
        content = response["Body"].read().decode("utf-8")
        assert "Inception" in content
        assert "Animation" in content
        assert "Action" not in content

