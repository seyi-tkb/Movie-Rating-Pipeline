import pytest
from moto import mock_aws
import boto3
import io
import pandas as pd
from unittest.mock import patch
from pipeline.b_silver.transform import (
    prepare_movie_df,
    prepare_users_df,
    prepare_ratings_df,
)


# test prepare_movie_df success
@pytest.mark.integration
@mock_aws
@patch("pipeline.b_silver.transform.initialize_s3_client")
def test_prepare_movie_df_success(mock_init_client, movie_data):
    """
    Integration test for prepare_movie_df() that reads movie data from the bronze bucket,
    transforms it, and writes the result to the silver bucket.

    Parameters:
        mock_init_client (MagicMock): Mocked S3 client initializer.
        movie_data (pytest Fixture): Sample data defined in conftest.py.
    """

    # setup fake s3
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="movie-pipeline-silver")
    s3.create_bucket(Bucket="movie-pipeline-bronze")

    s3.put_object(Bucket="movie-pipeline-bronze", Key="movies.csv", Body=movie_data)

    mock_init_client.return_value = s3

    with patch("config.S3_BUCKET_BRONZE", "movie-pipeline-bronze"), patch(
        "config.S3_BUCKET_SILVER", "movie-pipeline-silver"
    ), patch("config.WATERMARKS_PATH", "watermarks/watermarks.csv"):

        # Run
        pipeline_start = pd.Timestamp("2020-01-01", tz="UTC")
        kwargs = {"data_interval_end": pd.Timestamp("2020-12-31", tz="UTC")}
        prepare_movie_df(pipeline_start, **kwargs)

        # Load the CSV from mock S3 directly into a DataFrame
        response = s3.get_object(Bucket="movie-pipeline-silver", Key="movies.csv")
        df_result = pd.read_csv(io.BytesIO(response["Body"].read()))

        # Validate Resutt
        assert df_result.shape[0] == 5

        assert "Inception" in df_result["movie_title"].values
        assert "Animation" in df_result["primary_genre"].values
        assert "Action" not in df_result["primary_genre"].values


# test prepare_movie_df failure
@pytest.mark.integration
@mock_aws
@patch("pipeline.b_silver.transform.read_file")
@patch("pipeline.b_silver.transform.initialize_s3_client")
def test_prepare_movie_df_read_failure(mock_init_client, mock_read_file, caplog):
    """
    Integration test for prepare_movie_df() when reading from bronze fails.

    Args:
        mock_init_client (MagicMock): Mocked version of `initialize_s3_client`.
    """

    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="movie-pipeline-bronze")
    s3.create_bucket(Bucket="movie-pipeline-silver")

    mock_read_file.side_effect = Exception("Simulated read failure")
    mock_init_client.return_value = s3

    with patch("config.S3_BUCKET_BRONZE", "movie-pipeline-bronze"), patch(
        "config.S3_BUCKET_SILVER", "movie-pipeline-silver"
    ):

        # Run function
        with pytest.raises(Exception):
            pipeline_start = pd.Timestamp("2020-01-01", tz="UTC")
            kwargs = {"data_interval_end": pd.Timestamp("2020-12-31", tz="UTC")}
            prepare_movie_df(pipeline_start, **kwargs)

        # Validate log
        assert "prepare_movie_df failed: Simulated read failure" in caplog.text


# test prepare_users_df success
@pytest.mark.integration
@mock_aws
@patch("pipeline.b_silver.transform.initialize_s3_client")
def test_prepare_users_df_success(mock_init_client, user_data):
    """
    Integration test for prepare_users_df() that reads user data from the bronze bucket,
    transforms it, and writes the result to the silver bucket.

    Parameters:
        mock_init_client (MagicMock): Mocked S3 client initializer.
        user_data (pytest Fixture): Sample data defined in conftest.py.
    """

    # setup fake s3
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="movie-pipeline-silver")
    s3.create_bucket(Bucket="movie-pipeline-bronze")

    s3.put_object(Bucket="movie-pipeline-bronze", Key="users.csv", Body=user_data)

    mock_init_client.return_value = s3

    with patch("config.S3_BUCKET_BRONZE", "movie-pipeline-bronze"), patch(
        "config.S3_BUCKET_SILVER", "movie-pipeline-silver"
    ), patch("config.WATERMARKS_PATH", "watermarks/watermarks.csv"):

        # Run Transform
        prepare_users_df()

        # Load the CSV from mock S3 directly into a DataFrame
        response = s3.get_object(Bucket="movie-pipeline-silver", Key="users.csv")
        df_result = pd.read_csv(io.BytesIO(response["Body"].read()))

        # Validate Resutt
        assert df_result.shape[0] == 5

        assert "Doctor" in df_result["occupation"].values
        assert "teacher" not in df_result["occupation"].values


# test prepare_users_df failure
@pytest.mark.integration
@mock_aws
@patch("pipeline.b_silver.transform.write_to_silver")
@patch("pipeline.b_silver.transform.initialize_s3_client")
def test_prepare_movie_df_write_failure(
    mock_init_client, mock_write_file, user_data, caplog
):
    """
    Integration test for prepare_movie_df() when reading from bronze fails.

    Args:
        mock_init_client (MagicMock): Mocked version of `initialize_s3_client`.
        mock_write_file (MagicMock): Mocked version of `write_to_silver` to simulate failure.
        caplog (pytest.LogCaptureFixture): Captures log output for assertion.
    """

    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="movie-pipeline-bronze")
    s3.create_bucket(Bucket="movie-pipeline-silver")

    s3.put_object(Bucket="movie-pipeline-bronze", Key="users.csv", Body=user_data)

    mock_write_file.side_effect = Exception("Simulated write failure")
    mock_init_client.return_value = s3

    with patch("config.S3_BUCKET_BRONZE", "movie-pipeline-bronze"), patch(
        "config.S3_BUCKET_SILVER", "movie-pipeline-silver"
    ):

        # Run function
        with pytest.raises(Exception):
            prepare_users_df()

        # Validate log
        assert "Simulated write failure" in caplog.text


# test prepare_ratings_df success
@pytest.mark.integration
@mock_aws
@patch("pipeline.b_silver.transform.initialize_s3_client")
def test_prepare_ratings_df_success(mock_init_client, rating_data):
    """
    Integration test for prepare_movie_df() that reads movie data from the bronze bucket,
    transforms it, and writes the result to the silver bucket.

    Parameters:
        mock_init_client (MagicMock): Mocked S3 client initializer.
        movie_data (pytest Fixture): Sample data defined in conftest.py.
    """

    # setup fake s3
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="movie-pipeline-silver")
    s3.create_bucket(Bucket="movie-pipeline-bronze")

    s3.put_object(Bucket="movie-pipeline-bronze", Key="ratings.csv", Body=rating_data)

    mock_init_client.return_value = s3

    with patch("config.S3_BUCKET_BRONZE", "movie-pipeline-bronze"), patch(
        "config.S3_BUCKET_SILVER", "movie-pipeline-silver"
    ), patch("config.WATERMARKS_PATH", "watermarks/watermarks.csv"):

        # Run
        pipeline_start = pd.Timestamp("2000-01-01", tz="UTC")
        kwargs = {
            "data_interval_start": pd.Timestamp("1999-12-25", tz="UTC"),
            "data_interval_end": pd.Timestamp("2000-01-01", tz="UTC"),
            "logical_date": pd.Timestamp("2000-01-01", tz="UTC"),
        }

        prepare_ratings_df(pipeline_start, **kwargs)

        # Load the CSV from mock S3 directly into a DataFrame
        response = s3.get_object(
            Bucket="movie-pipeline-silver", Key="ratings/2000-01.csv"
        )
        df_result = pd.read_csv(io.BytesIO(response["Body"].read()))

        # Validate Resutt
        assert not df_result.empty
        assert df_result.shape[0] == 5

        assert 101 in df_result["item_id"].values
        assert 3 in df_result["user_id"].values
        assert 6 not in df_result["user_id"].values


# test prepare_ratings_df for failure path
@pytest.mark.integration
@patch("pipeline.b_silver.transform.update_watermarks")
@patch("pipeline.b_silver.transform.initialize_s3_client")
def test_prepare_ratings_df_write_failure(
    mock_init_client, mock_watermarks, rating_data, s3_mock_env, caplog
):
    """
    Integration test for prepare_movie_df() when reading from bronze fails.

    Args:
        mock_init_client (MagicMock): Mocked version of `initialize_s3_client`.
        mock_write_file (MagicMock): Mocked version of `write_to_silver` to simulate failure.
        caplog (pytest.LogCaptureFixture): Captures log output for assertion.
    """

    # Setup mock s3
    s3 = s3_mock_env  # advance generator to get actual client
    s3.put_object(Bucket="movie-pipeline-bronze", Key="ratings.csv", Body=rating_data)

    mock_watermarks.side_effect = Exception("Simulated update_watermark failure")
    mock_init_client.return_value = s3

    with patch("config.S3_BUCKET_BRONZE", "movie-pipeline-bronze"), patch(
        "config.S3_BUCKET_SILVER", "movie-pipeline-silver"
    ):

        # Run function
        with pytest.raises(Exception):
            pipeline_start = pd.Timestamp("2020-01-01", tz="UTC")
            kwargs = {
                "data_interval_start": pd.Timestamp("1999-12-25", tz="UTC"),
                "data_interval_end": pd.Timestamp("2000-01-01", tz="UTC"),
                "logical_date": pd.Timestamp("2000-01-01", tz="UTC"),
            }
            prepare_ratings_df(pipeline_start, **kwargs)

        # Validate log
        assert "Simulated update_watermark failure" in caplog.text
