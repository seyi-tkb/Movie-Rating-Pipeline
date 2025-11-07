import pytest
from moto import mock_aws
import boto3
import io
import pandas as pd
from unittest.mock import patch, MagicMock
from pipeline.s_gold.silver_watermarks import update_silver_watermarks
from pipeline.s_gold.load import load_movie_df, load_ratings_df, load_users_df
from config import S3_BUCKET_SILVER, WATERMARKS_PATH
from pipeline.s_gold.sql.schema import create_ratings_partition, upsert_ratings


# test silver_watermarks success
@pytest.mark.integration
@mock_aws
@patch("pipeline.s_gold.silver_watermarks.initialize_s3_client")
def test_silver_watermarks_success(mock_init_client, sample_watermarks):
    """
    Integration test for update_silver_watermarks() that reads existing watermarks from S3,
    appends a new one, and writes the result back to S3.

    Parameters:
        mock_init_client (MagicMock): Mocked S3 client initializer.
        sample_watermarks (str): CSV string defined in conftest.py.
    """

    # setup fake s3
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="movie-pipeline-silver")

    s3.put_object(
        Bucket="movie-pipeline-silver",
        Key="watermarks/watermarks.csv",
        Body=sample_watermarks,
    )

    mock_init_client.return_value = s3

    with patch("config.S3_BUCKET_SILVER", "movie-pipeline-silver"), patch(
        "config.WATERMARKS_PATH", "watermarks/watermarks.csv"
    ):

        new_data = {
            "dataset_name": "ratings",
            "max_value": "2025-11-01T00:00:00",
            "records_loaded": 2000,
            "processing_time": "2025-11-04T06:30:00",
        }

        update_silver_watermarks(new_data)

        # Load the CSV from mock S3 directly into a DataFrame to validate
        response = s3.get_object(
            Bucket="movie-pipeline-silver", Key="watermarks/watermarks.csv"
        )
        df_result = pd.read_csv(io.BytesIO(response["Body"].read()))

        # Validate Resutt
        assert df_result.shape[0] == 4

        assert "ratings" in df_result["dataset_name"].values
        assert "users" in df_result["dataset_name"].values
        assert "2025-11-01T00:00:00" in df_result["max_value"].values


# -------------
# load_movie_df
# ------------


# Success path
@pytest.mark.integration
@patch("pipeline.s_gold.load.update_silver_watermarks")
@patch("pipeline.s_gold.load.get_db_connection")
@patch("pipeline.s_gold.load.write_to_postgres")
@patch("pipeline.s_gold.load.read_silver_file")
def test_load_movie_df_success(
    mock_read_silver_file,
    mock_write_to_postgres,
    mock_get_conn,
    mock_update_watermarks,
    movie_data,
):
    """
    Integration test for load_movie_df: reads, stages, upserts, and updates watermark.

    Parameters:

    """

    # Mock
    mock_read_silver_file.return_value = pd.read_csv(io.StringIO(movie_data))

    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_get_conn.return_value = mock_engine
    # mock_engine.begin. = mock_engine.begin.return_value
    # `with` calls an "__enter__" since it's a context manager so we factor that in.
    # with engine.begin as conn
    mock_engine.begin.return_value.__enter__.return_value = mock_conn

    # Run the function
    load_movie_df()

    # Assert Postgres staging write called correctly
    mock_write_to_postgres.assert_called_once()
    args, _ = mock_write_to_postgres.call_args
    assert isinstance(args[0], pd.DataFrame)
    assert args[1] == "stg_movies"

    # Assert upsert was executed
    mock_conn.execute.assert_called_once()

    # Assert watermark update
    mock_update_watermarks.assert_called_once()
    args, _ = mock_update_watermarks.call_args
    watermark_data = args[0]
    assert watermark_data["dataset_name"] == "movies"
    assert watermark_data["records_loaded"] == 6
    assert isinstance(watermark_data["processing_time"], pd.Timestamp)


# Failure path
@pytest.mark.integration
@patch("pipeline.s_gold.load.read_silver_file")
def test_prepare_movie_df_failure(mock_read_file, caplog):
    """
    Integration test for prepare_movie_df() when reading from bronze fails.

    Args:
        mock_init_client (MagicMock): Mocked version of `initialize_s3_client`.
        mock_write_file (MagicMock): Mocked version of `write_to_silver` to simulate failure.
        caplog (pytest.LogCaptureFixture): Captures log output for assertion.
    """

    # Mock
    mock_read_file.side_effect = Exception("Simulated read failure")

    # Run function
    with pytest.raises(Exception):
        load_movie_df()

    # Validate log
    assert "Simulated read failure" in caplog.text


# -------------
# load_user_df
# ------------


# Success path
@pytest.mark.integration
@patch("pipeline.s_gold.load.update_silver_watermarks")
@patch("pipeline.s_gold.load.get_db_connection")
@patch("pipeline.s_gold.load.write_to_postgres")
@patch("pipeline.s_gold.load.read_silver_file")
def test_load_user_success(
    mock_read_silver_file,
    mock_write_to_postgres,
    mock_get_conn,
    mock_update_watermarks,
    user_data,
):
    """
    Integration test for load_movie_df: reads, stages, upserts, and updates watermark.

    Parameters:

    """

    # Mock
    mock_read_silver_file.return_value = pd.read_csv(io.StringIO(user_data))

    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_get_conn.return_value = mock_engine
    # mock_engine.begin. = mock_engine.begin.return_value
    # `with` calls an "__enter__" since it's a context manager so we factor that in.
    # with engine.begin as conn
    mock_engine.begin.return_value.__enter__.return_value = mock_conn

    # Run the function
    load_users_df()

    # Assert Postgres staging write called correctly
    mock_write_to_postgres.assert_called_once()
    args, _ = mock_write_to_postgres.call_args
    assert isinstance(args[0], pd.DataFrame)
    assert args[1] == "stg_users"

    # Assert upsert was executed
    mock_conn.execute.assert_called_once()

    # Assert watermark update
    mock_update_watermarks.assert_called_once()
    args, _ = mock_update_watermarks.call_args
    watermark_data = args[0]
    assert watermark_data["dataset_name"] == "users"
    assert watermark_data["records_loaded"] == 6
    assert isinstance(watermark_data["processing_time"], pd.Timestamp)


# Failure path
@pytest.mark.integration
@patch("pipeline.s_gold.load.write_to_postgres")
@patch("pipeline.s_gold.load.read_silver_file")
def test_load_user_failure(
    mock_read_silver_file, mock_write_to_postgres, user_data, caplog
):
    """
    Integration test for load_movie_df: reads, stages, upserts, and updates watermark.

    Parameters:

    """

    # Mock
    mock_read_silver_file.return_value = pd.read_csv(io.StringIO(user_data))
    mock_write_to_postgres.side_effect = Exception("Simulated failure")

    # Run function
    with pytest.raises(Exception):
        load_users_df()

    # Validate
    assert "Simulated failure" in caplog.text


# -------------
# load_ratings_df
# ------------


# Success path
@pytest.mark.integration
@patch("pipeline.s_gold.load.update_silver_watermarks")
@patch("pipeline.s_gold.load.execute_sql")
@patch("pipeline.s_gold.load.write_to_postgres")
@patch("pipeline.s_gold.load.read_silver_watermarks")
@patch("pipeline.s_gold.load.read_silver_file")
def test_load_rating_df_success(
    mock_read_silver_file,
    mock_read_watermarks,
    mock_write_to_postgres,
    mock_execute_sql,
    mock_update_watermarks,
    rating_data,
    sample_watermarks_n,
):
    """
    Integration test for load_ratings_df

    Parameters:
        mock_read_silver_file (MagicMock): Mocked function to simulate reading monthly ratings partitions from the silver layer.
        mock_read_watermarks (MagicMock): Mocked function to simulate reading watermark metadata.
        mock_write_to_postgres (MagicMock): Mocked function to simulate writing filtered ratings data to the staging table.
        mock_execute_sql (MagicMock): Mocked function to simulate SQL execution for partition creation and upsert.
        mock_update_watermarks (MagicMock): Mocked function to simulate updating watermark metadata.
        rating_data (pytest Fixture): Sample ratings data as a CSV string, defined in conftest.py.
        sample_watermarks_n (pytest Fixture): Sample watermark metadata as a CSV string, defined in conftest.py.
    """
    # Parse data into DataFrames
    df = pd.read_csv(io.StringIO(rating_data))
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")

    # Mock functions
    mock_read_silver_file.return_value = df
    mock_read_watermarks.return_value = pd.read_csv(io.StringIO(sample_watermarks_n))

    # Run function under test
    pipeline_start = pd.Timestamp("2000-01-01", tz="UTC")
    kwargs = {
        "data_interval_start": pd.Timestamp("1999-12-20", tz="UTC"),
        "data_interval_end": pd.Timestamp("1999-12-27", tz="UTC"),
    }

    load_ratings_df(pipeline_start, **kwargs)

    # Expectations to assert against
    expected_filtered = df[df["timestamp"] > pd.Timestamp("1999-12-16T12:00:00")]

    # Validate write was called with correct argumenst
    mock_write_to_postgres.assert_called_once()
    args, _ = mock_write_to_postgres.call_args
    written_df = args[0]
    assert args[1] == "stg_ratings"
    assert written_df.shape[0] == expected_filtered.shape[0]

    # Validate partition creation and upsert were triggered
    assert mock_execute_sql.call_count == 2
    mock_execute_sql.assert_any_call(create_ratings_partition, "ratings_partitions")
    mock_execute_sql.assert_any_call(upsert_ratings, "upsert ratings")

    # Validate watermark update with correct metadata
    mock_update_watermarks.assert_called_once()
    args, _ = mock_update_watermarks.call_args
    watermark_data = args[0]
    assert watermark_data["dataset_name"] == "ratings"
    assert watermark_data["records_loaded"] == expected_filtered.shape[0]
    assert isinstance(watermark_data["max_value"], pd.Timestamp)
    assert isinstance(watermark_data["processing_time"], pd.Timestamp)


# Failure path
@pytest.mark.integration
@patch("pipeline.s_gold.load.update_silver_watermarks")
@patch("pipeline.s_gold.load.execute_sql")
@patch("pipeline.s_gold.load.write_to_postgres")
@patch("pipeline.s_gold.load.read_silver_watermarks")
@patch("pipeline.s_gold.load.read_silver_file")
def test_load_ratings_df_failure(
    mock_read_silver_file,
    mock_read_watermarks,
    mock_write_to_postgres,
    mock_execute_sql,
    mock_update_watermarks,
    rating_data,
    sample_watermarks_n,
    caplog,
):
    """
    Integration test for load_ratings_df

    Parameters:

        mock_read_silver_file (MagicMock): Mocked function to simulate reading monthly ratings partitions from the silver layer.
        mock_read_watermarks (MagicMock): Mocked function to simulate reading watermark metadata.
        mock_write_to_postgres (MagicMock): Mocked function that raises an exception to simulate a write failure.
        mock_execute_sql (MagicMock): Mocked function to simulate SQL execution for partition creation and upsert.
        mock_update_watermarks (MagicMock): Mocked function to simulate updating watermark metadata.
        rating_data (pytest Fixture): Sample ratings data as a CSV string, defined in conftest.py.
        sample_watermarks_n (pytest Fixture): Sample watermark metadata as a CSV string, defined in conftest.py.
        caplog (pytest.LogCaptureFixture): Captures log output for assertion.
    """

    # Parse rating_data into DataFrame
    ratings_df = pd.read_csv(io.StringIO(rating_data))
    ratings_df["timestamp"] = pd.to_datetime(ratings_df["timestamp"], unit="s")
    mock_read_silver_file.return_value = ratings_df

    # Parse watermark CSV into DataFrame
    watermark_df = pd.read_csv(io.StringIO(sample_watermarks_n))
    mock_read_watermarks.return_value = watermark_df

    # Simulate failure in write_to_postgres
    mock_write_to_postgres.side_effect = Exception("Simulated write failure")

    # Run function under test
    with pytest.raises(Exception):
        pipeline_start = pd.Timestamp("2000-01-01", tz="UTC")
        kwargs = {
            "data_interval_start": pd.Timestamp("1999-12-20", tz="UTC"),
            "data_interval_end": pd.Timestamp("1999-12-27", tz="UTC"),
        }

        load_ratings_df(pipeline_start, **kwargs)

    # Validate
    assert "Simulated write failure" in caplog.text

    mock_execute_sql.assert_not_called()
    mock_update_watermarks.assert_not_called()
