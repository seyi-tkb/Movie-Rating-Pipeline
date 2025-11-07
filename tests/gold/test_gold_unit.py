import pandas as pd
import pytest
from io import BytesIO
from pipeline.s_gold.silver_watermarks import update_silver_watermarks
from pipeline.s_gold.read_bucket import read_silver_file
from sqlalchemy.sql import text
from pipeline.s_gold.connection import get_db_connection, execute_sql, write_to_postgres

from config import S3_BUCKET_SILVER

from botocore.exceptions import ClientError

from unittest.mock import patch, MagicMock, ANY

from moto import mock_aws
import boto3


# -------------
# read_silver_file()
# ------------
# success path
@pytest.mark.unit
@patch("pipeline.s_gold.read_bucket.initialize_s3_client")
def test_read_silver_file_success(mock_init_client, sample_bytes_data):
    """
    Test that `read_silver_file()` successfully reads from Silver bucket.
    """

    # Mock
    mock_client = MagicMock()
    mock_client.get_object.return_value = {"Body": BytesIO(sample_bytes_data)}
    mock_init_client.return_value = mock_client

    # Run Function
    df = read_silver_file("silver/test.csv")

    # Validate call
    mock_init_client.assert_called_once()
    mock_client.get_object.assert_called_once_with(
        Bucket="movie-pipeline-silver", Key="silver/test.csv"
    )

    # Validate result
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (2, 3)
    assert list(df.columns) == ["user_id", "rating", "movie_id"]
    assert df["rating"].iloc[0] == 5


# failure path
@pytest.mark.unit
@patch("pipeline.s_gold.read_bucket.initialize_s3_client")
def test_read_silver_file_success(mock_init_client, caplog):
    """
    Test that `read_silver_file()` successfully reads from Silver bucket.
    """

    # Mock
    mock_client = MagicMock()
    mock_init_client.return_value = mock_client
    mock_client.get_object.side_effect = Exception("Error Type")

    # Run Function
    with pytest.raises(Exception, match="Error Type"):
        read_silver_file("silver/bad_test.csv")

    # Assert that the error message was logged
    assert "Error Type" in caplog.text

    # Validate call
    mock_init_client.assert_called_once()
    mock_client.get_object.assert_called_once_with(
        Bucket="movie-pipeline-silver", Key="silver/bad_test.csv"
    )


# -------------
# get_db_connection
# ------------


@pytest.mark.unit
@patch(
    "pipeline.s_gold.connection.POSTGRES_CONFIG",
    {
        "user": "test_user",
        "password": "test_pass",
        "host": "localhost",
        "port": "5432",
        "database": "test_db",
    },
)
def test_get_db_connection_success():
    """ """
    # Run Function
    engine = get_db_connection()

    # Validate
    assert engine is not None
    assert "postgresql" in str(engine.url)


# -------------
# execute_sql
# ------------


# success path
@pytest.mark.unit
@patch("pipeline.s_gold.connection.get_db_connection")
def test_execute_sql_success(mock_get_conn, caplog):
    """Test that execute_sql runs successfully and logs confirmation."""

    # Mocks
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.begin.return_value.__enter__.return_value = mock_conn
    mock_get_conn.return_value = mock_engine

    # Sample text
    sql = "CREATE TABLE IF NOT EXISTS test_table (id INT);"
    text_name = "test_table"

    # Run Function
    with caplog.at_level("INFO"):
        execute_sql(sql, text_name)

    # Validate
    mock_conn.execute.assert_called_once_with(ANY)
    assert str(mock_conn.execute.call_args[0][0]) == sql
    assert f"Ensured '{text_name}' in Postgres." in caplog.text


# failure path
@pytest.mark.unit
@patch("pipeline.s_gold.connection.get_db_connection")
def test_execute_sql_failure(mock_get_conn, caplog):
    """Test that execute_sql logs and raises error when execution fails."""

    # Simulate engine.begin throwing an error
    mock_engine = MagicMock()
    mock_engine.begin.side_effect = Exception("Simulated failure")
    mock_get_conn.return_value = mock_engine

    # Sample text
    sql = "CREATE TABLE broken_table (id INT);"
    text_name = "broken_table"

    # Run Function
    with caplog.at_level("ERROR"):
        with pytest.raises(Exception, match="Simulated failure"):
            execute_sql(sql, text_name)

    # Validate
    assert f"Error creating table '{text_name}': Simulated failure" in caplog.text


import logging


# -------------
# write_to_postgres
# ------------


@pytest.mark.unit
@patch("pipeline.s_gold.connection.get_db_connection")
def test_write_to_postgres_success(mock_get_engine, sample_df, caplog):
    """✅ Test successful write with mocked engine"""

    caplog.set_level(logging.INFO)  # ensure INFO level is captured here

    # Mock db connection
    mock_engine = MagicMock()
    mock_get_engine.return_value = mock_engine

    with patch.object(sample_df, "to_sql") as mock_to_sql:
        write_to_postgres(sample_df, "test_table")

    mock_to_sql.assert_called_once_with(
        name="test_table",
        schema="stg",
        con=mock_engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )
    assert "Successfully inserted" in caplog.text
