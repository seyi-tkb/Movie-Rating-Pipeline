# General
import pandas as pd
import pytest
from io import BytesIO
from pipeline.b_silver.read_write_buckets import read_file, write_to_silver
from pipeline.b_silver.watermarks import read_watermarks
from config import S3_BUCKET_SILVER

from botocore.exceptions import ClientError



from unittest.mock import patch, MagicMock, ANY

from moto import mock_aws
import boto3


#-------------
# read_file()
# ------------

@pytest.mark.unit
@patch("pipeline.b_silver.read_write_buckets.initialize_s3_client")
def test_read_file_success(mock_init_client):
    """
    Test that `read_file()` successfully reads csv files.
    """

    # mock
    mock_client = MagicMock()
    mock_client.get_object.return_value = {
        "Body": BytesIO(b"user_id,rating,movie_id\n1,5,42\n2,4,37\n")
    }
    mock_init_client.return_value = mock_client

    # Run 
    df = read_file("test_bucket", "bronze/test.csv")

    # Validate call
    mock_init_client.assert_called_once()
    mock_client.get_object.assert_called_once_with(Bucket="test_bucket", Key="bronze/test.csv")
    
    # Validate result
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (2, 3)
    assert list(df.columns) == ["user_id", "rating", "movie_id"]
    assert df["rating"].iloc[0] == 5

# test read_file failure
@pytest.mark.unit
@patch("pipeline.b_silver.read_write_buckets.initialize_s3_client")
def test_read_file_failure(mock_init_client, caplog):
    """
    Test that `read_file()` raises when S3 read fails.
    """

    # Mock
    mock_client = MagicMock()
    mock_client.get_object.side_effect = Exception("Error Type")
    mock_init_client.return_value = mock_client

    # Run
    with pytest.raises(Exception, match = "Error Type"):
        read_file("test_bucket", "bronze/bad_test.csv")

    # Assert that the error message was logged
    assert "Error Type" in caplog.text

    # Validate call
    mock_init_client.assert_called_once()
    mock_client.get_object.assert_called_once_with(
        Bucket="test_bucket", Key="bronze/bad_test.csv"
    )

#------------------
# write_to_silver()
# -----------------

# Test write_to_silver success
@pytest.mark.unit
@patch("pipeline.b_silver.read_write_buckets.initialize_s3_client")
def test_write_to_silver_success(mock_init_client):
    """
    Test that `write_to_silver()` successfully uploads a DataFrame as a csv to S3.

    Parameters:
        mock_init_client (MagicMock): Mocked version of the S3 client 
            initializer, used to prevent real AWS interactions during testing.
            \n Automatically provided by defined @patch.
    """

    # Mock
    mock_client = MagicMock()
    mock_init_client.return_value = mock_client

    # Sample DataFrame
    df = pd.DataFrame({
        "user_id": [1, 2],
        "rating": [5, 4],
        "movie_id": [42, 37]})

    # Run
    write_to_silver(df, "test.csv")

    # Validate client initialization and call
    mock_init_client.assert_called_once()
    mock_client.put_object.assert_called_once()

    # Validate S3 put_object was called with correct paramameters
    args, kwargs = mock_client.put_object.call_args     # retrieves params
    assert kwargs["Bucket"] == S3_BUCKET_SILVER
    assert kwargs["Key"] == "test.csv"
    assert isinstance(kwargs["Body"], bytes)
    assert kwargs["ContentType"] == "application/csv"

    # Validate CSV content is correct 
    csv_content = kwargs["Body"].decode("utf-8")
    assert "user_id" in csv_content
    assert "movie_id" in csv_content
    assert "rating" in csv_content

# Test write_to_silver failure
@pytest.mark.unit
@patch("pipeline.b_silver.read_write_buckets.initialize_s3_client")
def test_write_to_silver_failure(mock_init_client, caplog):
    """
    Test that `write_to_silver()` raises if S3 upload fails.

    Parameters:
        mock_init_client (MagicMock): Mocked version of `initialize_s3_client`, 
            used to prevent real AWS interactions during testing.
            \n Automatically provided by defined @patch.
        
        caplog (pytest.LogCaptureFixture): Fixture used to capture
            log messages emitted during the test.\n 
            Automatically injected by pytest (not patched manually).
    """

    # Mocks
    mock_client = MagicMock()
    mock_client.put_object.side_effect = Exception("Error Type")
    mock_init_client.return_value = mock_client

    # Sample DataFrame
    df = pd.DataFrame({
        "user_id": [1, 2],
        "rating": [5, 4],
        "movie_id": [42, 37]})

    # Run function
    with pytest.raises(Exception, match = "Error Type"):
        write_to_silver(df, "bad_test.csv")

    # Assert that the error message was logged
    assert "Error writing bad_test.csv to Silver bucket: Error Type" in caplog.text

    # Validate call and params
    mock_client.put_object.assert_called_once_with(
        Bucket=S3_BUCKET_SILVER,
        Key="bad_test.csv",
        Body=ANY,
        ContentType="application/csv")

# integration for read & write_to_silver? mock aws?

# ---------------
# read_watermarks
# ---------------

# read_watermarks success
@pytest.mark.unit
@patch("pipeline.b_silver.watermarks.initialize_s3_client")
def test_read_watermarks_success(mock_init_client):
    """
    Unit test for `read_watermarks()` when S3 returns valid CSV data.

    Parameters:
        mock_init_client (MagicMock): Mocked version of initialize_s3_client.
    """

    # Sample Data
    csv_data = (
    "dataset_name,max_value,records_loaded,processing_time\n"
    "users,100,50,2025-10-09\n"
    )

    # Mock
    mock_client = MagicMock()
    mock_client.get_object.return_value = {"Body": BytesIO(csv_data.encode("utf-8"))}
    mock_init_client.return_value = mock_client

    # Run
    df = read_watermarks()

    # Validate
    mock_init_client.assert_called_once()
    mock_client.get_object.assert_called_once()

    # Validate result
    assert not df.empty
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (1, 4)
    assert list(df.columns) == ["dataset_name", "max_value", "records_loaded", "processing_time"]
    assert df.loc[0, "dataset_name"] == "users"


# read_watermarks failure
@pytest.mark.unit
@patch("pipeline.b_silver.watermarks.initialize_s3_client")
def test_read_watermarks_failure(mock_init_client, caplog):
    """
    Unit test for `read_watermarks()` when S3 client raises an exception.

    Parameters:
        mock_init_client (MagicMock): Mocked version of `initialize_s3_client`.
        caplog (pytest.LogCaptureFixture): Captures log output during test execution.
    """

    # Mock
    mock_client = MagicMock()
    mock_init_client.return_value = mock_client
    mock_client.get_object.side_effect = ClientError(
        {"Error": {"Code": "NoSuchKey", "Message": "Not Found"}},
        "get_object"
    )

    # Run
    df = read_watermarks()

    # Validate call
    mock_init_client.assert_called_once()
    
    # Validate Result
    assert "NoSuchKey" in caplog.text
    assert isinstance(df, pd.DataFrame)
    assert df.empty
    assert list(df.columns) == ["dataset_name", "max_value", "records_loaded", "processing_time"]
   

# ---------------
# update_watermarks
# ---------------


# integration for read watermarks

# 
# watermarks data quality (as CI/CD with faking real data)

# unit data quality after writing and embedding validate_data in runtime   