# General
import pandas as pd
import pytest

# Unit
from unittest.mock import patch, MagicMock
from pipeline.a_bronze.upload import upload_to_bronze

# Integration
from moto import mock_aws
import boto3
from pipeline.a_bronze.upload import upload_to_bronze

# Orchestrator
from pipeline.a_bronze.orchestration import source_to_bronze


@pytest.mark.unit
def test_upload_to_bronze_success():
    """
    Test that upload_to_bronze successfully uploads a DataFrame to S3.
    """

    # Create a mock S3 client and patch the initializer
    mock_client = MagicMock()  # fake s3_mock_client

    # Create a sample DataFrame to upload
    df = pd.DataFrame({"id": [1], "name": ["Alice"]})
    dataframes = {"users": df}

    # Run the upload function
    upload_to_bronze(mock_client, dataframes)

    # Assert that put_object was called once
    mock_client.put_object.assert_called_once()

    # Extract call arguments and validate structure
    args, kwargs = mock_client.put_object.call_args
    assert kwargs["Bucket"]  # Bucket name should be present
    assert kwargs["Key"] == "users.csv"  # File name should match
    assert isinstance(kwargs["Body"], bytes)  # Body should be byte stream


@pytest.mark.unit
def test_upload_to_bronze_failure(caplog):
    """
    Test that upload_to_bronze logs an error when S3 upload fails.

    Parameters:
        caplog (pytest.LogCaptureFixture): Pytest fixture used to capture
            log messages emitted during the test, enabling assertions
            on the logged output.
    """
    # Create a mock S3 client that raises an exception
    mock_client = MagicMock()
    mock_client.put_object.side_effect = Exception("Error Type")

    # Create a sample DataFrame
    df = pd.DataFrame({"id": [1], "name": ["Alice"]})
    dataframes = {"users": df}

    # Run the upload function
    upload_to_bronze(mock_client, dataframes)

    # Assert that the error message was logged
    assert "Failed to upload users. Error: Error Type" in caplog.text


@pytest.mark.unit
def test_upload_to_bronze_partial(caplog):
    """
    Test that upload_to_bronze handles partial failures correctly.
    One file should upload successfully, the other should fail.

    Parameters:
        caplog (pytest.LogCaptureFixture): Pytest fixture used to capture
            log messages emitted during the test, enabling assertions
            on the logged output.
    """
    # Create a mock S3 client with conditional failure
    mock_client = MagicMock()

    def side_effect(Bucket, Key, Body):
        if Key == "ratings.csv":
            raise Exception("Upload failed")

    mock_client.put_object.side_effect = side_effect

    # Create two sample DataFrames
    dfs = {"users": pd.DataFrame({"id": [1]}), "ratings": pd.DataFrame({"rating": [5]})}

    # Run the upload function
    upload_to_bronze(mock_client, dfs)

    # Assert that success and failure logs are both present
    assert "Successfully uploaded users.csv" in caplog.text
    assert "Failed to upload ratings" in caplog.text


@pytest.mark.integration
@mock_aws
# spins up fake versions of AWS services
# works on all boto3 calls in test
def test_upload_to_bronze_integration():
    """Test uploading a dataframe to a mocked S3 bucket."""
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="movie-pipeline-bronze")

    # Create a sample DataFrame
    df = pd.DataFrame({"id": [1], "name": ["Alice"]})
    dataframes = {"users": df}

    # Run function
    upload_to_bronze(s3, dataframes)

    # Assert
    result = s3.list_objects_v2(Bucket="movie-pipeline-bronze")
    assert "Contents" in result  # bucket contains at least an object


# -----------------
# ORCHESTRATOR FUNCTION -  Source_to_Bronze()
# ------------------


# source_to_bronze success
@pytest.mark.integration
# patch order matters: bottom to top - reverse of parameter order
@patch("pipeline.a_bronze.orchestration.read_files")
@patch("pipeline.a_bronze.orchestration.validate_columns")
@patch("pipeline.a_bronze.orchestration.validate_nulls")
@patch("pipeline.a_bronze.orchestration.create_bucket_if_not_exists")
@patch("pipeline.a_bronze.orchestration.upload_to_bronze")
@patch("pipeline.a_bronze.orchestration.initialize_s3_client")
def test_source_to_bronze_success(
    mock_init_client,
    mock_upload,
    mock_create_bucket,
    mock_validate_nulls,
    mock_validate_columns,
    mock_read_files,
):
    """
    Ensure full bronze pipeline executes all key steps in order.
    """

    # Mock S3 client initialization
    mock_client = MagicMock()
    mock_init_client.return_value = mock_client

    # Mock successful file ingestion
    mock_read_files.return_value = {
        "users": pd.DataFrame(
            columns=["user_id", "age", "gender", "occupation", "zip_code"]
        ),
        "movies": pd.DataFrame(
            columns=[
                "item_id",
                "movie_title",
                "release_date",
                "IMDb_URL",
                "primary_genre",
            ]
        ),
        "ratings": pd.DataFrame(columns=["user_id", "item_id", "rating", "timestamp"]),
    }

    # Run pipeline
    source_to_bronze()

    # Assertions
    mock_init_client.assert_called_once()
    mock_read_files.assert_called_once()
    mock_validate_columns.assert_called_once()
    mock_validate_nulls.assert_called_once()
    mock_create_bucket.assert_called_once()
    mock_upload.assert_called_once()


# source_to_bronze failure
@pytest.mark.integration
@patch("pipeline.a_bronze.orchestration.read_files")  # bottom-most patch
@patch("pipeline.a_bronze.orchestration.initialize_s3_client")  # top-most patch
def test_source_to_bronze_failure(mock_init_client, mock_read_files, caplog):
    """
    Ensure pipeline logs and raises exception when data ingestion fails.
    """

    # Mock: no S3 client needed since failure occurs early
    mock_init_client.return_value = None

    # Simulate read_files() raising an exception
    mock_read_files.side_effect = Exception("Error Type")

    # Run + expect raised error
    with pytest.raises(Exception, match="Error Type"):
        source_to_bronze()

    # Confirm the error message was logged
    assert "Pipeline execution failed. Error: Error Type" in caplog.text
