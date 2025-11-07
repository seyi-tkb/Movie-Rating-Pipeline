import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
from pipeline.a_bronze.ingest import read_files
from pipeline.a_bronze.upload import upload_to_bronze


# ------------
# read_files()
# ------------


# Success path
@pytest.mark.unit
@patch("pandas.read_csv")
def test_read_files_success(mock_read_csv):
    urls = {
        "users": "https://drive.google.com/file/d/abc123/view?usp=sharing",
        "ratings": "https://drive.google.com/file/d/xyz456/view?usp=sharing",
    }
    mock_read_csv.return_value = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
    result = read_files(urls)
    assert isinstance(result, dict)
    assert set(result.keys()) == {"users", "ratings"}
    for df in result.values():
        assert isinstance(df, pd.DataFrame)
        assert "id" in df.columns


# Failure path
@pytest.mark.unit
@patch("pandas.read_csv", side_effect=Exception("Bad URL"))
def test_read_files_failure(mock_read_csv, caplog):
    urls = {"bad_file": "https://drive.google.com/file/d/bad/view?usp=sharing"}
    result = read_files(urls)
    assert isinstance(result, dict)
    assert "bad_file" not in result or result["bad_file"] is None
    assert "Failed to read bad_file" in caplog.text


@pytest.mark.unit
@patch("pandas.read_csv")
def test_read_files_partial_failure(mock_read_csv):
    """
    Test that read_files handles partial failures gracefully.
    """

    # Simulate a good and bad file link
    urls = {
        "users": "https://drive.google.com/file/d/abc123/view?usp=sharing",
        "ratings": "https://drive.google.com/file/d/xyz456/view?usp=sharing",
    }

    # Define behaviour
    def side_effect(url, *args, **kwargs):
        if "abc123" in url:
            return pd.DataFrame({"id": [1], "name": ["Alice"]})
        else:
            raise Exception("Simulated read failure")

    mock_read_csv.side_effect = side_effect

    # Run function
    result = read_files(urls)

    # Assertions
    assert isinstance(result, dict)
    assert "users" in result
    assert "ratings" not in result or result["ratings"] is None
    assert isinstance(result["users"], pd.DataFrame)
    assert result["users"].shape[0] == 1


# ------------
# upload_to_bronze()
# ------------


# Success path
@pytest.mark.unit
@patch("pipeline.a_bronze.upload.S3_BUCKET_BRONZE", "movie-pipeline-bronze")
def test_upload_to_bronze_success():
    """
    Test that `upload_to_bronze` successfully uploads a DataFrame to S3.
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


# Failure path
@pytest.mark.unit
@patch("pipeline.a_bronze.upload.S3_BUCKET_BRONZE", "movie-pipeline-bronze")
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


# Partial failure
@pytest.mark.unit
@patch("pipeline.a_bronze.upload.S3_BUCKET_BRONZE", "movie-pipeline-bronze")
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
