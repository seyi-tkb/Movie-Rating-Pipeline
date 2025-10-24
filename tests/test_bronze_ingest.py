import pandas as pd
import pytest
from unittest.mock import patch
from pipeline.a_bronze.ingest import read_files

@pytest.mark.unit
@patch("pandas.read_csv")
def test_read_files_success(mock_read_csv):
    urls = {
        "users": "https://drive.google.com/file/d/abc123/view?usp=sharing",
        "ratings": "https://drive.google.com/file/d/xyz456/view?usp=sharing"
    }
    mock_read_csv.return_value = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
    result = read_files(urls)
    assert isinstance(result, dict)
    assert set(result.keys()) == {"users", "ratings"}
    for df in result.values():
        assert isinstance(df, pd.DataFrame)
        assert "id" in df.columns

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
        "ratings": "https://drive.google.com/file/d/xyz456/view?usp=sharing"
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
