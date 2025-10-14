# import neede
import pandas as pd
from ingest import read_files

# function to test ingestin
def test_read_files():
    # specify sample data path for test
    paths = ["sample/user_sample.csv", 
             "sample/item_sample.csv",
             "sample/rating.csv"]
    
    # columns to check during test
    columns = [["user_id", "age", "gender", "occupation","zip_code"], 
               ["item_id", "movie_title", "release_date", "IMDb_URL", "primary_genre"],
               ["user_id", "item_id", "rating", "timestamp"]]
    
    # read samples into dataframes
    dfs = read_files(paths)

    # check number of dataframes
    assert len(dfs) == 3

    # test for emptiness, type and expected columns
    for df, column in zip(dfs, columns):
        assert isinstance (df, pd.DataFrame)
        assert not df.empty
        for column in columns:
            assert column is df.columns

