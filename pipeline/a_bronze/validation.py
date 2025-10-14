# importing libraries/modules
from utils.logger import get_logger

# initialize logger
logger = get_logger(__name__)

# function to check columns
def validate_columns(dataframes: dict, expected_cols: dict):
    """
    Validates that each dataframe contains the expected columns.
    
    Parameters:
        dataframes (dict): {name: dataframe}
        expected_columns (dict): {name: list of expected columns}
    """
    for file, df in dataframes.items():
        logger.info(f"Validating columns for {file}..")
        expected_columns = set(expected_cols[file])
        actual_columns = set(df.columns)
        missing = expected_columns - actual_columns
        extra = actual_columns - expected_columns

        # if missing set is not empty
        if missing: 
            logger.error(f"Missing columns in {file}: {missing}")
            raise ValueError(f"Missing columns in {file}: {missing}")
        
        # if extra columns are present
        if extra:
            logger.warning(f"Extra columns found in {file}: {extra}")

        logger.info(f"successfully validated {file}'s columns.")

# function to check null values
def validate_nulls(dataframes: dict):
    """
    Logs if there are any null values in the dataframes.
    
    Parameters:
        dataframes (dict): {name: dataframe}
    """
    for name, df in dataframes.items():
        null_counts = df.isnull().sum()
        total_nulls = null_counts.sum()
        if total_nulls > 0:
            logger.warning(f"{name} has {total_nulls} missing values:\n{null_counts[null_counts > 0]}")
        else:
            logger.info(f"No missing values found in {name}.")

if __name__ == "__main__":
    pass