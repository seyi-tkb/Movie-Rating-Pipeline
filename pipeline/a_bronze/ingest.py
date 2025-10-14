# importing libraries/modules
import pandas as pd
from utils.logger import get_logger

# initialize logger
logger = get_logger(__name__)

# function to read files from source (drive)
def read_files(urls: dict) -> dict: 
    """
    Reads a list of the 3 file paths into dataframes

    Parameters:
        urls: dictionary of file names and their urls

    Returns:
        dict: dictionary of file names and created dataframes
    """
    dataframes = {}

    for file, url in urls.items():
        try:
            file_id = url.split('/')[-2]
            direct_url = f"https://drive.usercontent.google.com/download?id={file_id}&export=download&authuser=0&confirm=t"
            
            logger.info(f"Reading {file}...")
            
            df = pd.read_csv(direct_url)
            dataframes[file] = df
            
            logger.info(f"Successfully read {file}.")

        except Exception as e:
            logger.info(f"Failed to read {file}. Error: {e}")

    return dataframes

if __name__ == "__main__":
    pass