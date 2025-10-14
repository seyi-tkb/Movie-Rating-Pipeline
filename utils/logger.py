import logging
import os
from datetime import datetime

def get_logger(name: str, log_level=logging.INFO) -> logging.Logger:
    """
    Creates and returns a logger instance with console and file handlers.
    
    Args:
        name (str): The name of the logger, __name__.
        log_level (int): Logging level, default is INFO.

    Returns:
        logging.Logger: Configured logger instance.
    """

    # create a logs folder if it doesn't exist
    os.makedirs("logs", exist_ok=True)

    # define the log filename with timestamp
    log_filename = f"logs/{datetime.now().strftime('%Y-%m-%d')}.log"

    # create a logger
    logger = logging.getLogger(name)
    logger.setLevel(log_level)

    # prevent duplicating of logging
    if not logger.handlers:
        
        # Create formatter
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

        # Console Handler
        console_handler = logging.StreamHandler()       
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # File Handler
        file_handler = logging.FileHandler(log_filename)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


