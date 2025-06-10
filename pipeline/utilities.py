"""Python script for logging on AWS."""

from logging import getLogger, INFO, StreamHandler
from sys import stdout
import pandas as pd


def get_logger():
    """Return logger with desired config."""
    return getLogger(__name__)


def set_logger():
    """Set logger configuration."""
    logger = getLogger(__name__)
    logger.setLevel(INFO)
    logger.addHandler(StreamHandler(stdout))


def load_csv_data(file_path: str = 'data/output.csv') -> pd.DataFrame:
    """Loads the unclean data from csv."""
    logger = get_logger()
    if not isinstance(file_path, str):
        logger.critical("Invalid URL type.")
        raise TypeError("Invalid URL type.")
    if file_path[-4:] != ".csv":
        logger.critical("Filename doesn't end in .csv.")
        raise ValueError("Please end your filename in .csv.")
    return pd.read_csv(file_path)
