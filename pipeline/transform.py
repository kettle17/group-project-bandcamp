"""Script for the transform part of the ETL."""
import pandas as pd


def clean_dataframe_from_csv(file_path: str) -> pd.DataFrame:
    pass


def get_required_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Returns the dataframe with the required columns."""
    pass


def check_valid_sale(df: pd.DataFrame) -> pd.DataFrame:
    """Checks that the sale is valid using amount paid > 0."""
    pass


def clean_urls(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans the urls to be used in the dashboard."""
    pass


def get_daily_report(df: pd.DataFrame) -> None:
    """Returns what is required from the data to be included in the report."""
    pass


if __name__ == "__main__":
    pass
