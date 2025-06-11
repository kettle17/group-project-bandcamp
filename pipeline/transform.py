"""Script for the transform part of the ETL."""
# pylint: disable = unnecessary-pass, unused-argument

import pandas as pd


def get_required_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Returns the dataframe with the required columns."""
    pass


def check_valid_sale(df: pd.DataFrame) -> bool:
    """Checks that the sale is valid using amount paid > 0."""
    pass


def clean_urls(df: pd.DataFrame) -> pd.DataFrame:
    """Returns a pandas dataframe of cleaned urls."""
    pass


def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """Handles or removes rows with missing essential fields."""
    pass


def group_sales_by(df: pd.DataFrame, group_by: str = "date") -> pd.DataFrame:
    """Aggregates total revenue and sales by specified field."""
    pass


def filter_by_currency(df: pd.DataFrame, accepted: list = ["USD", "GBP", "EUR"]) -> pd.DataFrame:
    """Filters rows based on accepted currency types."""
    pass


def validate_forein_keys(df: pd.DataFrame) -> bool:
    """Checks if a forein key already exists before mapping it to a new id."""
    pass


def get_id_mapping(df: pd.DataFrame) -> pd.DataFrame:
    """Adds ID mapping to each foreign key on separate columns."""
    pass


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Combines all of the previous functions into one."""
    pass


def export_dataframe(df: pd.DataFrame, output_path: str = "data/clean_sales.csv") -> None:
    """Exports the cleaned DataFrame to a CSV file."""
    pass


if __name__ == "__main__":
    pass
