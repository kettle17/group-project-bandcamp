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
    """Returns a pandas dataframe of total revenue and sales aggregated by specified field."""
    pass


def filter_by_currency(df: pd.DataFrame, accepted: list = ["USD", "GBP", "EUR"]) -> pd.DataFrame:
    """Returns a pandas dataframe filtered using accepted currency types."""
    pass


def validate_foreign_keys(df: pd.DataFrame) -> bool:
    """Checks if a foreign key already exists before mapping it to a new id."""
    pass


def get_id_mapping(df: pd.DataFrame, table_name: str) -> dict:
    """Returns a dictionary of foriegn key mappings."""
    pass


def add_foreign_key_column(df: pd.DataFrame) -> pd.DataFrame:
    """Returns a pandas dataframe containing foreign key columns."""
    pass


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Returns a cleaned pandas dataframe after calling all transform functions in succession."""
    pass


def export_dataframe(df: pd.DataFrame, output_path: str = "data/clean_sales.csv") -> None:
    """Exports the cleaned DataFrame to a CSV file."""
    pass


if __name__ == "__main__":
    pass
