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


if __name__ == "__main__":
    pass
