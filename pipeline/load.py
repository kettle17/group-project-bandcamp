
# pylint: skip-file
"""Script for the load portion of the ETL pipeline."""
import pandas as pd
import psycopg2
from psycopg2.extensions import connection


def get_db_connection() -> connection:
    """Returns a database connection established using .env credentials."""


def upload_to_db(dataframe: pd.DataFrame, conn: connection) -> None:
    """Uploads a pandas dataframe to a database."""


def export_to_csv(dataframe: pd.DataFrame) -> None:
    """Exports a dataframe as a csv."""


def run_load() -> None:
    """Runs the functions required for the load portion of the ETL pipeline in succession."""


if __name__ == "__main__":
    pass
