
# pylint: skip-file
"""Script for the load portion of the ETL pipeline."""
from os import environ as ENV
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from psycopg2.extensions import connection


def get_db_connection() -> connection:
    """Returns a database connection established using .env credentials."""
    load_dotenv('.env')
    return psycopg2.connect(
        user=ENV["DB_USER"],
        password=ENV["DB_PASSWORD"],
        host=ENV["DB_HOST"],
        port=ENV["DB_PORT"],
        database=ENV["DB_NAME"]
    )


def upload_to_db(dataframe: pd.DataFrame, conn: connection) -> None:
    """Uploads a pandas dataframe to a database."""


def export_to_csv(dataframe: pd.DataFrame, output_path: str = "output.txt") -> None:
    """Exports a dataframe as a csv."""


def run_load(dataframe: pd.DataFrame = None, csv_path: str = None) -> None:
    """Runs the functions required for the load portion of the ETL pipeline in succession."""


if __name__ == "__main__":
    conn = get_db_connection()
    print(conn)
