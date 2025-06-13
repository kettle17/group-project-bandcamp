# pylint: skip-file
"""Script for the load portion of the ETL pipeline."""

from os import environ as ENV
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from psycopg2.extensions import connection
from psycopg2.extensions import cursor as pg_cursor


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


def export_to_csv(dataframe: pd.DataFrame, output_path: str = "data/output.txt") -> None:
    """Exports a dataframe as a csv."""


def run_load(dataframe: pd.DataFrame = None, csv_path: str = None) -> None:
    """Runs the functions required for the load portion of the ETL pipeline in succession."""


def parse_tag_list(raw: str) -> list[str]:
    """Returns a list of tags from a string."""


def extract_tags(df: pd.DataFrame) -> list[str]:
    """Returns a sorted list of unique tags from a DataFrame."""


def get_filtered(sales: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Returns filtered DataFrames for each content type."""


def load_sales_csv() -> pd.DataFrame:
    """Returns the full sales DataFrame from the CSV file."""


def load_existing(cursor: pg_cursor, sales: pd.DataFrame, content_dfs: dict[str, pd.DataFrame]) -> dict:
    """Returns a dictionary of existing database values like countries, artists, tags, and content."""


def remove_existing(sales: pd.DataFrame, existing: dict) -> pd.DataFrame:
    """Returns only the rows that aren't already in the database."""


def insert_entities(df: pd.DataFrame, entity_name: str, cursor: pg_cursor) -> dict:
    """Returns a dictionary mapping entity name to ID after inserting countries or artists."""


def insert_tags(df: pd.DataFrame, cursor: pg_cursor) -> dict:
    """Returns a dictionary mapping tag name to ID after inserting new tags."""


def insert_content(df: pd.DataFrame, content_type: str, cursor: pg_cursor) -> dict:
    """Returns a dictionary mapping content URL to its new or existing database ID."""


def insert_artist_assignments(df: pd.DataFrame, content_type: str, existing: dict, cursor: pg_cursor) -> None:
    """Returns None. Inserts links between artists and content (track, album, merch)."""


def insert_tag_assignments(df: pd.DataFrame, content_type: str, existing: dict, tag_map: dict, cursor: pg_cursor) -> None:
    """Returns None. Inserts links between tags and content (track or album)."""


def insert_sales_and_assignments(sales: pd.DataFrame, existing: dict, cursor: pg_cursor) -> None:
    """Returns None. Inserts sales records and connects them to content (track, album, or merch)."""


def load_data() -> None:
    """Returns None. Runs the full ETL process from CSV to database."""


if __name__ == "__main__":
    conn = get_db_connection()
    print(conn)
