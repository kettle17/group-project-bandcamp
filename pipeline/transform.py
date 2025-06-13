"""Script for the transform part of the ETL."""
import re
from datetime import datetime, UTC
import pandas as pd
from utilities import get_logger, set_logger


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Renames columns to a standardized format if needed."""
    return df.rename(columns={"country": "country_name", "album_title": "album_name",
                              "track_title": "track_name",
                              "amount_paid_usd": "sold_for", "genres": "tag_names"
                              })


def get_required_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Returns the dataframe with the required columns."""
    logger = get_logger()

    required_cols = [
        "utc_date", "item_type", "album_name", "artist_name", "item_description",
        "tag_names", "sold_for", "release_date", "country_name", "slug_type", "url", "art_url"
    ]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        logger.critical("Missing required columns: %s", missing_cols)
        raise KeyError(f"Missing required columns: {missing_cols}")
    return df[required_cols]


def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """Handles or removes rows with missing 'artist_name' values."""
    logger = get_logger()
    if "artist_name" not in df.columns:
        logger.critical("Column 'artist_name' is missing from DataFrame.")
        raise KeyError("Column 'artist_name' is missing from DataFrame.")
    return df.dropna(subset=["artist_name"])


def correct_album_name(df: pd.DataFrame) -> pd.DataFrame:
    """
    Copies item_description to album_name where item_type is 'a' (album).
    Does not modify other values.
    """
    required_cols = {"item_type", "item_description", "album_name"}
    if not required_cols.issubset(df.columns):
        raise KeyError(f"DataFrame must contain columns: {required_cols}")

    df = df.copy()
    album_mask = df["item_type"] == "a"
    df.loc[album_mask, "album_name"] = df.loc[album_mask, "item_description"]
    return df


def standardize_dates(date: float) -> datetime:
    """Converts a UTC timestamp string to a datetime object."""
    logger = get_logger()

    if not isinstance(date, (float, int)):
        logger.critical(
            "Date object must be a float or int before standardizing.")
        raise TypeError(
            "Date object must be a float or int before standardizing.")

    return datetime.fromtimestamp(float(date), tz=UTC)


def standardize_release_date(date: str) -> datetime:
    """Converts a string like 'released June 3, 2025' into a datetime object."""
    logger = get_logger()

    if not isinstance(date, str):
        logger.critical("Release date must be a string.")
        raise TypeError("Release date must be a string.")

    match = re.search(r"released\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})", date)
    if match:
        return datetime.strptime(match.group(1), "%B %d, %Y")
    logger.critical("Invalid release date format: %s", date)
    raise ValueError(f"Invalid release date format: {date}")


def sort_by_date(df: pd.DataFrame) -> pd.DataFrame:
    """Sorts the DataFrame by the utc_date column in ascending order.
    Raises a ValueError if any null values exist in the utc_date column.
    """
    if df["utc_date"].isnull().any():
        raise ValueError("utc_date column contains null values.")
    return df.sort_values(by="utc_date")


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Returns a cleaned pandas DataFrame after applying all transformation functions."""
    logger = get_logger()
    if df.empty:
        logger.critical("DataFrame cannot be empty before cleaning...")
        raise KeyError("DataFrame cannot be empty before cleaning...")

    logger.info("Cleaning dataframe...")

    df = rename_columns(df)
    df = get_required_columns(df)
    df = handle_missing_values(df)
    df = correct_album_name(df)
    df["utc_date"] = df["utc_date"].apply(standardize_dates)
    df["release_date"] = df["release_date"].apply(
        lambda x: standardize_release_date(x) if isinstance(x, str) else x
    )

    df = sort_by_date(df)
    logger.info("Cleaning successfull!")
    return df


def export_dataframe(df: pd.DataFrame, output_path: str = "data/clean_sales.csv") -> None:
    """Exports the cleaned DataFrame to a CSV file."""
    logger = get_logger()
    logger.info("Exporting to csv...")
    df.to_csv(output_path, index=False)
    logger.info("Success!")


if __name__ == "__main__":
    set_logger()
    FILE_PATH = "data/output.csv"
    data = pd.read_csv(FILE_PATH)

    CLEAN_DF = clean_dataframe(data)
    export_dataframe(CLEAN_DF)
