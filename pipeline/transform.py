"""Script for the transform part of the ETL."""
import re
from datetime import datetime, UTC
import pandas as pd
from utilities import get_logger, set_logger


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Renames columns to a standardized format if needed."""
    return df.rename(columns={"country": "country_name", "album_title": "album_name",
                              "track_title": "track_name", "releases": "release_date",
                              "amount_paid_usd": "sold_for", "genres": "tag_names"
                              })


def get_required_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Returns the dataframe with the required columns."""
    logger = get_logger()

    required_cols = [
        "utc_date", "artist_name", "item_type", "item_description",
        "album_name", "currency", "country_name", "url", "art_url",
        "tag_names", "sold_for", "release_date"
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
        raise ValueError("Column 'artist_name' is missing from DataFrame.")
    return df.dropna(subset=["artist_name"])


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
    """Extracts and standardizes release date strings into datetime objects."""
    logger = get_logger()

    if not isinstance(date, str):
        logger.critical("Release date must be a string.")
        raise TypeError("Release date must be a string.")

    match = re.search(r"released\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})", date)
    if not match:
        logger.critical(f"Invalid release date format: {date}")
        raise ValueError(f"Invalid release date format: {date}")

    try:
        naive_dt = datetime.strptime(match.group(1), "%B %d, %Y")
        return naive_dt.replace(tzinfo=UTC)
    except ValueError as e:
        logger.critical("Failed to parse release date: %s", e)
        raise


def sort_by_date(df: pd.DataFrame) -> pd.DataFrame:
    """Sorts the dataframe by the utc_date column in ascending order."""
    return df.sort_values(by="utc_date")


def standardize_currency(amount: float, currency: str) -> float:
    """Standardizes the currency amount to USD using static exchange rates."""
    rates = {"USD": 1.0, "EUR": 1.1, "GBP": 1.3}
    return amount * rates.get(currency, 1.0)


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Returns a cleaned pandas DataFrame after applying all transformation functions."""
    logger = get_logger()
    logger.info("Cleaning dataframe...")

    df = rename_columns(df)
    df = get_required_columns(df)
    df = handle_missing_values(df)
    df["utc_date"] = df["utc_date"].apply(standardize_dates)
    df = sort_by_date(df)
    df["sold_for"] = df.apply(lambda row: standardize_currency(
        row["sold_for"], row["currency"]), axis=1)

    return df


def export_dataframe(df: pd.DataFrame, output_path: str = "data/clean_sales.csv") -> None:
    """Exports the cleaned DataFrame to a CSV file."""
    df.to_csv(output_path, index=False)


if __name__ == "__main__":
    set_logger()
    FILE_PATH = "data/output (1).csv"
    data = pd.read_csv(FILE_PATH)

    CLEAN_DF = clean_dataframe(data)
    export_dataframe(CLEAN_DF)
