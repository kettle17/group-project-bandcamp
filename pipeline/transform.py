"""Script for the transform part of the ETL."""
from datetime import datetime
import pandas as pd
from utilities import get_logger, set_logger


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Renames columns to a standardized format if needed."""
    return df.rename(columns={"country": "country_name", "album_title": "album_name",
                              "track_title": "track_name", "releases": "release_date",
                              "amount_paid_usd": "sold_for"
                              })


def get_required_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Returns the dataframe with the required columns."""
    logger = get_logger()

    required_cols = [
        "utc_date", "artist_name", "item_type", "item_description",
        "album_name", "currency", "country_name", "url", "art_url",
        "tag_name", "sold_for", "release_date", "track_name", "amount_paid_usd"
    ]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        logger.critical("Missing required columns: %s", missing_cols)
        raise KeyError(f"Missing required columns: {missing_cols}")
    return df[required_cols]


def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """Handles or removes rows with missing essential fields."""
    return df.dropna(subset=["artist_name"])


def filter_by_currency(df: pd.DataFrame, accepted: list = ["USD", "GBP", "EUR"]) -> pd.DataFrame:
    """Returns a pandas dataframe filtered using accepted currency types."""
    return df[df["currency"].isin(accepted)]


def standardize_dates(date: str) -> datetime:
    """Converts a UTC timestamp string to a datetime object."""
    return datetime.utcfromtimestamp(float(date))


def sort_by_date(df: pd.DataFrame) -> pd.DataFrame:
    """Sorts the dataframe by the utc_date column in ascending order."""
    return df.sort_values(by="utc_date")


def standardize_currency(amount: float, currency: str) -> float:
    """Standardizes the currency amount to USD using static exchange rates."""
    rates = {"USD": 1.0, "EUR": 1.1, "GBP": 1.3}
    return amount * rates.get(currency, 1.0)


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Returns a cleaned pandas dataframe after applying all transformation functions."""
    logger = get_logger()
    logger.info("Cleaning dataframe...")
    df = rename_columns(df)
    df = get_required_columns(df)
    df = handle_missing_values(df)
    df = filter_by_currency(df)

    df["utc_date"] = pd.to_datetime(df["utc_date"], unit="s", errors="coerce")
    df = sort_by_date(df)

    df["amount_usd"] = df.apply(lambda row: standardize_currency(
        row["amount_paid"], row["currency"]), axis=1)

    return df


def export_dataframe(df: pd.DataFrame, output_path: str = "data/clean_sales.csv") -> None:
    """Exports the cleaned DataFrame to a CSV file."""
    df.to_csv(output_path, index=False)


if __name__ == "__main__":
    FILE_PATH = "data/output.csv"
    data = pd.read_csv(FILE_PATH)

    CLEAN_DF = clean_dataframe(data)
    export_dataframe(CLEAN_DF)
