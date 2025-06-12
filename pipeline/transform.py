"""Script for the transform part of the ETL."""
# pylint: disable =unused-argument,unnecessary-pass,function-redefined,assignment-from-no-return,dangerous-default-value

import pandas as pd


def get_required_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Returns the dataframe with the required columns."""
    required_cols = [
        "utc_date", "artist_name", "item_type", "item_description", "album_title",
        "currency", "amount_paid", "country", "url", "art_url"
    ]
    return df[required_cols]


def check_valid_sale(df: pd.DataFrame) -> bool:
    """Returns true when amount_paid > 0 in a pandas dataframe, false otherwise."""
    return True


def clean_urls(df: pd.DataFrame) -> pd.DataFrame:
    """Returns a pandas dataframe of cleaned urls."""
    df["url"] = df["url"].apply(
        lambda x: x if x.startswith("http") else "https:" + x)
    df["art_url"] = df["art_url"].apply(
        lambda x: x if x.startswith("http") else "https:" + x)
    return df


def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """Handles or removes rows with missing essential fields."""
    return df.dropna(subset=["artist_name"])


def group_sales_by(df: pd.DataFrame, group_by: str = "date") -> pd.DataFrame:
    """Returns a pandas dataframe of total revenue and sales aggregated by specified field."""
    return df


def filter_by_currency(df: pd.DataFrame, accepted: list = ["USD", "GBP", "EUR"]) -> pd.DataFrame:
    """Returns a pandas dataframe filtered using accepted currency types."""
    return df[df["currency"].isin(accepted)]


def validate_foreign_keys(df: pd.DataFrame) -> bool:
    """Checks if a foreign key already exists before mapping it to a new id."""
    return True


def get_id_mapping(df: pd.DataFrame, table_name: str) -> dict:
    """Returns a dictionary of foriegn key mappings."""
    unique_values = df["artist_name"].unique()
    return {name: idx + 100 for idx, name in enumerate(unique_values)}


def add_foreign_key_column(df: pd.DataFrame) -> pd.DataFrame:
    """Returns a pandas dataframe containing foreign key columns."""
    mapping = get_id_mapping(df, "artists")
    df = df.copy()
    df["foreign_key"] = df["artist_name"].map(mapping)
    return df


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Returns a cleaned pandas dataframe after calling all transform functions in succession."""
    df = filter_by_currency(df)
    df = group_sales_by(df)
    df = add_foreign_key_column(df)
    validate_foreign_keys(df)

    df["utc_date"] = pd.to_datetime(df["utc_date"], unit="s", errors="coerce")
    return df


def export_dataframe(df: pd.DataFrame, output_path: str = "data/clean_sales.csv") -> None:
    """Exports the cleaned DataFrame to a CSV file."""
    df.to_csv(output_path, index=False)


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
