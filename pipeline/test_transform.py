# pylint: skip-file

"""Test file for the transform pipeline."""

from transform import clean_dataframe_from_csv, get_required_columns, check_valid_sale, clean_urls
import pandas as pd
from unittest.mock import patch, mock_open
import pytest


@pytest.fixture
def sample_df():
    data = {
        "utc_date": [1749542761.50579, 1749542767.53329],
        "artist_name": ["Alex Lynch", "Blackchild (ITA)"],
        "item_type": ["t", "t"],
        "item_description": ["Thank Me Later", "Nothing Better"],
        "album_title": [None, "Nothing Better"],
        "currency": ["GBP", "GBP"],
        "amount_paid": [1.5, 1.5],
        "amount_paid_usd": [2.02, 2.02],
        "country": ["United Kingdom", "Germany"],
        "country_code": ["gb", "de"],
        "url": ["//bandcamp.com/track/1", "//bandcamp.com/track/2"],
        "art_url": ["//image.com/1.jpg", "//image.com/2.jpg"],
        "event_type": ["sale", "sale"]
    }
    return pd.DataFrame(data)


def test_get_required_columns(sample_df):
    result = get_required_columns(sample_df)
    expected_cols = [
        "utc_date", "artist_name", "item_type", "item_description", "album_title",
        "currency", "amount_paid", "country",
        "url", "art_url"
    ]
    assert list(result.columns) == expected_cols
    assert result.shape[1] == len(expected_cols)


def test_check_valid_sale(sample_df):
    result = check_valid_sale(sample_df)
    assert len(result) == 2
    assert (result["amount_paid"] > 0).all()


def test_clean_urls(sample_df):
    result = clean_urls(sample_df)
    assert result["url"].iloc[0].startswith("https://")
    assert result["art_url"].iloc[1].startswith("https://")


def test_clean_dataframe_from_csv(tmp_path):
    csv_content = """
utc_date,artist_name,item_type,item_description,album_title,currency,amount_paid,amount_paid_usd,country,country_code,url,art_url,event_type
1749542761.50579,Alex Lynch,t,Thank Me Later,,GBP,1.5,2.02,United Kingdom,gb,//bandcamp.com/track/1,//image.com/1.jpg,sale
"""
    file_path = tmp_path / "test.csv"
    file_path.write_text(csv_content.strip())

    df = clean_dataframe_from_csv(str(file_path))
    assert not df.empty
    assert "utc_date" in df.columns
    assert pd.api.types.is_datetime64_any_dtype(df["utc_date"])
