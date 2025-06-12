"""Contains all fixtures used for testing."""

import pytest
import pandas as pd


@pytest.fixture
def sample_df():
    """Returns a sample pandas DataFrame to test with."""
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


@pytest.fixture
def bad_df():
    """Returns a pandas DataFrame with intentionally bad data for testing."""
    data = {
        "utc_date": ["not_a_date", None],
        "artist_name": ["", None],
        "item_type": ["x", 123],
        "item_description": [None, 5],
        "album_title": [42, ""],
        "currency": ["usdollars", "€"],
        "amount_paid": ["ten", -5],
        "amount_paid_usd": [None, "NaN"],
        "country": [123, "Atlantis"],
        "country_code": ["XX", None],
        "url": ["not_a_url", ""],
        "art_url": [None, 12345],
        "event_type": ["", "refund?"]
    }
    return pd.DataFrame(data)


@pytest.fixture
def empty_df():
    """Returns a pandas DataFrame with empty data for testing."""
    data = {}
    return pd.DataFrame(data)
