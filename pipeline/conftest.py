"""Contains all fixtures used for testing."""

import pytest
import pandas as pd


@pytest.fixture
def example_api_call():
    """An example API call from the BandCamp API, calling two event items."""
    return {
        "start_date": 1749583860,
        "end_date": 1749584460,
        "data_delay_sec": 120,
        "events": [
            {
                "event_type": "sale",
                "utc_date": 1749638295.33538,
                "items": [
                    {
                        "utc_date": 1749638295.4456537,
                        "artist_name": "Ella Zirina",
                        "item_type": "a",
                        "item_description": "Boundless Blue, Sunset Hue",
                        "album_title": None,
                        "slug_type": "a",
                        "track_album_slug_text": None,
                        "currency": "EUR",
                        "amount_paid": 7,
                        "item_price": 7,
                        "amount_paid_usd": 8,
                        "country": "Japan",
                        "art_id": 3672109546,
                        "releases": None,
                        "package_image_id": None,
                        "url": "//ellazirina.bandcamp.com/album/boundless-blue-sunset-hue",
                        "country_code": "jp",
                        "amount_paid_fmt": "€7",
                        "art_url": "https://f4.bcbits.com/img/a3672109546_7.jpg"
                    }
                ]
            }
        ],
        "server_time": 1749584469
    }


@pytest.fixture
def incorrect_api_call():
    """An incorrect API call from the BandCamp API.
    This happens when the server time does not match anything in the API
    For example, calling the current time will return this, as it has not been generated yet."""
    return {
        "server_time": 1749584741
    }


@pytest.fixture
def sample_df():
    """Returns a sample pandas DataFrame to test with."""
    data = {
        "utc_date": [1749542761.50579, 1749542767.53329],
        "artist_name": ["Alex Lynch", "Blackchild (ITA)"],
        "item_type": ["t", "t"],
        "item_description": ["Thank Me Later", "Nothing Better"],
        "album_name": [None, "Nothing Better"],
        "currency": ["GBP", "GBP"],
        "amount_paid": [1.5, 1.5],
        "sold_for": [2.02, 2.02],
        "country_name": ["United Kingdom", "Germany"],
        "country_code": ["gb", "de"],
        "url": ["//bandcamp.com/track/1", "//bandcamp.com/track/2"],
        "art_url": ["//image.com/1.jpg", "//image.com/2.jpg"],
        "event_type": ["sale", "sale"],
        "slug_type": ["t", "t"],
        "tag_names": [["rock"], ["rnb"]],
        "release_date": ["released January 18, 2023", "released January 20, 2023"]
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
        "event_type": ["", "refund?"],
        "slug_type": ["", None]
    }
    return pd.DataFrame(data)


@pytest.fixture
def empty_df():
    """Returns a pandas DataFrame with empty data for testing."""
    data = {}
    return pd.DataFrame(data)


@pytest.fixture
def empty_df_with_columns():
    """Returns an empty DataFrame with the required structure."""
    return pd.DataFrame(columns=["item_type", "item_description", "album_name"])


@pytest.fixture
def df_with_albums():
    """Fixture for testing handling missing album names."""
    return pd.DataFrame({
        "item_type": ["a", "t", "a"],
        "item_description": ["Album One", "Track One", "Album Two"],
        "album_name": [None, "Track One Album", None]
    })


@pytest.fixture
def df_no_albums():
    """Fixture for testing handling missing album names."""
    return pd.DataFrame({
        "item_type": ["t", "t"],
        "item_description": ["Track A", "Track B"],
        "album_name": ["Old A", "Old B"]
    })

