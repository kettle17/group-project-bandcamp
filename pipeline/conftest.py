"""Contains all fixtures used for testing."""

import pytest
import pandas as pd


@pytest.fixture
def example_api_call():
    return {
        "start_date": 1749583860,
        "end_date": 1749584460,
        "data_delay_sec": 120,
        "events": [
            {
                "event_type": "sale",
                "utc_date": 1749638281.9409266,
                "items": [
                    {
                        "utc_date": 1749638281.672574,
                        "artist_name": "Andrew Applepie",
                        "item_type": "t",
                        "item_description": "When The World Goes Down",
                        "album_title": "A Couple Of Pop Songs",
                        "slug_type": "t",
                        "track_album_slug_text": None,
                        "currency": "EUR",
                        "amount_paid": 1,
                        "item_price": 1,
                        "amount_paid_usd": 1.14,
                        "country": "Japan",
                        "art_id": 2194995336,
                        "releases": None,
                        "package_image_id": None,
                        "url": "//andrewapplepie.bandcamp.com/track/when-the-world-goes-down",
                        "country_code": "jp",
                        "amount_paid_fmt": "€1",
                        "art_url": "https://f4.bcbits.com/img/a2194995336_7.jpg"
                    }
                ]
            },
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


@pytest.fixture
def mock_env_vars():
    """Fixture providing mock environment variables for database connection."""
    return {
        "DB_HOST": "testlocalhost",
        "DB_NAME": "testdb",
        "DB_USER": "testuser",
        "DB_PASSWORD": "veryrealpassword"
    }
