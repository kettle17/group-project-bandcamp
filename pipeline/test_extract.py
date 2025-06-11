# pylint: skip-file

"""Test file for the extract pipeline."""

import pytest
import os
from unittest.mock import patch
from extract import get_api_request, fetch_api_data, export_api_data_to_csv, run_extract, get_time_offset

pytest.skip(allow_module_level=True)

"""get_api_request tests"""


def test_get_api_request_start_date_invalid_type():
    """Test that checks if start_date is a valid int."""
    with pytest.raises(TypeError):
        get_api_request('I am not an int')


def test_get_api_request_start_date_should_deny_negative():
    """Test that checks if start_date accepts negative values."""
    with pytest.raises(ValueError):
        get_api_request(-58492573)


@pytest.fixture
def example_api_call():
    return {
        "start_date": 1749583860,
        "end_date": 1749584460,
        "data_delay_sec": 120,
        "events": [
            {
                "event_type": "sale",
                "utc_date": 1749583861.5860023,
                "items": [
                    {
                        "utc_date": 1749583861.7284331,
                        "artist_name": "nanobii",
                        "item_type": "t",
                        "item_description": "F**K (Extended Mix)",
                        "album_title": "F**K",
                        "slug_type": "t",
                        "track_album_slug_text": None,
                        "currency": "SEK",
                        "amount_paid": 8,
                        "item_price": 8,
                        "amount_paid_usd": 0.83,
                        "country": "United States",
                        "art_id": 34096151,
                        "releases": None,
                        "package_image_id": None,
                        "url": "//nanobii.bandcamp.com/track/f-k-extended-mix",
                        "country_code": "us",
                        "amount_paid_fmt": "8 SEK",
                        "art_url": "https://f4.bcbits.com/img/a0034096151_7.jpg"
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


"""fetch_api_data tests"""


@patch('extract.get_api_request')
def test_fetch_api_data_output_is_correct_format(fake_get_request, example_api_call):
    """Test that correctly executes the function."""
    fake_get_request.return_value = example_api_call
    assert type(fetch_api_data(1749583860)) == dict


def test_fetch_api_data_start_date_invalid_type():
    """Test that checks if script halts on incorrect type parameter."""
    with pytest.raises(TypeError):
        fetch_api_data('I am not an int')


def test_fetch_api_data_start_date_should_deny_negative():
    """Check if start_date accepts negative values."""
    with pytest.raises(ValueError):
        fetch_api_data(-58492573)


def test_fetch_api_data_incorrect_data_returned(fake_get_request, incorrect_api_call):
    """Test that checks if script halts if the format of dictionary data is not as expected."""
    fake_get_request.return_value = incorrect_api_call
    with pytest.raises(ValueError):
        fetch_api_data(1749583860)


"""export_api_data_to_csv tests"""


def test_export_api_data_to_csv_wrong_type_for_data():
    """Test that checks if script halts if the data type is not a dict."""
    with pytest.raises(TypeError):
        export_api_data_to_csv('I am not a dict', 'filepath')


def test_export_api_data_to_csv_wrong_type_for_filepath():
    """Test that checks if script halts if the file path isn't a string."""
    with pytest.raises(TypeError):
        export_api_data_to_csv({'dict': 'dict'}, 2424)


def test_export_api_data_to_csv_wrong_value_for_filepath():
    """Test that checks if script halts if the folder path doesn't exist."""
    with pytest.raises(LookupError):
        export_api_data_to_csv(
            {'dict': 'dict'}, 'data/data/data/data/data/output.csv')


def test_export_api_data_to_csv_wrong_value_for_filepath():
    """Test that checks if script halts if the file path doesn't end in csv"""
    with pytest.raises(ValueError):
        export_api_data_to_csv({'dict': 'dict'}, 'output')


@patch('extract.get_api_request')
def test_export_api_data_to_csv_incorrect_data_returned(fake_get_request, incorrect_api_call):
    """Test that checks that csv isn't saved if the data is incorrectly formatted."""
    fake_get_request.return_value = incorrect_api_call
    with pytest.raises(ValueError):
        export_api_data_to_csv(incorrect_api_call, 'output.csv')


@patch('extract.output_file')
@patch('extract.get_api_request')
def test_export_api_data_to_csv_correct_data_returned(fake_get_request, fake_output_file, example_api_call):
    """Test that checks that csv is saved if the data is correctly formatted."""
    fake_get_request.return_value = incorrect_api_call
    fake_output_file.return_value = None
    assert export_api_data_to_csv(example_api_call, 'output.csv')


"""run_extract tests"""


def test_run_extract_wrong_type_for_filepath():
    """Test that checks if script halts if the file path isn't a string."""
    with pytest.raises(TypeError):
        run_extract({"filepath": "i am not a filepath"})


def test_run_extract_wrong_value_folder_path_doesnt_exist():
    """Test that checks if script halts if the folder path doesn't exist."""
    with pytest.raises(LookupError):
        run_extract('data/data/data/data/data/output.csv')


def test_run_extract_wrong_type_for_curr_time():
    """Test that checks if script halts if the curr_time isn't an int."""
    with pytest.raises(TypeError):
        run_extract('data/output.csv', 'i am not an int')


def test_run_extract_wrong_value_for_curr_time():
    """Test that checks if script halts if the curr_time is negative."""
    with pytest.raises(TypeError):
        run_extract('data/output.csv', -49234739284723)


@patch('extract.get_api_request')
def test_export_api_data_to_csv_incorrect_data_returned(fake_get_request, incorrect_api_call):
    """Test that checks that csv isn't saved if the data is incorrectly formatted."""
    fake_get_request.return_value = incorrect_api_call
    with pytest.raises(ValueError):
        export_api_data_to_csv(incorrect_api_call, 'output.csv')


@patch('extract.get_api_request')
def test_run_extract_incorrect_data_returned(fake_get_request, incorrect_api_call):
    """Test that checks that csv isn't saved if the data is incorrectly formatted."""
    fake_get_request.return_value = incorrect_api_call
    with pytest.raises(ValueError):
        run_extract('data/output.csv', 1749583860242424)


@patch('extract.output_file')
@patch('extract.get_api_request')
def test_run_extract_correct_data_returned(fake_get_request, fake_output_file, example_api_call):
    """Test that checks that csv is saved if the data is correctly formatted."""
    fake_get_request.return_value = example_api_call
    fake_output_file.return_value = None
    assert run_extract('data/output.csv', 2380921482190481)


"""get_time_offset tests"""


def tests_get_time_offset_passing_5000_should_return_4800():
    """Tests if the offset is working."""
    assert get_time_offset(5000) == 4800


def tests_get_time_offset_raises_errors():
    """Tests if the function errors as expected."""
    with pytest.raises(TypeError):
        get_time_offset('I am not an int')
