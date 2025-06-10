# pylint: skip-file

"""Test file for the extract pipeline."""

import pytest
import os
from extract import get_api_request, fetch_api_data, export_api_data_to_csv, run_extract, get_time_offset


def test_if_extract_exists_should_exist():
    """Basic beginner test. If this test can't run, neither can the rest."""
    try:
        assert os.path.exists('pipeline/extract.py')
    except:
        assert os.path.exists('extract.py')


"""get_api_request tests"""


def test_get_api_request_start_date_invalid_type():
    """Check if start_date is a valid int."""
    assert False


def test_get_api_request_base_api_url_invalid_type():
    """Check if base_api_url is a valid string."""
    assert False


def test_get_api_request_start_date_should_deny_negative():
    """Check if start_date accepts negative values."""
    assert False


def test_get_api_request_base_api_url_is_incorrect():
    """Check if base_api_url accepts a string that isn't bandcamp's."""
    assert False


def test_get_api_request_base_api_url_is_correct():
    """Test to see if the function performs successfully."""
    assert False


"""fetch_api_data tests"""


def test_fetch_api_data_output_is_correct_format():
    """Test that correctly executes the function."""
    assert False


def test_fetch_api_data_incorrect_type():
    """Test that checks if script halts on incorrect type parameter."""
    assert False


def test_fetch_api_data_incorrect_data_returned():
    """Test that checks if script halts if the format of dictionary data is not as expected."""
    assert False


"""export_api_data_to_csv tests"""


def test_export_api_data_to_csv_wrong_type_for_data():
    """Test that checks if script halts if the data type is not a dict."""
    assert False


def test_export_api_data_to_csv_wrong_type_for_filepath():
    """Test that checks if script halts if the file path isn't a string."""
    assert False


def test_export_api_data_to_csv_wrong_value_for_filepath():
    """Test that checks if script halts if the file path doesn't exist."""
    assert False


def test_export_api_data_to_csv_incorrect_data_returned():
    """Test that checks that csv isn't saved if the data is incorrectly formatted."""
    assert False


def test_export_api_data_to_csv_correct_data_returned():
    """Test that checks that csv is saved if the data is correctly formatted."""
    assert False


"""run_extract tests"""


def test_run_extract_wrong_type_for_filepath():
    """Test that checks if script halts if the file path isn't a string."""
    assert False


def test_run_extract_wrong_value_for_filepath():
    """Test that checks if script halts if the file path doesn't exist."""
    assert False


def test_run_extract_wrong_type_for_curr_time():
    """Test that checks if script halts if the curr_time isn't an int."""
    assert False


def test_run_extract_wrong_value_for_curr_time():
    """Test that checks if script halts if the curr_time is negative."""
    assert False


def test_run_extract_wrong_type_for_base_api_url():
    """Test that checks if script halts if base_api_url isn't a string."""
    assert False


def test_run_extract_empty_base_api_url():
    """Test that checks if script halts if base_api_url is empty."""
    assert False


def test_run_extract_incorrect_data_returned():
    """Test that checks that csv isn't saved if the data is incorrectly formatted."""
    assert False


def test_run_extract_correct_data_returned():
    """Test that checks that csv is saved if the data is correctly formatted."""
    assert False


"""get_time_offset tests"""


def tests_get_time_offset_passing_5000_should_return_4800():
    """Tests if the offset is working."""
    assert get_time_offset(5000) == 4800


def tests_get_time_offset_raises_errors():
    """Tests if the function errors as expected."""
    with pytest.raises(TypeError):
        get_time_offset('I am not an int')
