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


def tests_get_time_offset_passing_5000_should_return_4800():
    """Tests if the offset is working."""
    assert get_time_offset(5000) == 4800


def tests_get_time_offset_raises_errors():
    """Tests if the function errors as expected."""
    with pytest.raises(TypeError):
        get_time_offset('crazy')
