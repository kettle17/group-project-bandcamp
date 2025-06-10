# pylint: skip-file

"""Test file for the extract pipeline."""

import pytest
import os
from extract import get_api_request, fetch_api_data, export_api_data_to_csv, run_extract, get_time_offset


def test_if_extract_exists():
    """Basic beginner test. If this test can't run, neither can the rest."""
    try:
        assert os.path.exists('pipeline/extract.py')
    except:
        assert os.path.exists('extract.py')


def test_offset_5000():
    """Tests if the offset is working."""
    assert get_time_offset(5000) == 4800


def test_offset_string():
    """Tests if the function errors as expected."""
    with pytest.raises(TypeError):
        get_time_offset('crazy')
