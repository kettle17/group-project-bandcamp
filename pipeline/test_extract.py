# pylint: skip-file

"""Test file for the extract pipeline."""

import pytest
import os
from extract import get_time_offset


def test_if_extract_exists():
    try:
        assert os.path.exists('pipeline/extract.py')
    except:
        assert os.path.exists('extract.py')


def test_offset_5000():
    assert get_time_offset(5000) == 4800


def test_offset_string():
    with pytest.raises(TypeError):
        get_time_offset('crazy')
