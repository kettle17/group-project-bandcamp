# pylint: skip-file

"""Test file for the extract pipeline."""

import pytest
import os


def test_if_extract_exists():
    assert os.path.exists('pipeline/extract.py')
