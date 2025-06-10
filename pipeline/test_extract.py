# pylint: skip-file

"""Test file for the extract pipeline."""

import pytest
import os


def test_example_one():
    assert os.path.exists("extract.py")
