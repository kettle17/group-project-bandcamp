# pylint: skip-file

"""Test file that stops pytest from failing if there are no other tests."""
import pytest
import os


def test_if_extract_exists_should_exist():
    """Basic beginner test. If this test can't run, neither can the rest."""
    try:
        assert os.path.exists('pipeline/extract.py')
    except:
        assert os.path.exists('extract.py')
