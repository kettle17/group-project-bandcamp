import pytest
from web_scraper import get_relevant_html, get_release_dates, filter_tags, get_genres


@pytest.mark.parametrize("tags, expected_genres",
                         (
                             (["techno", "electronic", "Sweden"], [
                                 "techno", "electronic"]),
                             (["techno", "Germany"], ["techno"]),
                             (["Germany", "Sweden"], []),
                             (["electro", "pop"], ["electro", "pop"])
                         ))
def test_get_filter_tags_returns_correct_tags(tags, expected_genres):
    """Tests that get_filter_tags filters out tags starting 
    with a capital letter."""

    assert filter_tags(tags) == expected_genres
