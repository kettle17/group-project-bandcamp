# pylint: skip-file

"""Test file for web scraper functions."""
import pytest
from bs4 import BeautifulSoup
from web_scraper import get_release_date, filter_tags, get_genres


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


@pytest.mark.parametrize("test_html, expected_date",
                         (
                             ("<div class='tralbumData tralbum-credits'> released June 11, 2025 "
                              "<br>\n written by C.Hammer",
                              "released June 11, 2025"),
                             ("<div class='tralbumData tralbum-credits'> released July 11, 2025"
                              " <br>\n written by C.Hammer"
                              " <br> produced by DayBehaviour", "released July 11, 2025")
                         ))
def test_get_release_date_returns_only_release_date(test_html, expected_date):
    """Tests that the get_release_date function filters html content to
     only return the release date and not other information that may also be
      present. """

    soup = BeautifulSoup(test_html, "html.parser")
    assert get_release_date(soup) == expected_date


@pytest.mark.parametrize("test_html, expected_genres",
                         (
                             ("<a class='tag'>electro</a><a class='tag'>pop</a>",
                              ["electro", "pop"]),
                             ("<a class='tag'>jazz</a><a class='tag'>Sweden</a>",
                              ["jazz"]),
                             ("<a class='tag'>electro</a><a class='tag'>pop</a><a class='tag'>techno</a>"
                              "\n <a class='tag'>Germany</a><a class='tag'>progressive</a>",
                              ["electro", "pop", "techno", "progressive"]),
                         ))
def test_get_genres_returns_only_genres(test_html, expected_genres):
    """Tests that the get_genres function filters html content to
     only return the genres and not other information that may also be
      present about country. """

    soup = BeautifulSoup(test_html, "html.parser")
    assert get_genres(soup) == expected_genres
