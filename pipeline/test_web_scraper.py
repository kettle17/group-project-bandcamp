# pylint: skip-file

"""Test file for web scraper functions."""
from unittest.mock import Mock, patch
import pytest
from bs4 import BeautifulSoup
from web_scraper import get_relevant_html, get_release_date, filter_tags, get_genres, get_release_date_and_genres
from utilities import set_logger, get_logger

set_logger()
logger = get_logger()


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

    assert filter_tags(tags, logger) == expected_genres


@pytest.mark.parametrize("invalid_tags",
                         (
                             (123),
                             (None),
                             ("techno, electronic, pop"),
                             ("techno")
                         ))
def test_get_filter_tags_raises_error_if_invalid_tags_type(invalid_tags):
    """Tests that get_filter_tags raises an error when an invalid 
     tags type is provided."""
    with pytest.raises(TypeError):
        filter_tags(invalid_tags, logger)


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
    assert get_release_date(soup, logger) == expected_date


@pytest.mark.parametrize("test_html, expected_result",
                         (
                             ("<div class='tralbumData tralbum-credits'"
                              "<br>\n written by C.Hammer",
                              None),
                             ("<div class='tralbumData tralbum-credits'"
                              " <br>\n written by C.Hammer"
                              " <br> produced by DayBehaviour", None)
                         ))
def test_get_release_date_when_no_release_date_found(test_html, expected_result):
    """Tests that the get_release_date function return None if no release date
     found for given item. """

    soup = BeautifulSoup(test_html, "html.parser")
    assert get_release_date(soup, logger) == expected_result


@pytest.mark.parametrize("test_input",
                         (
                             ("<div class='tralbumData tralbum-credits'> released June 11, 2025 "
                              "<br>\n written by C.Hammer"),
                             ("<div class='tralbumData tralbum-credits'> released July 11, 2025"
                              " <br>\n written by C.Hammer"
                              " <br> produced by DayBehaviour"),
                             (["Not a BeautifulSoup Tag"])
                         ))
def test_get_release_date_if_invalid_input_given(test_input):
    """Tests that the get_release_date function raises an error if the
     input provided is not a BeautifulSoup Tag. """
    with pytest.raises(TypeError):
        get_release_date(test_input, logger)


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
    assert get_genres(soup, logger) == expected_genres


@pytest.mark.parametrize("test_html, expected_result",
                         (
                             ("<a class='tag'>Sweden</a><a class='tag'>Germany</a>",
                              []),
                             ("<a class='tag'>Argentina</a><a class='tag'>Sweden</a>",
                              []),
                             ("<a class='tag'>Not a genre</a> \n <a class='tag'>Germany</a>",
                              []),
                         ))
def test_get_genres_when_no_genres_found(test_html, expected_result):
    """Tests that the get_genres function returns an empty list when no
     genres were found."""

    soup = BeautifulSoup(test_html, "html.parser")
    assert get_genres(soup, logger) == expected_result


@patch("web_scraper.requests.get")
def test_get_release_date_and_genres_returns_expected_dict(mock_get_request):
    """Tests that the get_release_date_and_genres functions returns the expected 
    dict containing release date and genres."""

    test_html = """
    <div id='pgBd'>
    <div class='tralbumData tralbum-credits'>
    released June 11, 2024 \n <br>by Artist
    </div>
    <a class='tag'>electronic</a>
    <a class='tag'>Germany</a>
    <a class='tag'>pop</a>
    </div>"""
    mock_response = Mock()
    mock_response.content = test_html
    mock_get_request.return_value = mock_response

    result = get_release_date_and_genres("http://test.com", logger)

    assert result["release_date"] == "released June 11, 2024"
    assert result["genres"] == ["electronic", "pop"]
