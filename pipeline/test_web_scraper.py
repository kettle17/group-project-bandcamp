from web_scraper import get_relevant_html, get_release_dates, filter_tags, get_genres


def test_get_filter_tags_returns_correct_tags():
    """Tests that get_filter_tags filters out tags starting 
    with a capital letter."""

    tags = ["techno", "electronic", "Sweden"]

    assert filter_tags(tags) == ["techno", "electronic"]
