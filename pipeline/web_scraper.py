"""Script for the web scraper part of ETL."""
import requests
from bs4 import BeautifulSoup


def filter_tags(tags: list[str]) -> list[str]:
    """Returns a list of filtered tags representing genres from an item listing.
    and not country."""

    genres = []
    for tag in tags:
        if tag[0].islower():
            genres.append(tag)
        else:
            continue
    return genres


def get_relevant_html(url: str):
    """Returns the html element containing details on release date and tags."""
    page = requests.get(url, timeout=5)
    soup = BeautifulSoup(page.content, "html.parser")
    results = soup.find(id="pgBd")
    return results


def get_release_date(results) -> str:
    """Returns the release date for a given item."""
    release_date = results.find(
        "div", class_="tralbumData tralbum-credits")

    splitlines = release_date.text.splitlines()
    for line in splitlines:
        if "released" in line.strip():
            return line.strip()


def get_genres(results) -> list[str]:
    """Returns a list of genres associated with the item."""
    tags_list = []
    tags = results.find_all("a", class_="tag")

    for tag in tags:
        tags_list.append(tag.text.strip())

    genres = filter_tags(tags_list)

    genres = filter_tags(tags_list)
    return genres


if __name__ == "__main__":
    html = get_relevant_html(
        "https://daybehavior.bandcamp.com/track/silver-song")
    item_release_date = get_release_date(html)
    item_genres = get_genres(html)
