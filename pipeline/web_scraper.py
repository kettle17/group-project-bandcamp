"""Script for the web scraper part of ETL."""
import requests
from bs4 import BeautifulSoup, Tag

from utilities import set_logger, get_logger


def filter_tags(tags: list[str], logger) -> list[str]:
    """Filters the tags associated with the given item to return genres
    and not country. Tags that start with lowercase letter are considered
    genres."""

    if not isinstance(tags, list):
        logger.error("Error: Unexpected type. "
                     " This functions expects a list.")
        raise TypeError("Input to filter tags must be a list.")
    genres = []
    for tag in tags:
        if tag[0].islower():
            logger.info(f"Tag is a genre: {tag}")
            genres.append(tag)
        else:
            logger.info(f"Tag is not a genre, most likely a location: {tag} ")
    return genres


def get_relevant_html(url: str, logger) -> Tag:
    """Returns the html element containing details on release date and tags."""

    logger.info(f"Retrieving page to be scraped from '{url}'")
    page = requests.get(url, timeout=5)
    soup = BeautifulSoup(page.content, "html.parser")
    results = soup.find(id="pgBd")

    if not results:
        logger.error("Could not find element with id 'pgBd in HTML.")
        raise ValueError("Could not find element with id 'pgBd in HTML.")

    logger.info("Relevant HTML content retrieved successfully.")
    return results


def get_release_date(results: Tag, logger) -> str:
    """Returns the release date for a given item."""

    if not isinstance(results, Tag):
        logger.error("Unexpected input. Results is not a BeautifulSoup Tag.")
        raise TypeError(
            "Unexpected input. Results is not a BeautifulSoup Tag.")

    release_date = results.find(
        "div", class_="tralbumData tralbum-credits")

    if release_date != None:
        splitlines = release_date.text.splitlines()
        for line in splitlines:
            if "released" in line.strip():
                return line.strip()

    logger.debug("No release date found in HTML content.")


def get_genres(results: Tag, logger) -> list[str]:
    """Returns a list of genres associated with the item."""
    if not isinstance(results, Tag):
        logger.error("Unexpected input. Results is not a BeautifulSoup Tag.")
        raise TypeError(
            "Unexpected input. Results is not a BeautifulSoup Tag.")

    tags_list = []
    tags = results.find_all("a", class_="tag")

    if not tags:
        logger.error("No tags found with the class 'tag' in HTML.")
    for tag in tags:
        tags_list.append(tag.text.strip())

    genres = filter_tags(tags_list, logger)

    if not genres:
        logger.info("No genre found given item.")

    return genres


def get_release_date_and_genres(url: str, logger) -> dict:
    """Scrapes the given web page and returns the release date and genres."""

    details = {}
    html = get_relevant_html(url, logger)
    item_release_date = get_release_date(html, logger)
    item_genres = get_genres(html, logger)

    details["release_date"] = item_release_date
    details["genres"] = item_genres

    return details


if __name__ == "__main__":
    set_logger()
    scraper_logger = get_logger()
    release_date_and_genres = get_release_date_and_genres(
        "https://daybehavior.bandcamp.com/track/silver-song", scraper_logger)
