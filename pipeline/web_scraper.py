import requests
from bs4 import BeautifulSoup


def filter_tags(tags: list[str]) -> list[str]:
    """Filters the tags associated with the given item to return genres
    and not country."""

    genres = []
    for tag in tags:
        if tag[0].islower():
            genres.append(tag)
    return genres


def get_relevant_html(url):
    """Returns the html element containing details on release date and tags."""
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")
    results = soup.find(id="pgBd")
    return results


def get_release_dates(results):
    """Returns the release date for a given item."""
    release_dates = results.find_all(
        "div", class_="tralbumData tralbum-credits")

    for release_date in release_dates:
        splitlines = release_date.text.splitlines()
        for line in splitlines:
            if "released" in line:
                return line.strip()


def get_genres(results):
    """Returns a list of genres associated with the item."""
    tags_list = []
    tags = results.find_all("a", class_="tag")
    for tag in tags:
        splitlines = tag.text.splitlines()
        tags_list.append(splitlines[0])

    genres = filter_tags(tags_list)
    return genres


if __name__ == "__main__":
    html = get_relevant_html(
        "https://daybehavior.bandcamp.com/track/silver-song")
    release = get_release_dates(html)
    genres = get_genres(html)
    print(release)
    print(genres)
