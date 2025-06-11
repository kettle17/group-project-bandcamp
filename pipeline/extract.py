# pylint: skip-file
"""Script for the extract part of the ETL."""

import time
import requests
import csv
import os
from utilities import get_logger, set_logger


def get_api_request(start_date: int,
                    bandcamp_url: str = "https://bandcamp.com/api/salesfeed/1/get?start_date=") -> dict:
    """Returns objects from a given API URL call.
    Separated from fetch_api_data to allow mocking."""
    if start_date < 0:
        raise ValueError("start_date is a negative value. Halting.")
    bandcamp_url += str(get_time_offset(start_date))

    response = requests.get(bandcamp_url, timeout=10)
    return response.json()


def fetch_api_data(start_date: int) -> dict:
    """Returns data fetched from BandCamp's API.
    Start date is a seconds from epoch int that gets called in the API under start_date.
    This is the report that gets called every 2 minutes."""
    api_data = get_api_request(start_date)
    if not api_data.get('start_date'):
        raise ValueError("API data did not return correctly.")
    return api_data


def export_api_data_to_csv(api_data: dict, file_path: str) -> bool:
    """Takes the dictionary contents and converts it into a base CSV file.
    Returns true or false based on if the API was able to be saved locally."""
    logger = get_logger()

    if not isinstance(file_path, str):
        raise TypeError("File path is not a string.")
    if not isinstance(api_data, dict):
        raise TypeError("Api data is not in the correct format.")
    if not os.path.isdir(os.path.dirname(file_path)):
        raise OSError("Folder path doesn't exist.")
    if file_path[-4:] != '.csv':
        raise ValueError("Path does not end in .csv.")
    if not api_data.get('start_date'):
        raise ValueError("API data did not return correctly.")
    if not api_data.get('events'):
        raise ValueError("API data did not return correctly.")

    list_of_items = []
    api_events = api_data['events']
    """Events comprise main API data
    For each event, we need to access 'items'
    and then output this to the csv per row"""
    all_keys = set()
    for event in api_events:
        for event_item in event['items']:
            list_of_items.append(event_item)
            all_keys.update(event_item.keys())
    keys = sorted(all_keys)
    logger.info("Updating data keys.")

    return save_to_csv(list_of_items, keys, file_path)


def save_to_csv(api_data: dict, keys: list, file_path: str) -> bool:
    """Creates a .csv file and writes the contents of the fetched API data
    to the file.
    Separated from export_api_data_to_csv to allow easy mocking."""
    try:
        with open(file_path, 'w', newline='', encoding='utf-8') as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(api_data)
    except Exception as exc:
        print(exc)
        return False
    return True


def run_extract(file_path: str,
                curr_time: int = int(time.time())) -> bool:
    """Runs all required extract functions in succession for the ETL pipeline."""
    api_data = fetch_api_data(curr_time)
    return export_api_data_to_csv(api_data, file_path)


def get_time_offset(curr_time: int = int(time.time()), offset: int = 200) -> int:
    """Returns current time in epoch format, minus time offset."""
    return curr_time - offset


if __name__ == "__main__":
    run_extract('data/output.csv')
