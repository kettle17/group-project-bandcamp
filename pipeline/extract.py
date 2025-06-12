"""Script for the extract part of the ETL."""

import time
import csv
import os
from datetime import datetime

import requests

from utilities import get_logger, set_logger

BANDCAMP_API_URL = "https://bandcamp.com/api/salesfeed/1/get?start_date="


def get_api_request(start_date: int,
                    bandcamp_url: str = BANDCAMP_API_URL
                    ) -> dict:
    """Returns objects from a given API URL call.
    Separated from fetch_api_data to allow mocking."""
    logger = get_logger()

    start_date = get_time_offset(start_date)

    if start_date < 0:
        logger.critical("start_date is a negative value. Halting.")
        raise ValueError("start_date is a negative value. Halting.")
    bandcamp_url += str(start_date)
    logger.info("Retrieving BandCamp API report from 2 minutes ago (%s)...",
                datetime.fromtimestamp(start_date).strftime('%Y-%m-%d %H:%M:%S'))
    response = requests.get(bandcamp_url, timeout=10)
    if response.status_code != 200:
        logger.critical("Could not connect to BandCamp API.")
        raise ConnectionError("Could not connect to BandCamp API.")
    return response.json()


def fetch_api_data(start_date: int) -> dict:
    """Returns data fetched from BandCamp's API.
    Start date is a seconds from epoch int that gets called in the API under start_date.
    This is the report that gets called every 2 minutes."""
    logger = get_logger()
    api_data = get_api_request(start_date)
    if not api_data.get('start_date'):
        logger.critical("API data did not return correctly.")
        raise ValueError("API data did not return correctly.")
    return api_data


def validate_api_data(api_data: dict, file_path: str) -> str:
    """Validates API contents to make sure format is correct.
    Will execute with no issues if the format fulfills all validation conditions.
    Returns modified file path based on script directory."""
    logger = get_logger()

    logger.info("Validating API data...")

    if not isinstance(file_path, str):
        logger.critical("File path is not a string.")
        raise TypeError("File path is not a string.")
    if not isinstance(api_data, dict):
        logger.critical("Api data is not in the correct format.")
        raise TypeError("Api data is not in the correct format.")

    new_file_path = os.path.dirname(__file__) + '/' + file_path

    if not os.path.isdir(os.path.dirname(new_file_path)):
        logger.critical("Folder path doesn't exist.")
        raise OSError("Folder path doesn't exist.")
    if new_file_path[-4:] != '.csv':
        logger.critical("Path does not end in .csv.")
        raise ValueError("Path does not end in .csv.")
    if (not api_data.get('start_date') or not api_data.get('events')
            or not any(api_data['events'])):
        logger.critical("API data did not return correctly.")
        raise ValueError("API data did not return correctly.")
    logger.info("API data validated with no issues found!")
    return new_file_path


def collect_api_rows_and_columns(api_data: dict) -> tuple:
    """Takes the API contents and readies them to be saved to csv.
    Returns a tuple containing:
      a list of all API items to be inserted into the csv
      a list of all column keys that appeared during iteration."""
    logger = get_logger()
    logger.info("Collecting API contents...")
    item_rows = []
    api_events = api_data['events']
    logger.info("Grabbing columns...")
    all_keys = set()
    for event in api_events:
        for event_item in event['items']:
            item_rows.append(event_item)
            all_keys.update(event_item.keys())
    all_keys.add('addl_count')  # hardcode very rare column
    keys = sorted(all_keys)
    return (item_rows, keys)


def save_to_csv(api_data: dict, keys: list, file_path: str) -> bool:
    """Creates a .csv file and writes the contents of the fetched API data
    to the file.
    Separated from export_api_data_to_csv to allow easy mocking."""
    logger = get_logger()
    logger.info("Saving to %s...", file_path)
    try:
        with open(file_path, 'w', newline='', encoding='utf-8') as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(api_data)
    except OSError as e:
        logger.exception("File I/O error.")
        return False
    except csv.Error as e:
        logger.exception("CSV format error.")
        return False
    logger.info("Success!")
    return True


def run_extract(file_path: str,
                curr_time: int = int(time.time())) -> bool:
    """Runs all required extract functions in succession for the ETL pipeline."""
    api_data = fetch_api_data(curr_time)
    directory_file_path = validate_api_data(api_data, file_path)
    api_rows, api_columns = collect_api_rows_and_columns(api_data)
    return save_to_csv(api_rows, api_columns, directory_file_path)


def get_time_offset(curr_time: int = int(time.time()), offset: int = 200) -> int:
    """Returns current time in epoch format, minus time offset."""
    return curr_time - offset


if __name__ == "__main__":
    set_logger()
    run_extract('data/output.csv')
