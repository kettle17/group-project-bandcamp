# pylint: skip-file
"""Script for the extract part of the ETL."""

import time


def get_api_request(start_date: int, base_api_url: str) -> dict:
    """Returns objects from a given API URL call.
    Separated from fetch_api_data to allow mocking."""
    start_date -= 200  # subtract 200 from current time so report is generated
    pass


def fetch_api_data(start_date: int, base_api_url: str) -> dict:
    """Returns data fetched from BandCamp's API.
    Start date is a seconds from epoch int that gets called in the API under start_date.
    This is the report that gets called every 2 minutes."""
    pass


def export_api_data_to_csv(api_data: dict, file_path: str) -> bool:
    """Takes the dictionary contents and converts it into a base CSV file.
    Returns true or false based on if the API was able to be saved locally."""
    pass


def run_extract(file_path: str,
                curr_time: int = int(time.time()),
                base_api_url: str = "https://bandcamp.com/api/salesfeed/1/get?start_date="):
    """Runs all required extract functions in succession for the ETL pipeline."""
    pass


def get_time_offset(curr_time: int = int(time.time()), offset: int = 200) -> int:
    """Returns current time in epoch format, minus time offset."""
    return curr_time - offset


if __name__ == "__main__":
    run_extract('')
