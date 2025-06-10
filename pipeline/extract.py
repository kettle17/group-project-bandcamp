"""Script for the extract part of the ETL."""


def get_api_request(start_date: int, base_api_url: str) -> dict:
    """Function that gets objects from a given API URL call.
    Separated from fetch_api_data to allow mocking."""
    pass


def fetch_api_data(start_date: int, base_api_url: str) -> dict:
    """Function that connects to the API and returns the data.
    Start date is a seconds from epoch int that gets called in the API under start_date.
    This is the report that gets called every 2 minutes."""
    pass


def save_api_data(api_data: dict, file_path: str) -> bool:
    """Function that takes the JSON and converts it into a base CSV file.
    Returns true or false based on if the API was able to be saved locally."""
    pass


def run_extract(start_date: int, file_path: str,
                base_api_url: str = "https://bandcamp.com/api/salesfeed/1/get?start_date="):
    """Function that runs the above three all in one."""
    pass


if __name__ == "__main__":
    run_extract()
