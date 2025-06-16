"""Script that combines all individual parts of the ETL into one."""
# pylint: disable=broad-except

import pandas as pd
from dotenv import load_dotenv
from utilities import get_logger, set_logger
from extract import run_extract
from transform import clean_dataframe
from load import run_load


def run_pipeline() -> None:
    """Runs each stage of the pipeline in succession."""
    set_logger()
    logger = get_logger()
    load_dotenv('.env')

    try:
        logger.info("Extracting...")
        run_extract('data/output.csv')

        logger.info("Transforming...")
        raw_df = pd.read_csv('data/output.csv')
        clean_df = clean_dataframe(raw_df)

        logger.info("Loading...")
        run_load(clean_df)
        logger.info("Success!...")
    except Exception:
        logger.exception("Critical error. Stopping pipeline")


if __name__ == "__main__":
    run_pipeline()
