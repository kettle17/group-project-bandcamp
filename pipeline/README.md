# 🚰 ETL Pipeline

This directory contains the scripts used to extract, transform, and load BandCamp API data into an RDS database.
We will also use a web scraper to gain additional data such as tag data and release dates.

## 📁 File Structure

### 📃 ETL Scripts

- `extract.py` – Extracts Bandcamp sales data and saves to CSV  
- `web_scraper.py` - Extracts data by scraping the api.
- `transform.py` – Cleans and transforms data for loading  
- `load.py` – Loads transformed data into an RDS database 
- `etl_controller.py` - Runs all stages of the ETL pipeline (contains Lambda Handler).

### 🧪 Tests
- `test_exist.py` – Test that checks existence of `extract.py`. Ensures pytest succeeds if there are no other tests. 
- `test_extract.py` – Tests for `extract.py`  
- `test_transform.py` – Tests for `transform.py`  
- `test_load.py` – Tests for `load.py` 
- `test_etl_controller.py` - Tests for `etl_controller.py`
- `test_web_scraper.py` - Tests for `web_scraper.py`

### 🛠️ Utilities & Other Scripts
- `utilities.py` – Helper functions used across ETL scripts  
- `requirements.txt` – Python dependencies  
- `Dockerfile` - File for dockerising the ETL pipeline.
- `conftest.py` - Contains all of the fixtures used in the tests.
- `schema.sql` - Script containing all of the queries required to create the database.


### 🏃💨 Instructions
When you have the .env information, simply run the following in the command line.
```
python3 etl_controller.py
```

