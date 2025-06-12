# 🚰 ETL Pipeline

This directory contains the scripts used to extract, transform, and load BandCamp API data into an RDS database.
We will also use a web scraper to gain additional data such as tag data and release dates.

## 📁 File Structure

### ETL Scripts
- `extract.py` – Extracts Bandcamp sales data and saves to CSV  
- `transform.py` – Cleans and transforms data for loading  
- `load.py` – Loads transformed data into an RDS database  

### Tests
- `test_exist.py` – Test that checks existence of `extract.py`. Ensures pytest succeeds if there are no other tests. 
- `test_extract.py` – Tests for `extract.py`  
- `test_transform.py` – Tests for `transform.py`  
- `test_load.py` – Tests for `load.py` 

### Utilities & Scripts
- `utilities.py` – Helper functions used across ETL scripts  
- `requirements.txt` – Python dependencies  

- `test_extract.py` – Tests for `extract.py`  
- `test_transform.py` – Tests for `transform.py`  
- `test_load.py` – Tests for `load.py` 

