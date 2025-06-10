# 🚰 ETL Pipeline

This directory contains the scripts used to extract, transform, and load BandCamp API data into an RDS database.
We will also use a web scraper to gain additional data such as tag data and release dates.

## 📁 File Structure

- `extract.py`  
  A script that is used to extract API data from BandCamp's undocumented sales API and save it to a csv. 
- `load.py`
  A script used to load transformed data into the database.