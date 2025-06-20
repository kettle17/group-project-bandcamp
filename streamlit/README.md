# 📊📈 Streamlit 📈📊

This directory contains the Streamlit configuration, used to deploy a web application that will display many different charts and visualisations generated from the data received from the RDS setup. This will visualise BandCamp data in real-time using AWS, Python, and PostgreSQL.

---

## 📁 File Structure

```bash
streamlit/
├── .streamlit/                   # Folder containing Streamlit theme configurations and secrets.
├── Home.py                       # Main Streamlit page containing the live artist data.
├── pages/                        # Folder containing sub-Streamlit pages.
    ├── Album_Art_Gallery.py      # Page showing a gallery of all of the album covers.
    ├── Alerts.py                 # Page showing the submission page for the alerts and daily reports.
    ├── Genres.py                 # Page showing all of the analytics for the genres.
    └── Privacy_Policy.py         # Page containing the projects privacy policy.
├── visualisations/               # Folder containing different visualisations for various charts.
└── style.css                     # CSS script responsible for the dashboard font styling.
```
---

## 🔧 Prerequisites
```
pip3 install streamlit
```
## ⚙ Instructions
```
streamlit run Live_Data.py
```
