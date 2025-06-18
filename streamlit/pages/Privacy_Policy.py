"""Main Streamlit dashboard for live data."""
from geopy.geocoders import Nominatim
import numpy as np
from os import environ as ENV
import pandas as pd
from dotenv import load_dotenv
import psycopg2
import streamlit as st

st.set_page_config(
    page_title="Tracktion",
    page_icon="🎶",
    layout="wide"
)


def local_css(file_name):
    """Connects to the style.css script to add a font."""
    with open(file_name) as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)


def get_connection(host, dbname, user, password, port):
    """Create and cache a SQL Server connection using pyodbc."""
    connection = psycopg2.connect(
        host=host,
        database=dbname,
        user=user,
        password=password,
        port=port
    )
    return connection


@st.cache_data
def run_query(_conn, query):
    """Loads the required data by querying the database."""
    df = pd.read_sql(query, _conn)
    return df


@st.cache_data
def load_sale_data(_conn):
    """Loads the required data by querying the database."""
    query = """SELECT s.*, c.*, a.*, ar.*, saa.* FROM sale s
    LEFT JOIN country c USING(country_id)
    LEFT JOIN sale_album_assignment saa USING(sale_id)
    LEFT JOIN album a USING(album_id)
    LEFT JOIN artist_album_assignment aaa USING(album_id)
    LEFT JOIN artist ar USING(artist_id);"""
    df = pd.read_sql(query, _conn)
    return df


@st.cache_data
def geocode_countries(df):
    """Loads every country into a dataframe to use on the map visualisation."""
    geolocator = Nominatim(user_agent="bandcamp-map", timeout=10)
    locations = []

    for country in df['country_name'].dropna().unique():
        location = geolocator.geocode(country)
        if location:
            locations.append(
                {'country_name': country, 'lat': location.latitude, 'lon': location.longitude})

    return pd.DataFrame(locations)


if __name__ == "__main__":

    local_css("style.css")
    LOGO = "../documentation/tracktion_logo.png"

    left_co, cent_co, last_co = st.columns(3)
    with cent_co:
        st.image(LOGO)
        st.title("Privacy Policy")
    st.text("""
    1. Introduction

    This dashboard ("Service") is operated by [Your Company/Name] ("we", "our", or "us"). This Privacy Policy explains how we collect, use, and protect your personal data in compliance with the General Data Protection Regulation (GDPR).
    
    2. What Data We Collect

    We may collect and store the following personal data:

        Email address
        Phone number

    3. Purpose of Data Collection

    We collect personal data for the following legitimate purposes:

        To contact users regarding insights, alerts and updates related to the dashboard

        To provide customer support or service-related communication

        To improve the functionality and user experience of the dashboard

    We do NOT sell or share personal data with third parties for marketing purposes.
            
    4. Legal Basis for Processing

    We process your data based on:

        Consent: You have given explicit consent via. the alert forms.

        Legitimate interest: For functionality, support, or performance analysis.

    You can withdraw consent at any time by contacting us at tracktionanalytics@sigmalabs.co.uk
            
    5. Data Storage & Security

        Personal data is stored securely and access is restricted to authorized personnel only.

        Data may be stored on third-party infrastructure (e.g. AWS SNS/SES).

        We use encryption and secure protocols where appropriate.

    6. Data Retention

    We retain personal data only as long as necessary for the purposes stated. You can request deletion of your data at any time.
    
    7. Your Rights Under GDPR

    You have the right to:

        Access the personal data we hold about you

        Request correction or deletion of your data

        Object to or restrict certain types of processing

        File a complaint with a data protection authority

    To exercise your rights, contact us at tracktionanalytics@sigmalabs.co.uk.
            
    8. Cookies and Analytics

    If applicable, we may use cookies or analytics tools to monitor usage, but we do not use them for profiling or targeted advertising.
    
    9. Changes to This Policy

    We may update this policy occasionally. Changes will be posted here with an updated effective date.
    
    10. Contact Us

    If you have any questions or concerns about this Privacy Policy or your data, please contact:

    Tracktion Analytics
    Email: tracktionanalytics@sigmalabs.co.uk
    Phone: 19930 123456
    Address: 123 Fake Street""")
