"""Main Streamlit dashboard for live data."""
from geopy.geocoders import Nominatim
import numpy as np
from os import environ as ENV
import pandas as pd
from dotenv import load_dotenv
import psycopg2
import streamlit as st


load_dotenv()

st.set_page_config(
    page_title="Live Data",
    page_icon="🎶",
    layout="wide"
)


@st.cache_resource
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
def load_sale_data(conn):
    query = """SELECT s.*, c.*, saa.* FROM sale s
    LEFT JOIN country c USING(country_id)
    LEFT JOIN sale_album_assignment saa USING(sale_id);"""
    df = pd.read_sql(query, conn)
    conn.close()
    return df


@st.cache_data
def geocode_countries(df):
    geolocator = Nominatim(user_agent="bandcamp-map")
    locations = []

    for country in df['country_name'].dropna().unique():
        location = geolocator.geocode(country)
        if location:
            locations.append(
                {'country_name': country, 'lat': location.latitude, 'lon': location.longitude})

    return pd.DataFrame(locations)


if __name__ == "__main__":
    conn = get_connection(
        ENV['DB_HOST'],
        ENV['DB_NAME'],
        ENV['DB_USERNAME'],
        ENV['DB_PASSWORD'],
        ENV['DB_PORT']
    )
    sale_df = load_sale_data(conn)

    st.title("Live Data Insights")
    st.subheader("🔍 Filters")

    geo_df = geocode_countries(sale_df)
    st.map(geo_df, size=20, color="#0044ff")
