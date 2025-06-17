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
    page_title="Tracktion",
    page_icon="🎶",
    layout="wide"
)


def local_css(file_name):
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


def load_sale_data(_conn):
    query = """SELECT s.*, c.*, a.*, ar.* FROM sale s
    LEFT JOIN country c USING(country_id)
    LEFT JOIN sale_album_assignment saa USING(sale_id)
    LEFT JOIN album a USING(album_id)
    LEFT JOIN artist_album_assignment aaa USING(album_id)
    LEFT JOIN artist ar USING(artist_id);"""
    df = pd.read_sql(query, _conn)
    _conn.close()
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
    local_css("style.css")
    left_co, cent_co, last_co = st.columns(3)
    with cent_co:
        st.image("Tracktion (1).png")

    conn = get_connection(
        ENV['DB_HOST'],
        ENV['DB_NAME'],
        ENV['DB_USERNAME'],
        ENV['DB_PASSWORD'],
        ENV['DB_PORT']
    )
    sale_df = load_sale_data(conn)

    st.subheader("🔍 Filters")
    country_options = sale_df['country_name'].dropna().unique()
    selected_countries = st.multiselect(
        "Country", options=country_options, default=None)

    artist_options = sale_df['artist_name'].dropna().unique()
    selected_artists = st.multiselect(
        "Artist", options=artist_options, default=None)

    sale_df['utc_date'] = pd.to_datetime(sale_df['utc_date'])
    start_date, end_date = st.date_input(
        "Sale Date Range",
        [sale_df['utc_date'].min(), sale_df['utc_date'].max()]
    )

    filtered_df = sale_df[
        (sale_df['country_name'].isin(selected_countries)) &
        (sale_df['artist_name'].isin(selected_artists)) &
        (sale_df['utc_date'].between(start_date, end_date))
    ]

    geo_df = geocode_countries(sale_df)
    st.map(geo_df, size=20, color="#0044ff")
