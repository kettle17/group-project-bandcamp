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


def run_query(_conn, query):
    """Loads the required data by querying the database."""
    df = pd.read_sql(query, _conn)
    return df


def load_sale_data(_conn):
    """Loads the required data by querying the database."""
    query = """SELECT s.*, c.*, a.*, ar.* FROM sale s
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

    left_co, cent_co, last_co = st.columns(3)
    with cent_co:
        st.markdown("### Average Transaction Values")

    avg_track_sale = run_query(conn,
                               "SELECT ROUND(AVG(sold_for), 2) FROM sale_track_assignment")
    avg_album_sale = run_query(conn,
                               "SELECT ROUND(AVG(sold_for), 2) FROM sale_album_assignment")
    avg_merch_sale = run_query(conn,
                               "SELECT ROUND(AVG(sold_for), 2) FROM sale_merchandise_assignment")

    col1, col2, col3 = st.columns(3)
    col1.metric("Avg Track Sale", f"${avg_track_sale}")
    col2.metric("Avg Album Sale", f"${avg_album_sale}")
    col3.metric("Avg Merchandise Sale", f"${avg_merch_sale}")

    st.subheader("🔍 Filters")
    col1, col2, col3 = st.columns(3)
    with col1:
        country_options = sale_df['country_name'].dropna().unique()
        selected_countries = st.multiselect(
            "Country", options=country_options, default=None)
    with col2:
        artist_options = sale_df['artist_name'].dropna().unique()
        selected_artists = st.multiselect(
            "Artist", options=artist_options, default=None)

    with col3:
        sale_df['utc_date'] = pd.to_datetime(sale_df['utc_date'])
        start_date, end_date = st.date_input(
            "Sale Date Range",
            [sale_df['utc_date'].min(), sale_df['utc_date'].max()]
        )
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)

        filtered_df = sale_df[sale_df['utc_date'].between(
            start_date, end_date)]

    filtered_df = sale_df[
        (sale_df['country_name'].isin(selected_countries)) &
        (sale_df['artist_name'].isin(selected_artists)) &
        (sale_df['utc_date'].between(start_date, end_date))
    ]

    geo_df = geocode_countries(sale_df)
    st.map(geo_df, size=20, color="#0044ff")

    conn.close()
