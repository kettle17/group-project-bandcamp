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
def load_sale_data(host, dbname, user, password, port):
    conn = psycopg2.connect(
        host=host,
        database=dbname,
        user=user,
        password=password,
        port=port
    )
    query = """SELECT s.* FROM sale s
    LEFT JOIN country c USING(country_id)
    LEFT JOIN sale_album_assignment saa USING(sale_id);"""
    df = pd.read_sql(query, conn)
    conn.close()
    return df


@st.cache_data
def load_album_data(host, dbname, user, password, port):
    conn = psycopg2.connect(
        host=host,
        database=dbname,
        user=user,
        password=password,
        port=port
    )
    query = """
    SELECT a.*, ar.artist_name FROM album a
    LEFT JOIN artist_album_assignment aaa USING(album_id)
    LEFT JOIN artist ar USING(artist_id);
    """
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
    sale_df = load_sale_data(
        ENV['DB_HOST'],
        ENV['DB_NAME'],
        ENV['DB_USERNAME'],
        ENV['DB_PASSWORD'],
        ENV['DB_PORT']
    )

    album_df = load_album_data(
        ENV['DB_HOST'],
        ENV['DB_NAME'],
        ENV['DB_USERNAME'],
        ENV['DB_PASSWORD'],
        ENV['DB_PORT']
    )

    st.title("🎶 Live Data Insights 🎶")
    st.subheader("🔍 Filters")

    geo_df = geocode_countries(sale_df)

    st.map(geo_df, size=20, color="#0044ff")

    st.title("🎨 Album Art Gallery")
    st.write(album_df)

    album_popularity = sale_df.groupby(
        'album_id').size().reset_index(name='num_sales')

    album_df = album_df.merge(album_popularity, on='album_id', how='left')
    album_df['num_sales'] = album_df['num_sales'].fillna(0).astype(int)

    album_df = album_df.dropna(
        subset=['art_url', 'url', 'album_name', 'artist_name'])
    album_df = album_df.sort_values(
        'num_sales', ascending=False).reset_index(drop=True)

    st.title("🔥 Album Art Heatmap by Popularity")

    num_cols = 4
    cols = st.columns(num_cols)

    for idx, row in album_df.iterrows():
        col = cols[idx % num_cols]

        count = row['num_sales']
        if count >= 10:
            heat_color = "#d73027"
        elif count >= 5:
            heat_color = "#fc8d59"
        elif count >= 2:
            heat_color = "#fee08b"
        else:
            heat_color = "#d9ef8b"

        caption = f"{row['artist_name']} – {row['album_name']} ({count} sale{'s' if count != 1 else ''})"

        with col:
            st.markdown(
                f"<div style='border: 3px solid {heat_color}; border-radius: 8px; padding: 5px'>", unsafe_allow_html=True)
            st.image(row['art_url'], use_column_width=True, caption=caption)
            st.markdown(
                f"[Open on Bandcamp]({row['url']})", unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)
