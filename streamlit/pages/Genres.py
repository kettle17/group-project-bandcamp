"""Artists page."""
import numpy as np
from os import environ as ENV
import pandas as pd
from dotenv import load_dotenv
import psycopg2
import streamlit as st
from Live_Data import get_connection

load_dotenv()

st.set_page_config(
    page_title="Genres",
    page_icon="🎶",
    layout="wide"
)


@st.cache_data
def load_genre_album_data(_conn):
    query = """
        SELECT * FROM tag
        JOIN album_tag_assignment USING (tag_id)
        JOIN album USING (album_id)
        JOIN sale_album_assignment USING (album_id)
        JOIN sale USING (sale_id)
        JOIN country USING (country_id);
    """
    df = pd.read_sql(query, _conn)
    _conn.close()
    df = df.drop(['country_id', 'sale_id',
                  'album_tag_assignment_id', 'album_assignment_id'], axis=1)
    return df


@st.cache_data
def load_genre_track_data(_conn):
    query = """
        SELECT * FROM tag
        JOIN track_tag_assignment USING (tag_id)
        JOIN track USING (track_id)
        JOIN sale_track_assignment USING (track_id)
        JOIN sale USING (sale_id)
        JOIN country USING (country_id);
        """
    df = pd.read_sql(query, _conn)
    _conn.close()
    df = df.drop(['country_id', 'sale_id',
                  'track_tag_assignment_id', 'track_assignment_id'], axis=1)
    return df


if __name__ == "__main__":
    conn = get_connection(
        ENV['DB_HOST'],
        ENV['DB_NAME'],
        ENV['DB_USER'],
        ENV['DB_PASSWORD'],
        ENV['DB_PORT']
    )

    genre_df = load_genre_album_data(conn)

    print(genre_df)
