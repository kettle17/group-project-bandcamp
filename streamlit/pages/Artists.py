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
    page_title="Artists",
    page_icon="🎶",
    layout="wide"
)


@st.cache_data
def load_album_data(conn):
    query = """
    SELECT a.*, ar.artist_name FROM album a
    LEFT JOIN artist_album_assignment aaa USING(album_id)
    LEFT JOIN artist ar USING(artist_id);
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


if __name__ == "__main__":
    conn = get_connection(
        ENV['DB_HOST'],
        ENV['DB_NAME'],
        ENV['DB_USERNAME'],
        ENV['DB_PASSWORD'],
        ENV['DB_PORT']
    )
    st.title("Album Art Gallery")

    album_popularity = sale_df.groupby(
        'album_id').size().reset_index(name='num_sales')
    album_df = album_df.merge(album_popularity, on='album_id', how='left')
    album_df['num_sales'] = album_df['num_sales'].fillna(0).astype(int)

    album_df = album_df.dropna(
        subset=['art_url', 'url', 'album_name', 'artist_name'])
    album_df = album_df.sort_values(
        'num_sales', ascending=False).reset_index(drop=True)

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
                f"""
                <div style='border: 3px solid {heat_color}; border-radius: 8px; padding: 5px'>
                    <a href="{row['url']}" target="_blank">
                        <img src="{row['art_url']}" style="width:100%;" alt="{caption}" />
                    </a>
                    <div style="text-align:center; font-style:italic;">{caption}</div>
                </div>
                """,
                unsafe_allow_html=True
            )
