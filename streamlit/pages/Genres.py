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
    """Gets tag album data from the RDS.
    Combines all relevant tables for querying, removing unneeded IDs"""
    query = """
        SELECT * FROM tag
        JOIN album_tag_assignment USING (tag_id)
        JOIN album USING (album_id)
        JOIN sale_album_assignment USING (album_id)
        JOIN sale USING (sale_id)
        JOIN country USING (country_id);
    """
    df = pd.read_sql(query, _conn)
    df = df.drop(['country_id',
                  'album_tag_assignment_id', 'album_assignment_id'], axis=1)
    return df


@st.cache_data
def load_genre_track_data(_conn):
    """Gets tag track data from the RDS.
    Combines all relevant tables for querying, removing unneeded IDs"""
    query = """
        SELECT * FROM tag
        JOIN track_tag_assignment USING (tag_id)
        JOIN track USING (track_id)
        JOIN sale_track_assignment USING (track_id)
        JOIN sale USING (sale_id)
        JOIN country USING (country_id);
        """
    df = pd.read_sql(query, _conn)

    df = df.drop(['country_id',
                  'track_tag_assignment_id', 'track_assignment_id'], axis=1)
    return df


def find_most_popular_tags(sales: pd.DataFrame) -> pd.DataFrame:
    """Filters the most popular tags for albums/tracks,
    returning a Dataframe of the tag, the quantity it appeared,
    the overall sale value, and the top 3 countries for it."""
    sales_unique = sales.drop_duplicates(
        subset=['sale_id', 'tag_id'])
    tag_stats = (sales_unique.groupby(['tag_id', 'tag_name']).agg(
        sale_count=('sale_id', 'nunique'),
        total_revenue=('sold_for', 'sum')
    ).reset_index()
    )
    tag_country_sales = (sales_unique.groupby(['tag_id', 'country_name']).agg(
        country_sales=('sale_id', 'nunique')).reset_index()
    )
    tag_country_sales['rank'] = tag_country_sales.groupby('tag_id')['country_sales'] \
        .rank(method='first', ascending=False)
    top_countries_pivot = (tag_country_sales[tag_country_sales['rank'] <= 3].sort_values(['tag_id', 'rank'])
                           .pivot(index='tag_id', columns='rank', values='country_name')
                           .rename(columns={1.0: 'top_country_1', 2.0: 'top_country_2', 3.0: 'top_country_3'})
                           .reset_index()
                           )

    return tag_stats.merge(top_countries_pivot, on='tag_id', how='left')


if __name__ == "__main__":
    conn = get_connection(
        ENV['DB_HOST'],
        ENV['DB_NAME'],
        ENV['DB_USER'],
        ENV['DB_PASSWORD'],
        ENV['DB_PORT']
    )

    genre_album_data = load_genre_album_data(conn)
    genre_track_data = load_genre_track_data(conn)

    popular_track_genres = find_most_popular_tags(genre_track_data)
    popular_album_genres = find_most_popular_tags(genre_album_data)

    print(popular_album_genres.sort_values(by='sale_count', ascending=False))
    print(popular_track_genres.sort_values(by='sale_count', ascending=False))

    conn.close()
