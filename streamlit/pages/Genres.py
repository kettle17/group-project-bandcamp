"""Artists page."""
from Live_Data import get_connection
import numpy as np
from os import environ as ENV
import pandas as pd
from dotenv import load_dotenv
import psycopg2
import streamlit as st
import altair as alt
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import datetime
import random

load_dotenv()

st.set_page_config(
    page_title="Genres",
    page_icon="🎶",
    layout="wide"
)


def get_fresh_connection():
    """Reconnect with connection if closed or not usable."""
    conn = get_connection(
        ENV['DB_HOST'],
        ENV['DB_NAME'],
        ENV['DB_USER'],
        ENV['DB_PASSWORD'],
        ENV['DB_PORT']
    )
    if conn.closed != 0:
        st.cache_resource.clear()
        conn = get_connection(
            ENV['DB_HOST'],
            ENV['DB_NAME'],
            ENV['DB_USER'],
            ENV['DB_PASSWORD'],
            ENV['DB_PORT']
        )
    return conn


@st.cache_data
def load_genre_album_data(date1: datetime.date, date2: datetime.date) -> pd.DataFrame:
    """Gets tag album data from the RDS for chosen date range.
    Combines all relevant tables for querying, removing unneeded IDs"""

    with get_fresh_connection() as conn:
        query = """
            SELECT * FROM tag
            JOIN album_tag_assignment USING (tag_id)
            JOIN album USING (album_id)
            JOIN sale_album_assignment USING (album_id)
            JOIN sale USING (sale_id)
            JOIN country USING (country_id)
            WHERE DATE(utc_date) BETWEEN %s AND %s;
            """
        df = pd.read_sql(query, conn, params=[date1, date2])
        df = df.drop(['country_id',
                      'album_tag_assignment_id', 'album_assignment_id'], axis=1)
        return df


@st.cache_data
def load_genre_track_data(date1: datetime.date, date2: datetime.date):
    """Gets tag track data from the RDS for chosen date range.
    Combines all relevant tables for querying, removing unneeded IDs"""
    with get_fresh_connection() as conn:
        query = """
            SELECT * FROM tag
            JOIN track_tag_assignment USING (tag_id)
            JOIN track USING (track_id)
            JOIN sale_track_assignment USING (track_id)
            JOIN sale USING (sale_id)
            JOIN country USING (country_id)
            WHERE DATE(utc_date) BETWEEN %s AND %s;
            """
        df = pd.read_sql(query, conn, params=[date1, date2])

        df = df.drop(['country_id',
                      'track_tag_assignment_id', 'track_assignment_id'], axis=1)
        return df


def find_genre_position(curr_df: pd.DataFrame) -> pd.DataFrame:
    """Used to evaluate the position for chosen genre for chosen date range."""
    pass


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


def generate_wordcloud_genres(chosen_df: pd.DataFrame, chosen_metric: str) -> None:
    """Generates a wordcloud image for the dashboard, showing the most
    popular genres for the chosen arguments."""
    word_freq = dict(zip(chosen_df['tag_name'], chosen_df[chosen_metric]))
    wordcloud = WordCloud(width=800, height=400,
                          background_color='white').generate_from_frequencies(word_freq)
    image = wordcloud.to_image()
    st.image(
        image, use_container_width=True)


if __name__ == "__main__":

    st.title("Genres")

    date_range = st.date_input(
        "Select date range:",
        value=(datetime.date.today() -
               datetime.timedelta(days=7), datetime.date.today()),
        min_value=datetime.date(2025, 1, 1),
        max_value=datetime.date.today()
    )
    # --- Validate and unpack dates ---
    if isinstance(date_range, tuple) and len(date_range) == 2:
        date1, date2 = date_range
    else:
        st.error("Please select both a start and end date.")
        st.stop()

    genre_album_data = load_genre_album_data(date1, date2)
    genre_track_data = load_genre_track_data(date1, date2)

    popular_track_genres = find_most_popular_tags(genre_track_data)
    popular_album_genres = find_most_popular_tags(genre_album_data)
    popular_album_and_track_genres = pd.concat(
        [popular_track_genres, popular_album_genres], ignore_index=True)

    st.subheader("Word Cloud")

    col1, col2 = st.columns(2)

    with col1:
        dataset_choice = st.selectbox(
            "Select data:", ["All", "Albums", "Tracks"])
    with col2:
        metric_labels = {
            "Quantity": "sale_count",
            "Total Sales": "total_revenue"
        }
        selected_label = st.selectbox("Query by:", list(metric_labels.keys()))
        metric_choice = metric_labels[selected_label]
    if dataset_choice == "Albums":
        generate_wordcloud_genres(popular_album_genres, metric_choice)
    elif dataset_choice == "Tracks":
        generate_wordcloud_genres(popular_track_genres, metric_choice)
    else:
        generate_wordcloud_genres(
            popular_album_and_track_genres, metric_choice)

    # print(popular_album_genres.sort_values(by='sale_count', ascending=False))
    # print(popular_track_genres.sort_values(by='sale_count', ascending=False))
