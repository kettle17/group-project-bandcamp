"""Streamlit page for genre page."""
import datetime
from os import environ as ENV

from Home import get_connection

import pandas as pd
import streamlit as st
from wordcloud import WordCloud


def local_css(file_name):
    """Connects to the style.css script to add a font."""
    with open(file_name) as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)


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
            JOIN artist_album_assignment USING (album_id)
            JOIN artist USING (artist_id)
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
            JOIN artist_track_assignment USING (track_id)
            JOIN artist USING (artist_id)
            JOIN sale_track_assignment USING (track_id)
            JOIN sale USING (sale_id)
            JOIN country USING (country_id)
            WHERE DATE(utc_date) BETWEEN %s AND %s;
            """
        df = pd.read_sql(query, conn, params=[date1, date2])

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
    top_countries_pivot = (
        tag_country_sales[tag_country_sales['rank']
                          <= 3].sort_values(['tag_id', 'rank'])
        .pivot(index='tag_id', columns='rank', values='country_name')
        .rename(columns={1.0: 'top_country_1', 2.0: 'top_country_2',
                         3.0: 'top_country_3'})
        .reset_index()
    )

    return tag_stats.merge(top_countries_pivot, on='tag_id', how='left')


def get_current_date_range(uni_key: str = None) -> (datetime.date, datetime.date):
    """Generate date range and return chosen dates."""
    date_range = st.date_input(
        "Select date range:",
        value=(datetime.date.today() -
               datetime.timedelta(days=1), datetime.date.today()),
        min_value=datetime.date(2025, 1, 1),
        max_value=datetime.date.today(),
        key=uni_key
    )
    if isinstance(date_range, tuple) and len(date_range) == 2:
        date1, date2 = date_range
    else:
        st.error("Please select both a start and end date.")
        st.stop()
    return date1, date2


def generate_wordcloud_genres(chosen_df: pd.DataFrame, chosen_metric: str) -> None:
    """Generates a wordcloud image for the dashboard, showing the most
    popular genres for the chosen arguments."""
    word_freq = dict(zip(chosen_df['tag_name'], chosen_df[chosen_metric]))
    wordcloud = WordCloud(width=800, height=400,
                          background_color='white').generate_from_frequencies(word_freq)
    image = wordcloud.to_image()
    st.image(
        image, use_container_width=True)


def get_3_by_3_top_albums(chosen_df: pd.DataFrame, selected_genre: str, album: bool):
    """Displays a 3x3 of the most popular items of the chosen genre in the selected period"""

    chosen_genre_df = chosen_df[chosen_df['tag_name'] == selected_genre]
    sales_df = (
        chosen_genre_df.groupby('album_name' if album else 'track_name')
        .agg(
            total_revenue=pd.NamedAgg(column='sold_for', aggfunc='sum'),
            sale_count=pd.NamedAgg(column='sale_id', aggfunc='count'),
            art_url=pd.NamedAgg(column='art_url', aggfunc='first'),
            url=pd.NamedAgg(column='url', aggfunc='first'),
            artist=pd.NamedAgg(column='artist_name', aggfunc='first')
        )
        .reset_index()
    )
    top_results = sales_df.sort_values(
        by='total_revenue', ascending=False).head(9)

    if top_results.empty:
        st.text("No albums found.")
    else:
        three_by_threecol1, three_by_threecol2, three_by_threecol3 = st.columns(
            3)
        columns = [three_by_threecol1, three_by_threecol2, three_by_threecol3]
        for i, (_, row) in enumerate(top_results.iterrows()):
            col = columns[i % 3]
            caption = row['caption'] if 'caption' in row else ""

            with col:
                st.markdown(
                    f"""
                    <div style="margin-bottom: 20px; width:100%; height:100%">
                        <a href="{row['url']}" target="_blank">
                            <img src="{row['art_url']}" style="; object-fit:cover; border-radius: 8px;" alt="{caption}" title="{row['artist']} - {row[('album_name' if album else 'track_name')]}" />
                        </a>
                        <div style="text-align:center; font-style:italic; color: #555;">{caption}</div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )


def return_genre_popularity_position(combined_df: pd.DataFrame, selected_genre: str) -> int:
    """Returns the placement of the genre for the chosen date period."""
    combined_df['popularity_rank'] = combined_df['sale_count'].rank(
        method='dense', ascending=False).astype(int)
    chosen_genre_df = combined_df[combined_df['tag_name'] == selected_genre]

    return int(chosen_genre_df['popularity_rank'].iloc[0])


def display_genre_menu() -> None:
    """Display genre menu section of Streamlit page."""
    genre_col1, genre_col2 = st.columns(2)

    with genre_col2:
        genre_date1, genre_date2 = get_current_date_range("genre_data_date")

    genre_album_data = load_genre_album_data(genre_date1, genre_date2)
    genre_track_data = load_genre_track_data(genre_date1, genre_date2)

    popular_track_genres = find_most_popular_tags(genre_track_data)
    popular_album_genres = find_most_popular_tags(genre_album_data)

    popular_album_and_track_genres = (
        pd.concat([popular_track_genres, popular_album_genres],
                  ignore_index=True)
        .groupby(['tag_id', 'tag_name'], as_index=False)
        .agg({
            'sale_count': 'sum',
            'total_revenue': 'sum',
            'top_country_1': 'first',
            'top_country_2': 'first',
            'top_country_3': 'first'
        })
    )

    with genre_col1:
        unique_tags = popular_album_and_track_genres['tag_name'].unique()
        selected_genre = st.selectbox("Select a genre", options=unique_tags)

    st.subheader(f"Genre data for {selected_genre} music")

    data_col1, data_col2 = st.columns(2)

    with data_col1:
        st.metric(label="Overall popularity",
                  value=f"#{return_genre_popularity_position(
                      popular_album_and_track_genres, selected_genre)}")
    with data_col2:
        st.metric(label="Amount sold",
                  value=f"£{(round(popular_album_and_track_genres.loc[popular_album_and_track_genres['tag_name']
                                                                      == selected_genre, 'total_revenue'].iloc[0], 2)):.2f}")

    chart_col1, chart_col2 = st.columns(2)

    with chart_col1:
        st.subheader("Popular albums right now in this genre")
        get_3_by_3_top_albums(genre_album_data, selected_genre, True)
    with chart_col2:
        st.subheader("Popular tracks right now in this genre")
        get_3_by_3_top_albums(genre_track_data, selected_genre, False)


def display_wordcloud_menu() -> None:
    """Display wordcloud menu section of Streamlit page."""
    col1, col2, col3 = st.columns(3)

    with col1:
        dataset_choice = st.selectbox(
            "Select data:", ["All", "Albums", "Tracks"], key="wordcloud_data_dataset")
    with col2:
        metric_labels = {
            "Quantity": "sale_count",
            "Total Sales": "total_revenue"
        }
        selected_label = st.selectbox("Query by:", list(metric_labels.keys()))
        metric_choice = metric_labels[selected_label]
    with col3:
        date1, date2 = get_current_date_range("wordcloud_data_date")

    genre_album_data_cloud = load_genre_album_data(date1, date2)
    genre_track_data_cloud = load_genre_track_data(date1, date2)
    popular_track_genres_cloud = find_most_popular_tags(genre_track_data_cloud)
    popular_album_genres_cloud = find_most_popular_tags(genre_album_data_cloud)
    popular_album_and_track_genres1 = pd.concat(
        [popular_track_genres_cloud, popular_album_genres_cloud], ignore_index=True)

    st.subheader("Word Cloud")

    if dataset_choice == "Albums":
        generate_wordcloud_genres(popular_album_genres_cloud, metric_choice)
    elif dataset_choice == "Tracks":
        generate_wordcloud_genres(popular_track_genres_cloud, metric_choice)
    else:
        generate_wordcloud_genres(
            popular_album_and_track_genres1, metric_choice)


if __name__ == "__main__":
    local_css("style.css")
    LOGO = "../documentation/tracktion_logo.png"
    st.logo(LOGO, size="large")

    st.markdown(
        "<h1 style='text-align: center;'>Genres</h1>", unsafe_allow_html=True)

    display_wordcloud_menu()
    display_genre_menu()
