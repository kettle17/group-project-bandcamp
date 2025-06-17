"""This script queries the database for the previous day's sales
and generates a pdf report."""

from os import environ as ENV
from datetime import date

from dotenv import load_dotenv
import psycopg2
from psycopg2.extensions import connection
from psycopg2.extras import RealDictCursor
import pandas as pd
from pandas import DataFrame
import altair as alt
from fpdf import FPDF


def get_db_connection() -> connection:
    """Returns a psycopg2 connection."""
    load_dotenv('.env')
    return psycopg2.connect(
        user=ENV["DB_USER"],
        password=ENV["DB_PASSWORD"],
        host=ENV["DB_HOST"],
        port=ENV["DB_PORT"],
        database=ENV["DB_NAME"],
        cursor_factory=RealDictCursor
    )


def get_top_artists_by_album_sales(conn):
    """Returns a dataframe of the top 10 artists by total revenue made from album sales."""
    with conn.cursor() as curs:
        curs.execute("""select artist_name, sum(sold_for)::float as total_revenue from 
    sale_album_assignment
    join sale 
    using (sale_id)
    join album 
    using (album_id)
    join artist_album_assignment
    using (album_id)
    join artist
    using (artist_id)
    group by artist_name
    order by total_revenue desc
    limit 10
    ;""")
        album_results = curs.fetchall()
        album_df = pd.DataFrame(album_results)
    return album_df


def get_top_artists_by_album_chart(df: DataFrame):
    """Returns a bar chart showing top 10 artists. """

    chart_top_artists = alt.Chart(df, title="Top 10 Artists by Album Revenue").mark_bar().encode(
        x=alt.X('total_revenue:Q', title="Total Revenue"),
        y=alt.Y("artist_name:N", sort="-x", title="Artist Name"),
        tooltip=["artist_name", "total_revenue"],
        color=alt.Color("artist_name").scale(scheme="goldgreen").legend(None)

    )
    return chart_top_artists


def get_top_artists_by_track_sales(conn):
    """Returns a dataframe of the top 10 artists by total revenue made from track sales."""

    with conn.cursor() as curs:
        curs.execute("""select artist_name, sum(sold_for)::float as total_revenue from 
    sale_track_assignment
    join sale 
    using (sale_id)
    join track 
    using (track_id)
    join artist_track_assignment
    using (track_id)
    join artist
    using (artist_id)
    group by artist_name
    order by total_revenue desc
    limit 10
    ;""")
        track_results = curs.fetchall()
        track_df = pd.DataFrame(track_results)
    return track_df


def get_top_artists_by_tracks_chart(df):

    top_artists_by_tracks = alt.Chart(df, title="Top 10 Artists by Track Revenue").mark_bar().encode(
        x=alt.X('total_revenue:Q', title="Total Revenue"),
        y=alt.Y("artist_name:N", sort="-x", title="Artist Name"),
        tooltip=["artist_name", "total_revenue"],
        color=alt.Color("artist_name").scale(scheme="goldred").legend(None)

    )

    return top_artists_by_tracks


def get_top_genres_by_album_sales(conn):
    """Returns the top 10 genres by total revenue made from album sales."""
    with conn.cursor() as curs:
        curs.execute("""select tag_name, sum(sold_for)::float as total_revenue from 
                    tag 
                    join album_tag_assignment 
                    using (tag_id)
                    join sale_album_assignment 
                    using (album_id)
                    join sale
                    using (sale_id)
                    group by tag_name
                    order by total_revenue desc
                    limit 10
                    """)
        album_tag_results = curs.fetchall()
        album_tag_df = pd.DataFrame(album_tag_results)
        return album_tag_df


def get_top_genres_by_album_chart(df):
    """Returns a bar chart showing top genres by album sales."""
    top_genres_by_albums = alt.Chart(df, title="Top 10 Genres by Album Revenue").mark_bar().encode(
        x=alt.X('total_revenue:Q', title="Total Revenue"),
        y=alt.Y("tag_name:N", sort="-x", title="Genre"),
        tooltip=["tag_name", "total_revenue"],
        color=alt.Color("tag_name").scale(scheme="goldred").legend(None)

    )
    return top_genres_by_albums


def get_top_genres_by_track_sales(conn):
    """Returns the top 10 genres by total revenue made from track sales."""

    with conn.cursor() as curs:
        curs.execute("""select tag_name, sum(sold_for)::float as total_revenue from 
                    tag 
                    join track_tag_assignment 
                    using (tag_id)
                    join sale_track_assignment 
                    using (track_id)
                    join sale
                    using (sale_id)
                    group by tag_name
                    order by total_revenue desc
                    limit 10
                    """)
        track_tag_results = curs.fetchall()
        track_tag_df = pd.DataFrame(track_tag_results)

        return track_tag_df


def get_top_genres_by_track_chart(df):
    """Returns a bar chart showing top genres by track sales."""

    top_genres_by_tracks = alt.Chart(df, title="Top 10 Genres by Track Revenue").mark_bar().encode(
        x=alt.X('total_revenue:Q', title="Total Revenue"),
        y=alt.Y("tag_name:N", sort="-x", title="Genre"),
        tooltip=["tag_name", "total_revenue"],
        color=alt.Color("tag_name").scale(scheme="goldred").legend(None)

    )

    return top_genres_by_tracks


def get_total_sale_transactions(conn):
    """Returns the total sale transactions made."""
    with conn.cursor() as curs:
        curs.execute("""select 
                    (select count(*) from sale_merchandise_assignment) +
                    (select count(*) from sale_album_assignment) +
                    (select count(*) from sale_track_assignment) as total_sales;""")
        sale_results = curs.fetchall()
        sale_df = pd.DataFrame(sale_results)

    return sale_df


def get_total_sale_transactions_categorised(conn):
    """Returns the total sale transactions made categorised by item type."""
    with conn.cursor() as curs:
        curs.execute("""select 'Merchandise' as Type, count(*) as count from sale_merchandise_assignment
                    union all
                    select 'Album', count(*) from sale_album_assignment
                    union all
                    select 'Track', count(*) from sale_track_assignment
                    order by count desc
                    ;""")
        sales_res = curs.fetchall()
        sale_by_item_df = pd.DataFrame(sales_res)

        return sale_by_item_df


def get_total_revenue_made(conn):
    """Returns the total revenue made from all sales."""
    with conn.cursor() as curs:
        curs.execute("""select 
                    (select sum(sold_for) from sale_merchandise_assignment) +
                    (select sum(sold_for) from sale_album_assignment) +
                    (select sum(sold_for) from sale_track_assignment) as total_sales;""")
        revenue_results = curs.fetchall()
        revenue_df = pd.DataFrame(revenue_results)
        return revenue_df


def get_total_revenue_made_categorised(conn):
    """Returns the total revenue made from all sales categorsised by item type."""
    with conn.cursor() as curs:
        curs.execute("""select 'Merchandise' as Type, sum(sold_for)::float as total_revenue from sale_merchandise_assignment
                    union all
                    select 'Album', sum(sold_for) from sale_album_assignment
                    union all
                    select 'Track', sum(sold_for) from sale_track_assignment
                    order by total_revenue desc
                    ;""")
        revenue_res = curs.fetchall()
        rev_df = pd.DataFrame(revenue_res)
    return rev_df


if __name__ == "__main__":

    conn = get_db_connection()
    top_artists_by_album_df = get_top_artists_by_album_sales(conn)
    top_artists_by_track_df = get_top_artists_by_track_sales(conn)
    top_genres_by_album = get_top_genres_by_album_sales(conn)
    top_genres_by_track = get_top_genres_by_track_sales(conn)

    total_sales = get_total_sale_transactions(conn)
    total_sales_categorised = get_total_sale_transactions_categorised(conn)
    total_revenue = get_total_revenue_made(conn)
    total_revenue_categorised = get_total_revenue_made_categorised(conn)

    top_artists_by_album_chart = get_top_artists_by_album_chart(
        top_artists_by_album_df)
    top_artists_by_track_chart = get_top_artists_by_tracks_chart(
        top_artists_by_track_df)
    top_genres_by_album = get_top_genres_by_album_chart(top_genres_by_album)
    top_genres_by_track = get_top_genres_by_track_chart(top_genres_by_track)

    top_artists_by_album_chart.save("top_artists_by_album.png", "png")
    top_artists_by_track_chart.save("top_artists_by_track.png", "png")
    top_genres_by_album.save("top_genres_by_album.png", "png")
    top_genres_by_track.save("top_genres_by_track.png", "png")

    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", "B", 24)
    title = "Daily Report for Top 10 Artists and Genres"
    pdf.cell(0, 20, title, align="C", ln=1)

    image_list = ["top_artists_by_album.png", "top_artists_by_track.png",
                  "top_genres_by_album.png", "top_genres_by_track.png"]

    for image in image_list:
        pdf.image(image)
    pdf.output(f"daily_report_{date.today()}.pdf", "F")
