"""This script contains the functions for querying the database and making charts."""

from psycopg2.extensions import connection
import pandas as pd
from pandas import DataFrame
import altair as alt


def get_top_artists_by_album_sales(conn: connection) -> DataFrame:
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
    WHERE UTC_DATE::DATE = CURRENT_DATE - INTERVAL '1 DAY'
    group by artist_name
    order by total_revenue desc
    limit 10
    ;""")
        album_results = curs.fetchall()
        album_df = pd.DataFrame(album_results)
    return album_df


def get_top_artists_by_album_chart(df: DataFrame) -> alt.Chart:
    """Returns a bar chart showing top 10 artists. """

    chart_top_artists = alt.Chart(df, title="Top 10 Artists by Album Revenue").mark_bar().encode(
        x=alt.X('total_revenue:Q', title="Total Revenue"),
        y=alt.Y("artist_name:N", sort="-x", title="Artist Name"),
        tooltip=["artist_name", "total_revenue"],
        color=alt.Color("artist_name").scale(scheme="goldgreen").legend(None)

    )
    return chart_top_artists


def get_top_artists_by_track_sales(conn: connection) -> DataFrame:
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
    WHERE UTC_DATE::DATE = CURRENT_DATE - INTERVAL '1 DAY'
    group by artist_name
    order by total_revenue desc
    limit 10
    ;""")
        track_results = curs.fetchall()
        track_df = pd.DataFrame(track_results)
    return track_df


def get_top_artists_by_tracks_chart(df: DataFrame) -> alt.Chart:
    """Returns a bar chart of the top 10 artists by track revenue."""
    top_artists_by_tracks = alt.Chart(df, title="Top 10 Artists by Track Revenue").mark_bar().encode(
        x=alt.X('total_revenue:Q', title="Total Revenue"),
        y=alt.Y("artist_name:N", sort="-x", title="Artist Name"),
        tooltip=["artist_name", "total_revenue"],
        color=alt.Color("artist_name").scale(scheme="goldred").legend(None)

    )

    return top_artists_by_tracks


def get_top_genres_by_album_sales(conn: connection) -> DataFrame:
    """Returns the top 10 genres by total revenue made from album sales."""
    with conn.cursor() as curs:
        curs.execute("""select tag_name,
                    sum(sold_for)::float as total_revenue from
                    tag 
                    join album_tag_assignment 
                    using (tag_id)
                    join sale_album_assignment 
                    using (album_id)
                    join sale
                    using (sale_id)
                    WHERE UTC_DATE::DATE = CURRENT_DATE - INTERVAL '1 DAY'
                    group by tag_name
                    order by total_revenue desc
                    limit 10
                    """)
        album_tag_results = curs.fetchall()
        album_tag_df = pd.DataFrame(album_tag_results)
        return album_tag_df


def get_top_genres_by_album_chart(df: DataFrame) -> alt.Chart:
    """Returns a bar chart showing top genres by album sales."""
    top_genres_by_albums = alt.Chart(df, title="Top 10 Genres by Album Revenue").mark_bar().encode(
        x=alt.X('total_revenue:Q', title="Total Revenue"),
        y=alt.Y("tag_name:N", sort="-x", title="Genre"),
        tooltip=["tag_name", "total_revenue"],
        color=alt.Color("tag_name").scale(scheme="goldred").legend(None)

    )
    return top_genres_by_albums


def get_top_genres_by_track_sales(conn: connection) -> DataFrame:
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
                    WHERE UTC_DATE::DATE = CURRENT_DATE - INTERVAL '1 DAY'
                    group by tag_name
                    order by total_revenue desc
                    limit 10
                    """)
        track_tag_results = curs.fetchall()
        track_tag_df = pd.DataFrame(track_tag_results)

        return track_tag_df


def get_top_genres_by_track_chart(df: DataFrame) -> alt.Chart:
    """Returns a bar chart showing top genres by track sales."""

    top_genres_by_tracks = alt.Chart(df, title="Top 10 Genres by Track Revenue").mark_bar().encode(
        x=alt.X('total_revenue:Q', title="Total Revenue"),
        y=alt.Y("tag_name:N", sort="-x", title="Genre"),
        tooltip=["tag_name", "total_revenue"],
        color=alt.Color("tag_name").scale(scheme="goldred").legend(None)

    )

    return top_genres_by_tracks


def get_total_sale_transactions(conn: connection) -> dict:
    """Returns the total sale transactions made."""
    with conn.cursor() as curs:
        curs.execute("""select
                    (select count(*) from sale_merchandise_assignment
                    JOIN sale using (sale_id)
                    WHERE UTC_DATE::DATE = CURRENT_DATE - INTERVAL '1 DAY') +
                    (select count(*) from sale_album_assignment
                    JOIN sale using (sale_id)
                    WHERE UTC_DATE::DATE = CURRENT_DATE - INTERVAL '1 DAY') +
                    (select count(*) from sale_track_assignment
                    JOIN sale using (sale_id)
                    WHERE UTC_DATE::DATE = CURRENT_DATE - INTERVAL '1 DAY') as total_sales;""")
        sale_results = curs.fetchall()
        return sale_results


def get_total_sale_transactions_categorised(conn: connection) -> dict:
    """Returns the total sale transactions made categorised by item type."""
    with conn.cursor() as curs:
        curs.execute("""select 'Merchandise' as Type,
                    count(*) as count from sale_merchandise_assignment
                    join sale using (sale_id)
                    WHERE UTC_DATE::DATE = CURRENT_DATE - INTERVAL '1 DAY'
                    union all
                    select 'Album', count(*) from sale_album_assignment
                    join sale using (sale_id)
                    WHERE UTC_DATE::DATE = CURRENT_DATE - INTERVAL '1 DAY'
                    union all
                    select 'Track', count(*) from sale_track_assignment
                    join sale using (sale_id)
                    WHERE UTC_DATE::DATE = CURRENT_DATE - INTERVAL '1 DAY'
                    order by Type
                    ;""")
        sales_res = curs.fetchall()
        return sales_res


def get_total_revenue_made(conn: connection) -> dict:
    """Returns the total revenue made from all sales."""
    with conn.cursor() as curs:
        curs.execute("""select
                    (select sum(sold_for) from sale_merchandise_assignment
                    JOIN sale using (sale_id)
                    WHERE UTC_DATE::DATE = CURRENT_DATE - INTERVAL '1 DAY') +
                    (select sum(sold_for) from sale_album_assignment
                    JOIN sale using (sale_id)
                    WHERE UTC_DATE::DATE = CURRENT_DATE - INTERVAL '1 DAY') +
                    (select sum(sold_for) from sale_track_assignment
                    JOIN sale using (sale_id)
                    WHERE UTC_DATE::DATE = CURRENT_DATE - INTERVAL '1 DAY') as total_revenue;""")
        revenue_results = curs.fetchall()
        return revenue_results


def get_total_revenue_made_categorised(conn):
    """Returns the total revenue made from all sales categorised by item type."""
    with conn.cursor() as curs:
        curs.execute("""select 'Merchandise' as Type,
                    sum(sold_for)::float as total_revenue
                    from sale_merchandise_assignment
                    join sale using (sale_id)
                    WHERE UTC_DATE::DATE = CURRENT_DATE - INTERVAL '1 DAY'
                    union all
                    select 'Album', sum(sold_for) from sale_album_assignment
                    join sale using (sale_id)
                    WHERE UTC_DATE::DATE = CURRENT_DATE - INTERVAL '1 DAY'
                    union all
                    select 'Track', sum(sold_for) from sale_track_assignment
                    join sale using (sale_id)
                    WHERE UTC_DATE::DATE = CURRENT_DATE - INTERVAL '1 DAY'
                    order by Type
                    ;""")
        revenue_res = curs.fetchall()
        return revenue_res
