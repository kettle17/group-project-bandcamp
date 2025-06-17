"""This script queries the database for the previous day's sales
and generates a pdf report."""

from os import environ as ENV
from dotenv import load_dotenv
import psycopg2
from psycopg2.extensions import connection
from psycopg2.extras import RealDictCursor
import pandas as pd
import altair as alt


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


conn = get_db_connection()

# top 10 artists by album sales

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

# chart
chart_top_artists = alt.Chart(album_df).mark_bar().encode(
    x=alt.X('total_revenue:Q', title="Total Revenue"),
    y=alt.Y("artist_name:N", sort="-x", title="Artist Name"),
    tooltip=["artist_name", "total_revenue"],
    color=alt.Color("artist_name").scale(scheme="goldgreen").legend(None)

)

# top 10 artists by track sales

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

# chart

top_artists_by_tracks = alt.Chart(track_df).mark_bar().encode(
    x=alt.X('total_revenue:Q', title="Total Revenue"),
    y=alt.Y("artist_name:N", sort="-x", title="Artist Name"),
    tooltip=["artist_name", "total_revenue"],
    color=alt.Color("artist_name").scale(scheme="goldred").legend(None)

)

# top 10 genres by album sales

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

# charts
top_genres_by_albums = alt.Chart(album_tag_df).mark_bar().encode(
    x=alt.X('total_revenue:Q', title="Total Revenue"),
    y=alt.Y("tag_name:N", sort="-x", title="Genre"),
    tooltip=["tag_name", "total_revenue"],
    color=alt.Color("tag_name").scale(scheme="goldred").legend(None)

)

# top 10 genres by track sales

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

# chart

top_genres_by_tracks = alt.Chart(track_tag_df).mark_bar().encode(
    x=alt.X('total_revenue:Q', title="Total Revenue"),
    y=alt.Y("tag_name:N", sort="-x", title="Genre"),
    tooltip=["tag_name", "total_revenue"],
    color=alt.Color("tag_name").scale(scheme="goldred").legend(None)

)


# total sales made

with conn.cursor() as curs:
    curs.execute("""select 
                 (select count(*) from sale_merchandise_assignment) +
                 (select count(*) from sale_album_assignment) +
                 (select count(*) from sale_track_assignment) as total_sales;""")
    sale_results = curs.fetchall()
    sale_df = pd.DataFrame(sale_results)

# total sales made categorised by item

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


# total revenue made
with conn.cursor() as curs:
    curs.execute("""select 
                 (select sum(sold_for) from sale_merchandise_assignment) +
                 (select sum(sold_for) from sale_album_assignment) +
                 (select sum(sold_for) from sale_track_assignment) as total_sales;""")
    revenue_results = curs.fetchall()
    revenue_df = pd.DataFrame(revenue_results)


# total revenue made categorised by item

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

print(sale_by_item_df)
