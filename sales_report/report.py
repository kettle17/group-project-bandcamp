"""This script queries the database for the previous day's sales
and generates a pdf report."""

from os import environ as ENV
from datetime import date
from datetime import timedelta

from dotenv import load_dotenv
import psycopg2
from psycopg2.extensions import connection
from psycopg2.extras import RealDictCursor

from queries import (get_top_artists_by_album_sales, get_top_artists_by_track_sales,
                     get_top_genres_by_album_sales, get_top_genres_by_track_sales,
                     get_total_sale_transactions, get_total_sale_transactions_categorised,
                     get_total_revenue_made, get_total_revenue_made_categorised,
                     get_top_artists_by_album_chart, get_top_artists_by_tracks_chart,
                     get_top_genres_by_album_chart, get_top_genres_by_track_chart)

from pdf_class import PDFReport


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

    print(total_sales)
    total_sales_figure = total_sales[0]["total_sales"]
    total_album_sales = total_sales_categorised[0]["count"]
    total_track_sales = total_sales_categorised[1]["count"]
    total_merchandise_sales = total_sales_categorised[2]["count"]

    # total_revenue_figure = float(total_revenue[0]["total_revenue"])
    total_album_revenue = total_revenue_categorised[0]["total_revenue"]
    total_track_revenue = total_revenue_categorised[1]["total_revenue"]
    total_merchandise_revenue = total_revenue_categorised[2]["total_revenue"]

    print(total_revenue)

    yesterday = date.today() - timedelta(days=1)

    pdf = PDFReport()
    pdf.add_page()

    pdf.section_title("Overview")
    pdf.paragraph(
        "This report presents the key performance metrics for music sales recorded on BandCamp "
        f"based on the sales data from the previous day. TThis report looks at data for {yesterday.strftime('%B %d, %Y')}. It highlights the top performing artists and genres "
        "based on revenue generated from album and track sales. The insights aim to support business "
        "decisions by identifying emerging trends and top contributors to revenue."
    )

    pdf.section_title("Top 10 Artists by Album Revenue")
    pdf.paragraph(
        "The chart below displays the top 10 artists ranked by total revenue generated from album sales. "
        "It displays artist popularity in descending revenue amount."
    )
    pdf.insert_chart("top_artists_by_album.png",
                     "Figure 1: Top Artists by Album Sales Revenue")

    pdf.section_title("Top 10 Artists by Track Revenue")
    pdf.paragraph(
        "The following chart ranks artists based on revenue from individual track sales. This can be indicative "
        "of strong single releases or viral trends impacting specific tracks."
    )
    pdf.insert_chart("top_artists_by_track.png",
                     "Figure 2: Top Artists by Track Sales Revenue")

    pdf.section_title("Top Genres by Revenue")
    pdf.paragraph(
        "Genre analysis helps identify musical styles that are resonating with listeners. The charts below show "
        "the top 10 genres by album and track revenue, respectively. These insights may guide future marketing and "
        "and offers valuable insight into what genres are currently generating the most revenue."
    )
    pdf.insert_chart("top_genres_by_album.png",
                     "Figure 3: Top Genres by Album Sales")
    pdf.insert_chart("top_genres_by_track.png",
                     "Figure 4: Top Genres by Track Sales")

    pdf.section_title("Summary Metrics")
    pdf.paragraph(
        "Below are key summary figures capturing the total number of sale transactions and total revenue generated. "
        "Values are broken down by item category."
    )

    pdf.cell(60, 8, f"Total sales by albums: {total_album_sales}", ln=True)
    pdf.cell(60, 8, f"Total sales by tracks: {total_track_sales}", ln=True)
    pdf.cell(
        60, 8, f"Total sales by merchandise: {total_merchandise_sales}", ln=True)

    pdf.ln(2)

    pdf.cell(
        60, 8, f"Total revenue for albums: {total_album_revenue}", ln=True)
    pdf.cell(
        60, 8, f"Total revenue for tracks: {total_track_revenue}", ln=True)
    pdf.cell(
        60, 8, f"Total revenue for merchandise: {total_merchandise_revenue}", ln=True)

    pdf.output(f"daily_bandcamp_report_{date.today()}.pdf")
