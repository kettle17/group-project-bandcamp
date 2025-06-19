"""This script queries the database for the previous day's sales
and generates a pdf report."""

import os
from os import environ as ENV
from datetime import date
from datetime import timedelta

from dotenv import load_dotenv
import psycopg2
from psycopg2.extensions import connection
from psycopg2.extras import RealDictCursor
from boto3 import client

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


def generate_report(total_album_sales, total_track_sales, total_merch_sales,
                    total_album_revenue, total_track_revenue, total_merch_revenue,
                    top_artists_by_album_chart="top_artists_by_album.png",
                    top_artists_by_track_chart="top_artists_by_track.png",
                    top_genres_by_album_chart="top_artists_by_album.png",
                    top_genres_by_track_chart="top_artists_by_track.png"):
    """Generates a pdf report of the previous day's sales."""

    yesterday = date.today() - timedelta(days=1)

    pdf = PDFReport()
    pdf.add_page()
    pdf.ln(60)
    pdf.set_font("Helvetica", "B", 18)
    pdf.cell(0, 50, txt="BANDCAMP SALES PERFORMANCE REPORT", align="C", ln=True)

    pdf.cell(0, 0, txt="DAILY SUMMARY OF KEY REVENUE METRICS", align="C")
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
    pdf.insert_chart(top_artists_by_album_chart,
                     "Figure 1: Top Artists by Album Sales Revenue")

    pdf.section_title("Top 10 Artists by Track Revenue")
    pdf.paragraph(
        "The following chart ranks artists based on revenue from individual track sales. This can be indicative "
        "of strong single releases or viral trends impacting specific tracks."
    )
    pdf.insert_chart(top_artists_by_track_chart,
                     "Figure 2: Top Artists by Track Sales Revenue")

    pdf.section_title("Top Genres by Revenue")
    pdf.paragraph(
        "Genre analysis helps identify musical styles that are resonating with listeners. The charts below show "
        "the top 10 genres by album and track revenue, respectively. These insights may guide future marketing and "
        "and offers valuable insight into what genres are currently generating the most revenue."
    )
    pdf.insert_chart(top_genres_by_album_chart,
                     "Figure 3: Top Genres by Album Sales")
    pdf.insert_chart(top_genres_by_track_chart,
                     "Figure 4: Top Genres by Track Sales")

    pdf.section_title("Summary Metrics")
    pdf.paragraph(
        "Below are key summary figures capturing the total number of sale transactions and total revenue generated. "
        "Values are broken down by item category."
    )

    pdf.cell(60, 8, f"Total sales by albums: {total_album_sales}", ln=True)
    pdf.cell(60, 8, f"Total sales by tracks: {total_track_sales}", ln=True)
    pdf.cell(
        60, 8, f"Total sales by merchandise: {total_merch_sales}", ln=True)

    pdf.ln(2)

    pdf.cell(
        60, 8, f"Total revenue for albums: {total_album_revenue}", ln=True)
    pdf.cell(
        60, 8, f"Total revenue for tracks: {total_track_revenue}", ln=True)
    pdf.cell(
        60, 8, f"Total revenue for merchandise: {total_merch_revenue}", ln=True)

    pdf.output(f"daily_bandcamp_report_{date.today()}.pdf")


def generate_charts(top_artists_by_album_df, top_artists_by_track_df, top_genres_by_album_df, top_genres_by_track_df):
    """Generates the bar charts for the top 10 artists and genres."""

    top_artists_by_album_chart = get_top_artists_by_album_chart(
        top_artists_by_album_df)
    top_artists_by_track_chart = get_top_artists_by_tracks_chart(
        top_artists_by_track_df)
    top_genres_by_album_chart = get_top_genres_by_album_chart(
        top_genres_by_album_df)
    top_genres_by_track_chart = get_top_genres_by_track_chart(
        top_genres_by_track_df)

    top_artists_by_album_chart.save("top_artists_by_album.png", "png")
    top_artists_by_track_chart.save("top_artists_by_track.png", "png")
    top_genres_by_album_chart.save("top_genres_by_album.png", "png")
    top_genres_by_track_chart.save("top_genres_by_track.png", "png")


def connect_to_s3_client() -> client:
    """Returns a connection to s3 bucket."""
    return client("s3", aws_access_key_id=ENV["AWS_ACCESS_KEY_ID"],
                  aws_secret_access_key=ENV["AWS_SECRET_ACCESS_KEY"])


def upload_file_to_s3(s3_client, filename, object_name=None):
    """Uploads the pdf file to an s3 bucket."""

    object_name = filename
    if object_name is None:
        object_name = os.path.basename(filename)
    s3_client.upload_file(
        filename, "c17-tracktion-daily-reports", object_name)


def generate_pdf_and_upload_to_s3():
    """Generates a pdf report and stores it in an s3 bucket."""

    conn = get_db_connection()
    top_artists_by_album_df = get_top_artists_by_album_sales(conn)
    top_artists_by_track_df = get_top_artists_by_track_sales(conn)
    top_genres_by_album_df = get_top_genres_by_album_sales(conn)
    top_genres_by_track_df = get_top_genres_by_track_sales(conn)

    generate_charts(top_artists_by_album_df, top_artists_by_track_df,
                    top_genres_by_album_df, top_genres_by_track_df)

    total_sales = get_total_sale_transactions(conn)
    total_sales_categorised = get_total_sale_transactions_categorised(conn)
    total_revenue = get_total_revenue_made(conn)
    total_revenue_categorised = get_total_revenue_made_categorised(conn)

    total_sales_figure = total_sales[0]["total_sales"]
    total_album_sales = total_sales_categorised[0]["count"]
    total_track_sales = total_sales_categorised[1]["count"]
    total_merchandise_sales = total_sales_categorised[2]["count"]

    total_revenue_figure = float(total_revenue[0]["total_revenue"])
    total_album_revenue = total_revenue_categorised[0]["total_revenue"]
    total_track_revenue = total_revenue_categorised[1]["total_revenue"]
    total_merchandise_revenue = total_revenue_categorised[2]["total_revenue"]

    generate_report(total_album_sales, total_track_sales, total_merchandise_sales,
                    total_album_revenue, total_track_revenue, total_merchandise_revenue)

    s3_client = connect_to_s3_client()
    upload_file_to_s3(
        s3_client, f"daily_bandcamp_report_{date.today()}.pdf")


if __name__ == "__main__":

    generate_pdf_and_upload_to_s3()
