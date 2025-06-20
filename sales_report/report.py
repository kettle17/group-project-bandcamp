"""This script queries the database for the previous day's sales
and generates a pdf report."""

from logging import getLogger, INFO, StreamHandler
from sys import stdout
import os
from os import environ as ENV
from datetime import date
from datetime import timedelta
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

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

MVP_EMAIL = "trainee.xac.parnell@sigmalabs.co.uk"


def get_logger():
    """Return logger with desired config."""
    return getLogger(__name__)


def set_logger():
    """Set logger configuration."""
    logger = getLogger(__name__)
    logger.setLevel(INFO)
    logger.addHandler(StreamHandler(stdout))


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

    top_artists_by_album_chart.save("/tmp/top_artists_by_album.png", "png")
    top_artists_by_track_chart.save("/tmp/top_artists_by_track.png", "png")
    top_genres_by_album_chart.save("/tmp/top_genres_by_album.png", "png")
    top_genres_by_track_chart.save("/tmp/top_genres_by_track.png", "png")


def generate_report(total_album_sales, total_track_sales, total_merch_sales,
                    total_album_revenue, total_track_revenue, total_merch_revenue,
                    top_artists_by_album_chart="/tmp/top_artists_by_album.png",
                    top_artists_by_track_chart="/tmp/top_artists_by_track.png",
                    top_genres_by_album_chart="/tmp/top_artists_by_album.png",
                    top_genres_by_track_chart="/tmp/top_artists_by_track.png"):
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

    pdf.output(f"/tmp/daily_bandcamp_report_{date.today()}.pdf")


def connect_to_s3_client(logger) -> client:
    """Returns a connection to s3 bucket."""
    logger.info("Connecting to S3...")
    return client("s3")


def upload_file_to_s3(logger, s3_client, filename, object_name=None):
    """Uploads the pdf file to an s3 bucket."""
    logger.info("Uploading to S3...")

    if object_name is None:
        object_name = os.path.basename(filename)
    s3_client.upload_file(
        filename, "c17-tracktion-daily-reports-and-images", Key=f"report/{object_name}")
    logger.info("Successfully uploaded to S3.")

def send_email_with_attachment(path: str):
    """Sends an email via AWS SES."""
    logger = get_logger()
    msg = MIMEMultipart()
    msg['Subject'] = "Tracktion Analytics - Daily Report"
    msg['From'] = MVP_EMAIL
    msg['To'] = MVP_EMAIL

    body = MIMEText("Please find the attached PDF summary pulled from BandCamp.", "plain")
    msg.attach(body)
    logger.info("Building email...")
    with open(path, 'rb') as attachment:
        part = MIMEApplication(attachment.read())
        part.add_header('Content-Disposition', 'attachment', filename=path)

    msg.attach(part)
    ses_client = client('ses', region_name='eu-west-2')
    response = ses_client.send_raw_email(
        Source=MVP_EMAIL,
        Destinations=[MVP_EMAIL],
        RawMessage={'Data': msg.as_string()}
    )
    print(response)


def generate_pdf_and_upload_to_s3():
    """Generates a pdf report and stores it in an s3 bucket."""

    set_logger()
    logger = get_logger()
    conn = get_db_connection()
    top_artists_by_album_df = get_top_artists_by_album_sales(conn)
    top_artists_by_track_df = get_top_artists_by_track_sales(conn)
    top_genres_by_album_df = get_top_genres_by_album_sales(conn)
    top_genres_by_track_df = get_top_genres_by_track_sales(conn)

    generate_charts(top_artists_by_album_df, top_artists_by_track_df,
                    top_genres_by_album_df, top_genres_by_track_df)
    logger.info("Successfully generated visualisations.")

    total_sales = get_total_sale_transactions(conn)
    total_sales_categorised = get_total_sale_transactions_categorised(conn)
    total_revenue = get_total_revenue_made(conn)
    total_revenue_categorised = get_total_revenue_made_categorised(conn)
    logger.info("Getting relevant figures.")

    total_album_sales = total_sales_categorised[0]["count"]
    total_track_sales = total_sales_categorised[1]["count"]
    total_merchandise_sales = total_sales_categorised[2]["count"]
    logger.info("Extracting statistics.")

    total_album_revenue = total_revenue_categorised[0]["total_revenue"]
    total_track_revenue = total_revenue_categorised[1]["total_revenue"]
    total_merchandise_revenue = total_revenue_categorised[2]["total_revenue"]
    logger.info("Generating report.")
    generate_report(total_album_sales, total_track_sales, total_merchandise_sales,
                    total_album_revenue, total_track_revenue, total_merchandise_revenue)
    logger.info("Report successfully generated.")
    s3_client = connect_to_s3_client(logger)
    upload_file_to_s3(
        logger, s3_client, f"/tmp/daily_bandcamp_report_{date.today()}.pdf")


def report_lambda_handler(event, context):
    """AWS Lambda handler to trigger generating report and loading to s3."""
    try:
        generate_pdf_and_upload_to_s3()
        return {
            "statusCode": 200,
            "body": "Daily report uploaded successfully.",
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": f"Failed to generate and upload daily report.: {str(e)}",
        }


if __name__ == "__main__":

    generate_pdf_and_upload_to_s3()
