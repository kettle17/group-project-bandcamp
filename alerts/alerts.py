"""Script that queries the database and analyses for trending artists."""
from os import environ as ENV
from datetime import date, timedelta

from dotenv import load_dotenv
import psycopg2
from psycopg2.extensions import connection
from psycopg2.extras import RealDictCursor
import pandas as pd
from pandas import DataFrame


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


def get_artist_revenue_data(conn: connection) -> DataFrame:
    """Returns a dataframe consisting of album and track sales data."""

    with conn.cursor() as curs:
        curs.execute("""SELECT
                    a.artist_id,
                    a.artist_name,
                    s.utc_date::DATE,
                    sa.sold_for,
                    al.album_name AS title,
                    'album' AS type
                    FROM 
                    sale_album_assignment AS sa
                    JOIN sale AS s ON (s.sale_id = sa.sale_id)
                    JOIN album AS al ON (al.album_id = sa.album_id)
                    JOIN artist_album_assignment AS aaa ON (al.album_id = aaa.album_id)
                    JOIN artist AS a ON (aaa.artist_id = a.artist_id)
                    UNION ALL
                    SELECT
                    a.artist_id,
                    a.artist_name,
                    s.utc_date::DATE,
                    sa.sold_for,
                    t.track_name AS title,
                    'track' AS type
                    FROM 
                    sale_track_assignment AS sa
                    JOIN sale AS s ON (s.sale_id = sa.sale_id)
                    JOIN track AS t ON (t.track_id = sa.track_id)
                    JOIN artist_track_assignment AS ata ON (t.track_id = ata.track_id)
                    JOIN artist AS a ON (ata.artist_id = a.artist_id)
                    """)
        revenue_figures = curs.fetchall()
        revenue_df = pd.DataFrame(revenue_figures)
        return revenue_df


def get_recent_revenue_dataframe(revenue_df: DataFrame, yesterday, two_days_ago) -> DataFrame:
    """Returns a dataframe that gets the sum of revenue made and only includes results where 
    revenue is more than zero and the sale date was either yesterday or two days ago."""

    revenue_df["sold_for"] = pd.to_numeric(revenue_df["sold_for"])
    recent_revenue_df = revenue_df[(revenue_df["utc_date"].isin([yesterday, two_days_ago])) &
                                   (revenue_df["sold_for"] > 0)]

    sum_recent_revenue_df = recent_revenue_df.groupby(["artist_name", "artist_id",
                                                       "type", "utc_date"])["sold_for"].sum(
    ).reset_index(
    ).sort_values(by="sold_for", ascending=False)

    return sum_recent_revenue_df


def get_sales_from_yesterday(df: DataFrame, yesterday) -> DataFrame:
    """Returns a dataframe containing total revenue made yesterday."""

    yesterday_sum_revenue_df = df[df["utc_date"] == yesterday].reset_index(
    ).rename(columns={"sold_for": "revenue_yesterday"})

    return yesterday_sum_revenue_df


def get_sales_from_two_days_ago(df: DataFrame, two_days_ago) -> DataFrame:
    """Returns a dataframe containing total revenue made two days ago."""

    two_days_ago_sum_revenue_df = df[df["utc_date"] == two_days_ago].reset_index(
    ).rename(columns={"sold_for": "revenue_two_days_ago"})

    return two_days_ago_sum_revenue_df


def get_recent_revenue_percentage_increase(yesterday_df: DataFrame,
                                           two_days_ago_df: DataFrame) -> DataFrame:
    """Combines the dataframe from yesterday and two days ago and calculates the percentage
    that the revenue increased by between the 2 days."""

    combined_recent_revenue_df = pd.merge(yesterday_df, two_days_ago_df, on=[
                                          "artist_id", "artist_name", "type"])
    combined_recent_revenue_df["percentage_increase"] = ((combined_recent_revenue_df["revenue_yesterday"] -
                                                          combined_recent_revenue_df["revenue_two_days_ago"]) /
                                                         combined_recent_revenue_df["revenue_two_days_ago"]) * 100

    return combined_recent_revenue_df


def get_trending_artists(df: DataFrame) -> DataFrame:
    """Returns trending artists which are artists that have had more than 50% increase in revenue when comparing 
    yesterday's revenue to the revenue made two days ago."""

    trending_artists = df[df["percentage_increase"] > 50].sort_values(
        by="percentage_increase", ascending=False)

    return trending_artists


def get_alerts_message(df: DataFrame) -> str:
    """Returns a string containing the alerts message for trending artists which 
    includes artists with more than 50% increase in revenue over the past 2 days."""

    message = "ALERT: TRENDING ARTISTS\n"

    for _, row in df.iterrows():
        message += f"{row['artist_name']} ({row['type']}): + {row['percentage_increase']:.1f}%\n"

    return message


def get_trending_artists_alert():
    """The main function that combines all the logic for querying the database and checking for recently trending artists
     to generate an alerts message."""

    conn = get_db_connection()
    yesterday = date.today() - timedelta(days=1)
    two_days_ago = date.today() - timedelta(days=2)

    revenue_df = get_artist_revenue_data(conn)
    recent_revenue_df = get_recent_revenue_dataframe(
        revenue_df, yesterday, two_days_ago)
    yesterday_df = get_sales_from_yesterday(recent_revenue_df, yesterday)
    two_days_ago_df = get_sales_from_two_days_ago(
        recent_revenue_df, two_days_ago)
    percentage_increase_df = get_recent_revenue_percentage_increase(
        yesterday_df, two_days_ago_df)
    trending_artists = get_trending_artists(percentage_increase_df)

    if trending_artists.empty:
        return "No trending artists over the past 2 days."
    alert_message = get_alerts_message(trending_artists)
    return alert_message


def alerts_lambda_handler(event, context):
    """AWS Lambda handler to trigger generating the alerts message for trending artists."""
    try:
        get_trending_artists_alert()
        return {
            "statusCode": 200,
            "body": "Trending artists script successful.",
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": f"Failed to retrieve trending artists.: {str(e)}",
        }


if __name__ == "__main__":

    alerts_message = get_trending_artists_alert()
    print(alerts_message)
