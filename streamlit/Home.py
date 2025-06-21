"""Main Streamlit dashboard for live data: Artist-overview dashboard(v2)"""
from datetime import timezone
from os import environ as ENV
import os
import psycopg2
import plotly.express as px
import country_converter
from dotenv import load_dotenv
from botocore.config import Config
import boto3
import pandas as pd
import streamlit as st
S3_ARTIST_IMG_PATH = "https://c17-tracktion-daily-reports.s3.eu-west-2.amazonaws.com/artist_images/"

st.set_page_config(
    page_title="Tracktion",
    page_icon="🎶",
    layout="wide"
)


def get_connection(host, dbname, user, password, port):
    """Create and cache a SQL Server connection using pyodbc."""
    connection = psycopg2.connect(
        host=host,
        database=dbname,
        user=user,
        password=password,
        port=port
    )
    return connection


def local_css(file_name):
    """Connects to the style.css script to add a font."""
    base_dir = os.path.dirname(__file__)  # directory of current script
    css_path = os.path.join(base_dir, file_name)
    with open(css_path) as f:
        st.markdown(f"<style>{f.read()}</style>",
                    unsafe_allow_html=True)


@st.cache_data
def run_query(_conn, sql, params=None):
    """Runs the queries for average summaries."""
    return pd.read_sql(sql, _conn, params=params)


@st.cache_data
def load_sale_data(_conn):
    """Loads the required data by querying the database."""
    query = """SELECT s.*, c.*, a.*, ar.*, saa.* FROM sale s
    LEFT JOIN country c USING(country_id)
    LEFT JOIN sale_album_assignment saa USING(sale_id)
    LEFT JOIN album a USING(album_id)
    LEFT JOIN artist_album_assignment aaa USING(album_id)
    LEFT JOIN artist ar USING(artist_id);"""
    df = pd.read_sql(query, _conn)
    return df


@st.cache_data
def _load_combined_sale_data(_conn):
    """Loads the combined sale data."""
    sql = """
      SELECT s.*, c.*,

             -- album sales
             saa.album_id, saa.sold_for            AS album_sold_for,

             -- track sales
             sta.track_id, sta.sold_for            AS track_sold_for,

             -- merch sales
             sma.merchandise_id, sma.sold_for      AS merch_sold_for,

             a.*, ar.*
      FROM sale s
      LEFT JOIN country                  c   USING(country_id)

      LEFT JOIN sale_album_assignment    saa USING(sale_id)
      LEFT JOIN sale_track_assignment    sta USING(sale_id)
      LEFT JOIN sale_merchandise_assignment sma USING(sale_id)

      LEFT JOIN album                    a   USING(album_id)
      LEFT JOIN artist_album_assignment  aaa USING(album_id)
      LEFT JOIN artist                   ar  USING(artist_id)
      ORDER BY album_sold_for DESC;
    """
    df = pd.read_sql(sql, _conn)
    df["utc_date"] = pd.to_datetime(df["utc_date"], utc=True)
    return df


def show_artist_image(artist_name: str):
    """Shows the artist image on dashboard."""
    slug = "_".join(artist_name.lower().split()) + ".jpg"
    bucket = "c17-tracktion-daily-reports-and-images"
    st.markdown(
        "<div style='text-align: center;'><h2>Artist Portrait</h2></div>", unsafe_allow_html=True)
    cfg = Config(region_name="eu-west-2", s3={"addressing_style": "virtual"})
    s3 = boto3.client(
        "s3",
        config=cfg,
        aws_access_key_id=ENV["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=ENV["AWS_SECRET_ACCESS_KEY"],
    )

    try:
        url = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": f"artist_images/{slug}"},
            ExpiresIn=3600,
        )
        print(url)
        st.image(url, use_container_width=True, caption=artist_name)
    except s3.exceptions.NoSuchKey:
        st.image("https://placehold.co/160x160?text=No+image", width=160)


@st.cache_data(ttl=900)
def _top_media(_c, artist, s, e, n=5):
    """Returns the top media of each artist."""
    sql = """
    WITH win AS (SELECT sale_id FROM sale
                 WHERE utc_date BETWEEN %(s)s AND %(e)s),
         id  AS (SELECT artist_id FROM artist WHERE artist_name=%(a)s),

    tr AS (
      SELECT tr.track_name AS title, tr.art_url, 'Track' AS kind,
             MIN(tr.release_date) AS released, COUNT(*) AS buys,
             SUM(sta.sold_for) AS revenue
      FROM track tr
      JOIN artist_track_assignment ata USING(track_id)
      JOIN sale_track_assignment  sta USING(track_id)
      WHERE ata.artist_id = (SELECT artist_id FROM id)
        AND sale_id IN (SELECT sale_id FROM win)
      GROUP BY tr.track_id
    ),
    al AS (
      SELECT al.album_name AS title, al.art_url, 'Album' AS kind,
             MIN(al.release_date) AS released, COUNT(*) AS buys,
             SUM(saa.sold_for) AS revenue
      FROM album al
      JOIN artist_album_assignment aaa USING(album_id)
      JOIN sale_album_assignment  saa USING(album_id)
      WHERE aaa.artist_id = (SELECT artist_id FROM id)
        AND sale_id IN (SELECT sale_id FROM win)
      GROUP BY al.album_id
    ),
    me AS (
    SELECT me.merchandise_name AS title, me.art_url, 'Merch' AS kind,
            NULL::DATE AS released, COUNT(*) AS buys,
            SUM(sma.sold_for) AS revenue
    FROM merchandise me
    JOIN artist_merchandise_assignment ama USING(merchandise_id)
    JOIN sale_merchandise_assignment  sma USING(merchandise_id)
    WHERE ama.artist_id = (SELECT artist_id FROM id)
        AND sale_id IN (SELECT sale_id FROM win)
    GROUP BY me.merchandise_id
    )

    SELECT * FROM (
       SELECT * FROM tr
       UNION ALL
       SELECT * FROM al
       UNION ALL
       SELECT * FROM me
    ) x
    ORDER BY revenue DESC
    LIMIT %(n)s;
    """
    return run_query(_c, sql, {"a": artist, "s": s, "e": e, "n": n})


def avg(series):
    """Calculates the average of a series of numbers."""
    v = series[series > 0].mean()
    return 0 if pd.isna(v) else v


def headline_box(filtered_df: pd.DataFrame, col):
    """
    Display key metrics for a given artist.
    """
    if filtered_df.empty:
        st.info("No sales for this selection.")
        return

    downloads = (filtered_df["track_sold_for"] == 0).sum()
    avg_track = avg(filtered_df["track_sold_for"])
    avg_album = avg(filtered_df["album_sold_for"])
    avg_merch = avg(filtered_df["merch_sold_for"])
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Free Track downloads", f"{downloads:,}")
    with col2:
        st.metric("Avg Track Sale",  f"£{avg_track:,.2f}")
    with col3:
        st.metric("Avg Album Sale",  f"£{avg_album:,.2f}")
    with col4:
        st.metric("Avg Merch Sale",  f"£{avg_merch:,.2f}")


@st.cache_data(ttl=600)
def _time_series(_c, artist, s, e, bucket):
    """Runs the following queries."""
    sql = f"""
    WITH win AS (
        SELECT sale_id, date_trunc('{bucket}', utc_date) AS p
        FROM sale
        WHERE utc_date BETWEEN %(s)s AND %(e)s
    ), id AS (
        SELECT artist_id FROM artist WHERE artist_name=%(a)s
    )
    SELECT p,
           'Track' AS cat, SUM(sta.sold_for) AS rev
      FROM win JOIN sale_track_assignment sta USING(sale_id)
               JOIN artist_track_assignment ata USING(track_id)
      WHERE ata.artist_id=(SELECT artist_id FROM id)
      GROUP BY p
    UNION ALL
    SELECT p,
           'Album', SUM(saa.sold_for)
      FROM win JOIN sale_album_assignment saa USING(sale_id)
               JOIN artist_album_assignment aaa USING(album_id)
      WHERE aaa.artist_id=(SELECT artist_id FROM id)
      GROUP BY p
    UNION ALL
    SELECT p,
           'Merch', SUM(sma.sold_for)
      FROM win JOIN sale_merchandise_assignment sma USING(sale_id)
               JOIN artist_merchandise_assignment ama USING(merchandise_id)
      WHERE ama.artist_id=(SELECT artist_id FROM id)
      GROUP BY p
    ORDER BY p;
    """
    return run_query(_c, sql, {"a": artist, "s": s, "e": e})


def show_time_series_fixed(filtered_df: pd.DataFrame, choice: str):
    """
    Draw a revenue time-series for the selected artist and date range using pandas resampling.
    """
    if filtered_df.empty:
        st.info("No sales in this period")
        return

    freq_map = {"Day": "H", "Week": "D", "Month": "D",
                "Year": "W", "Custom": "W"}
    freq = freq_map.get(choice, "W")
    bucket_lbl = {"H": "hour", "D": "day", "W": "week"}[freq]

    rev_data = filtered_df[["utc_date", "track_sold_for",
                            "album_sold_for", "merch_sold_for"]].copy()

    rev_data = rev_data.fillna(0)

    rev = (
        rev_data.set_index("utc_date")
        .rename(columns={
            "track_sold_for": "Track",
            "album_sold_for": "Album",
            "merch_sold_for": "Merch"
        })
        .resample(freq).sum()
        .reset_index()
        .rename(columns={"utc_date": "bucket"})
    )

    if rev[["Track", "Album", "Merch"]].sum().sum() == 0:
        st.info("No revenue in this period")
        return

    fig = px.line(
        rev, x="bucket", y=["Track", "Album", "Merch"], markers=True,
        title=f"Revenue by product every {bucket_lbl.capitalize()}",
        labels={"bucket": "Time", "value": "£", "variable": "Product"}
    )
    fig.update_layout(template="plotly_dark", height=300,
                      legend_orientation="h", margin=dict(t=40, r=10))
    st.plotly_chart(fig, use_container_width=True)


def filter_bar(sale_df: pd.DataFrame, default_artist: str = "Four Tet"):
    """Sidebar widget: artist selector + date-range filter."""
    st.markdown(
        "<div style='text-align: center;'><h2>Filters</h2></div>", unsafe_allow_html=True)

    another_spacer, spacer_left, col1, col2, col3, spacer_right = st.columns(
        [1, 4, 4, 4, 4, 1])

    with col1:
        artist_options = sale_df["artist_name"].dropna().unique()
        default_index = (
            list(artist_options).index(default_artist)
            if default_artist in artist_options
            else 0
        )

        artist = st.selectbox(
            "Artist",
            options=artist_options,
            index=default_index,
            key="artist",
        )
    with col2:
        choice = st.radio(
            "Range", ["Hour", "Day", "Month", "Custom"],
            index=0,
            horizontal=True, key="range"
        )

    end_ts = pd.Timestamp.utcnow()

    if choice == "Custom":
        with col3:
            start_d, end_d = st.date_input(
                "Custom dates",
                value=(
                    (end_ts - pd.Timedelta(days=7)).date(),
                    end_ts.date()
                ),
            )

        start_ts = pd.Timestamp(start_d, tz="UTC")
        end_ts = (pd.Timestamp(end_d, tz="UTC")
                  + pd.Timedelta(days=1)
                  - pd.Timedelta(microseconds=1))
    elif choice == "Hour":
        today_midnight = pd.Timestamp.utcnow().normalize().replace(tzinfo=timezone.utc)
        start_ts = today_midnight - pd.Timedelta(days=1)
        end_ts = today_midnight - pd.Timedelta(microseconds=1)
    elif choice == "Day":
        today_midnight = pd.Timestamp.utcnow().normalize().replace(tzinfo=timezone.utc)
        start_ts = today_midnight - pd.Timedelta(days=7)
        end_ts = today_midnight - pd.Timedelta(microseconds=1)
    elif choice == "Month":
        today_midnight = pd.Timestamp.utcnow().normalize().replace(tzinfo=timezone.utc)
        start_ts = today_midnight - pd.Timedelta(days=30)
        end_ts = today_midnight - pd.Timedelta(microseconds=1)
    else:
        delta = pd.Timedelta(days=7)
        start_ts = end_ts - delta

    return artist, start_ts, end_ts, choice


@st.cache_data(ttl=900)
def _top_items(_c, artist, s, e, n=3):
    """Returns the top items sold per artist."""
    sql = """
    WITH win AS (SELECT sale_id FROM sale WHERE utc_date BETWEEN %(s)s AND %(e)s)
    SELECT tr.track_name        AS title,
           tr.art_url,
           MIN(tr.release_date) AS released,
           COUNT(*)             AS buys,
           SUM(sta.sold_for)    AS revenue
    FROM track tr
    JOIN artist_track_assignment ata USING(track_id)
    JOIN sale_track_assignment   sta USING(track_id)
    WHERE ata.artist_id = (SELECT artist_id FROM artist WHERE artist_name=%(a)s)
      AND sale_id IN (SELECT sale_id FROM win)
    GROUP BY tr.track_id
    ORDER BY revenue DESC
    LIMIT %(n)s;
    """
    return run_query(_c, sql, {"a": artist, "s": s, "e": e, "n": n})


def show_top_media(conn, artist, s, e):
    """Shows the top media items onto the dashboard."""
    data = _top_media(conn, artist, s, e, n=10)
    st.markdown(
        "<div style='text-align: center;'><h2>Top Media Sales</h2></div>", unsafe_allow_html=True)

    if data.empty:
        st.info("No sales in this period")
        return

    with st.container(height=400):
        for i, (_, row) in enumerate(data.iterrows()):
            img_col, info_col = st.columns([1, 3], gap="small")

            with img_col:
                st.image(row["art_url"] or
                         "https://placehold.co/120x120?text=No+art", width=120)

            with info_col:
                st.markdown(
                    f"**{row['title']}** ({row['kind']})  \n"
                    f"Released : {row['released'] or '—'}  \n"
                    f"Units Sold : {int(row['buys']):,}  \n"
                    f"Revenue : **£{row['revenue']:.2f}**"
                )

            if i < len(data) - 1:
                st.divider()


def show_choropleth(df: pd.DataFrame):
    """Shows the chloropleth map onto streamlit."""
    df = df.assign(
        revenue=lambda d: (
            d["track_sold_for"].fillna(0)
            + d["album_sold_for"].fillna(0)
            + d["merch_sold_for"].fillna(0)
        )
    )
    st.markdown(
        "<div style='text-align: center;'><h2>Global Purchase Map</h2></div>",
        unsafe_allow_html=True)
    geo = (df.groupby("country_name", as_index=False)
             .agg(total_revenue=("revenue", "sum")))

    geo["iso_alpha"] = country_converter.convert(
        names=geo["country_name"], to="ISO3", not_found=None)

    fig = px.choropleth(
        geo, locations="iso_alpha", color="total_revenue",
        hover_name="country_name",
        hover_data={"total_revenue": ":,.2f"},
        color_continuous_scale=px.colors.sequential.Oranges,
    )
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="#181818",
        plot_bgcolor="#181818",
        margin=dict(l=0, r=0, t=0, b=0),
        dragmode='pan',
    )

    fig.update_geos(
        bgcolor="#181818",
        landcolor="rgba(255,255,255,0.05)",
        lakecolor="#181818",
        coastlinecolor="grey",
        showcountries=True,
        countrycolor="grey",
        center=dict(lat=0, lon=0)
    )
    st.plotly_chart(fig, use_container_width=True)


def main():
    """Main function that calls all of the previous functions."""
    load_dotenv()

    with get_connection(ENV['DB_HOST'], ENV['DB_NAME'], ENV['DB_USER'],
                        ENV['DB_PASSWORD'], ENV['DB_PORT']) as conn:
        df = _load_combined_sale_data(conn)

        artist, start_date, end_date, choice = filter_bar(df)

        filtered_df = df[
            (df["artist_name"] == artist)
            & df["utc_date"].between(start_date, end_date)
        ]

        p_col, r_col = st.columns([0.9, 1.1])

        with p_col:
            show_artist_image(artist)
        with r_col:
            show_top_media(conn, artist, start_date, end_date)

        if filtered_df.empty:
            st.warning("No sales for this selection.")
            return

        freq_map = {"Hour": "hour", "Day": "day",
                    "Month": "month", "Custom": "week"}
        bucket = freq_map.get(choice, "week")

        ts_df = _time_series(conn, artist, start_date, end_date, bucket)
        show_choropleth(filtered_df)

        if ts_df.empty:
            st.info("No revenue data for this period")
        else:
            ts_df_pivot = ts_df.pivot(
                index="p", columns="cat", values="rev").fillna(0).reset_index()

            required_cols = ['Track', 'Album', 'Merch']
            for col in required_cols:
                if col not in ts_df_pivot.columns:
                    ts_df_pivot[col] = 0
            left_c, center_c, right_c = st.columns(3)
            with center_c:
                st.markdown(
                    f"<div style='text-align: center;'><h2>Revenue by {choice}</h2></div>",
                    unsafe_allow_html=True)

            fig = px.line(
                ts_df_pivot,
                x="p",
                y=required_cols,
                markers=True,
                labels={"p": "Time", "value": "£", "variable": "Product"},
                color_discrete_sequence=["#FFA500", "#FF8C00", "#FF4500"]
            )
            fig.update_layout(
                template="plotly_dark",
                height=300,
                legend_orientation="h",
                legend=dict(
                    yanchor="top",
                    y=-0.2,
                    xanchor="center",
                    x=0.5
                ),
                margin=dict(t=40, r=10)
            )
            st.plotly_chart(fig, use_container_width=True)


if __name__ == "__main__":
    local_css("style.css")

    LOGO = os.path.join(os.path.dirname(__file__),
                        "../documentation/tracktion_logo.png")

    left_co, cent_co, last_co = st.columns(3)
    with cent_co:
        st.image(LOGO)
    main()
