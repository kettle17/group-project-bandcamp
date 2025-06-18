"""Script for the load portion of the ETL pipeline."""
import io, os, sys
from os import environ as ENV
import pandas as pd, psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor, 
from psycopg2.extensions import connection, cursor
from utilities import set_logger, get_logger


STAGING_DDL = """
CREATE UNLOGGED TABLE IF NOT EXISTS stage_sale(
  utc_date      TIMESTAMP,
  country_name  TEXT,
  artist_name   TEXT,
  track_name    TEXT,
  album_name    TEXT,
  merch_name    TEXT,
  slug_type     CHAR(1),
  item_type     CHAR(1),
  url           TEXT,
  art_url       TEXT,
  release_date  DATE,
  sold_for      NUMERIC);

CREATE UNLOGGED TABLE IF NOT EXISTS stage_tag(
  url TEXT,
  tag_name TEXT);
"""

SALE_COLS = [
    "utc_date", "country_name", "artist_name",
    "track_name", "album_name", "merch_name",
    "slug_type", "item_type", "url", "art_url",
    "release_date", "sold_for",
]
TAG_LEN, ARTIST_LEN, COUNTRY_LEN, NAME_LEN = 50, 100, 60, 255


def get_db_connection(config: dict[str, str]) -> :
    """Returns a psycopg2 database connection."""
    
    return psycopg2.connect(
        host=config["DB_HOST"],
        port=config["DB_PORT"],
        user=config["DB_USER"],
        password=config["DB_PASSWORD"],
        dbname=config["DB_NAME"],
        cursor_factory=RealDictCursor,
    )


def copy_df(cur: cursor, df: pd.DataFrame, table: str, cols: list[str]) -> None:
    """Returns None, but copies DataFrame data to PostgreSQL table using COPY command."""
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, columns=cols)
    buf.seek(0)
    cur.copy_expert(f"COPY {table} ({', '.join(cols)}) FROM STDIN WITH CSV", buf)


def build_frames(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Returns tuple of (sales_dataframe, tags_dataframe) prepared for database insertion."""
    tag_df = (
        df[["url", "tag_names"]]
        .explode("tag_names")
        .dropna()
        .rename(columns={"tag_names": "tag_name"})
    )
    tag_df["tag_name"] = (
        tag_df["tag_name"].astype(str).str.strip().str.lower().str[:TAG_LEN]
    )

    sales = pd.DataFrame(index=df.index)
    sales["utc_date"] = pd.to_datetime(df["utc_date"], errors="coerce")
    sales["country_name"] = df["country_name"].astype(str).str[:COUNTRY_LEN]
    sales["artist_name"] = df["artist_name"].astype(str).str[:ARTIST_LEN]
    sales["slug_type"], sales["item_type"] = df["slug_type"], df["item_type"]
    sales["url"], sales["art_url"] = df["url"].astype(str), df["art_url"]
    sales["release_date"] = pd.to_datetime(df["release_date"], errors="coerce")
    sales["sold_for"] = df["sold_for"]

    item_description = df["item_description"].fillna("")

    sales["track_name"] = df.apply(
        lambda row: (
            item_description[row.name][:NAME_LEN]
            if (row.slug_type == "t" and row.item_type == "t")
            else None
        ),
        axis=1,
    )
    sales["album_name"] = df.apply(
        lambda row: (
            (row.album_name if row.item_type == "p" 
             else (row.get('album_title') or item_description[row.name] or 'Unknown Album'))[:NAME_LEN] 
            if (row.slug_type == "a" and row.item_type in ["a", "p"]) else None
        ),
        axis=1,
    )
    sales["merch_name"] = df.apply(
        lambda row: (item_description[row.name][:NAME_LEN] 
                    if (row.slug_type == "p" and row.item_type == "p") else None), axis=1
    )

    return sales[SALE_COLS], tag_df[["url", "tag_name"]]


def insert_dimension_data(cur: cursor, label: str, count_sql: str, insert_sql: str) -> None:
    """Returns None but executes dimension table inserts and logs insertion statistics."""
    logger = get_logger()
    cur.execute(count_sql)
    attempted = next(iter(cur.fetchone().values()))
    cur.execute(insert_sql)
    inserted = cur.rowcount
    logger.info(f"{label:12} {inserted:7}/{attempted:<7} inserted")


def run_load(df=None, csv_path=None) -> None:
    """Returns None, but loads DataFrame or CSV data into the database with full ETL process."""
    logger = get_logger()
    if df is None and csv_path is None:
        raise ValueError("Provide df or csv_path")
    if df is None:
        df = pd.read_csv(csv_path)
    if df.empty:
        logger.info("Nothing to load"); 
        return
    load_dotenv(".env")
    sales, tags = build_frames(df)

    with get_db_connection(ENV) as conn:
        with conn.cursor() as cur:
            cur.execute(STAGING_DDL)
            cur.execute("TRUNCATE stage_sale, stage_tag;")
            copy_df(cur, sales, "stage_sale", SALE_COLS)
            if not tags.empty:
                copy_df(cur, tags, "stage_tag", ["url", "tag_name"])

            insert_dimension_data(
                cur,
                "country",
                "SELECT COUNT(DISTINCT country_name) FROM stage_sale WHERE country_name IS NOT NULL",
                """INSERT INTO country(country_name)
                SELECT country_name FROM stage_sale
                WHERE country_name IS NOT NULL
                ON CONFLICT DO NOTHING
                RETURNING 1"""
            )
            insert_dimension_data(
                cur,
                "artist",
                "SELECT COUNT(DISTINCT artist_name) FROM stage_sale WHERE artist_name IS NOT NULL",
                """INSERT INTO artist(artist_name)
                SELECT artist_name FROM stage_sale
                WHERE artist_name IS NOT NULL
                ON CONFLICT DO NOTHING
                RETURNING 1"""
            )
            insert_dimension_data(
                cur,
                "tag",
                "SELECT COUNT(DISTINCT tag_name) FROM stage_tag",
                """INSERT INTO tag(tag_name)
                SELECT tag_name FROM stage_tag
                ON CONFLICT DO NOTHING
                RETURNING 1"""
            )
            insert_dimension_data(
                cur,
                "track",
                "SELECT COUNT(*) FROM stage_sale WHERE track_name IS NOT NULL",
                """INSERT INTO track(track_name, url, art_url, release_date)
                SELECT track_name, url, art_url, release_date
                FROM stage_sale
                WHERE track_name IS NOT NULL
                ON CONFLICT (url) DO NOTHING
                RETURNING 1"""
            )
            insert_dimension_data(
                cur,
                "album",
                "SELECT COUNT(*) FROM stage_sale WHERE album_name IS NOT NULL",
                """INSERT INTO album(album_name, url, art_url, release_date)
                SELECT album_name, url, art_url, release_date
                FROM stage_sale
                WHERE album_name IS NOT NULL
                ON CONFLICT (url) DO NOTHING
                RETURNING 1"""
            )
            insert_dimension_data(
                cur,
                "merchandise",
                "SELECT COUNT(*) FROM stage_sale WHERE merch_name IS NOT NULL",
                """INSERT INTO merchandise(merchandise_name, url, art_url, release_date)
                SELECT merch_name, url, art_url, release_date
                FROM stage_sale
                WHERE merch_name IS NOT NULL
                ON CONFLICT (url) DO NOTHING
                RETURNING 1"""
            )

            cur.execute("CREATE TEMP TABLE inserted_sales (sale_id BIGINT, utc_date TIMESTAMP, country_id SMALLINT) ON COMMIT DROP;")

            cur.execute("""
                WITH ins AS (
                    INSERT INTO sale(utc_date, country_id)
                    SELECT s.utc_date, c.country_id
                    FROM   stage_sale s
                    JOIN   country c USING (country_name)
                    ON CONFLICT DO NOTHING
                    RETURNING sale_id, utc_date, country_id
                )
                INSERT INTO inserted_sales
                SELECT * FROM ins;
            """)
            new_sales = cur.rowcount
            logger.info(f"sale{new_sales:7} new rows")

            cur.execute("""
                INSERT INTO sale_track_assignment(track_id, sale_id, sold_for)
                SELECT t.track_id, inserted_sales.sale_id, s.sold_for
                FROM stage_sale s
                JOIN track        t  USING (url)
                JOIN country      c  USING (country_name)
                JOIN inserted_sales
                ON inserted_sales.utc_date = s.utc_date
                AND inserted_sales.country_id = c.country_id
                WHERE s.track_name IS NOT NULL
                ON CONFLICT DO NOTHING
                RETURNING 1""")
            logger.info(f"sale_track{cur.rowcount:7} rows linked")

            cur.execute("""
                INSERT INTO sale_album_assignment(album_id, sale_id, sold_for, is_physical)
                SELECT al.album_id, inserted_sales.sale_id, s.sold_for, (s.item_type = 'p')
                FROM stage_sale s
                JOIN album        al USING (url)
                JOIN country      c  USING (country_name)
                JOIN inserted_sales
                ON inserted_sales.utc_date = s.utc_date
                AND inserted_sales.country_id = c.country_id
                WHERE s.album_name IS NOT NULL
                ON CONFLICT DO NOTHING
                RETURNING 1""")
            logger.info(f"sale_album{cur.rowcount:7} rows linked")

            cur.execute("""
                INSERT INTO sale_merchandise_assignment(merchandise_id, sale_id, sold_for)
                SELECT m.merchandise_id, inserted_sales.sale_id, s.sold_for
                FROM stage_sale s
                JOIN merchandise   m  USING (url)
                JOIN country       c  USING (country_name)
                JOIN inserted_sales
                ON inserted_sales.utc_date = s.utc_date
                AND inserted_sales.country_id = c.country_id
                WHERE s.merch_name IS NOT NULL
                ON CONFLICT DO NOTHING
                RETURNING 1""")
            logger.info(f"sale_merch{cur.rowcount:7} rows linked")

            cur.execute("""
                INSERT INTO artist_track_assignment(artist_id, track_id)
                SELECT a.artist_id, t.track_id
                FROM stage_sale s
                JOIN artist a USING (artist_name)
                JOIN track  t USING (url)
                WHERE s.track_name IS NOT NULL
                ON CONFLICT DO NOTHING
                RETURNING 1""")
            logger.info(f"artist_track{cur.rowcount:7} rows linked")

            cur.execute("""
                INSERT INTO artist_album_assignment(artist_id, album_id)
                SELECT a.artist_id, al.album_id
                FROM stage_sale s
                JOIN artist a USING (artist_name)
                JOIN album  al USING (url)
                WHERE s.album_name IS NOT NULL
                ON CONFLICT DO NOTHING
                RETURNING 1""")
            logger.info(f"artist_album{cur.rowcount:7} rows linked")

            cur.execute("""
                INSERT INTO artist_merchandise_assignment(artist_id, merchandise_id)
                SELECT a.artist_id, m.merchandise_id
                FROM stage_sale s
                JOIN artist      a USING (artist_name)
                JOIN merchandise m USING (url)
                WHERE s.merch_name IS NOT NULL
                ON CONFLICT DO NOTHING
                RETURNING 1""")
            logger.info(f"artist_merch{cur.rowcount:7} rows linked")

            cur.execute("""
                INSERT INTO track_tag_assignment(tag_id, track_id)
                SELECT tg.tag_id, tr.track_id
                FROM stage_tag st
                JOIN tag   tg USING (tag_name)
                JOIN track tr USING (url)
                ON CONFLICT DO NOTHING
                RETURNING 1""")
            logger.info(f"track_tag{cur.rowcount:7} rows linked")

            cur.execute("""
                INSERT INTO album_tag_assignment(tag_id, album_id)
                SELECT tg.tag_id, al.album_id
                FROM stage_tag st
                JOIN tag   tg USING (tag_name)
                JOIN album al USING (url)
                ON CONFLICT DO NOTHING
                RETURNING 1""")
            logger.info(f"album_tag{cur.rowcount:7} rows linked")
        conn.commit()
        logger.info(f"\nLoaded{len(df):,} rows ✔")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(1)
    run_load(csv_path=sys.argv[1])