# pylint: skip-file
"""Script for the load portion of the ETL pipeline."""

import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from psycopg2.extensions import connection
from psycopg2.extensions import cursor as pg_cursor
from psycopg2.extras import execute_values, RealDictCursor
from utilities import get_logger, set_logger


def get_db_connection() -> connection:
    load_dotenv('.env')
    return psycopg2.connect(
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        host=os.environ["DB_HOST"],
        port=os.environ["DB_PORT"],
        database=os.environ["DB_NAME"]
    )


def parse_tag_list(raw: str) -> list[str]:
    """Returns a list of tags from a string."""
    if not isinstance(raw, str):
        return []
    raw = raw.strip()
    if raw.startswith("[") and raw.endswith("]"):
        content = raw[1:-1]
        return [tag.strip(" '\"\n") for tag in content.split(",") if tag.strip()]
    return []


def extract_tags(tag_column: pd.Series) -> list[str]:
    """Returns a sorted list of unique tags from a column of tag strings."""
    tag_lists = tag_column.dropna().apply(parse_tag_list)
    flat_tags = [tag for sublist in tag_lists for tag in sublist]
    return sorted(pd.Series(flat_tags).dropna().str.strip().str.lower().unique())


def get_filtered(sales: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Returns filtered DataFrames for each content type."""
    return {
        'track': sales[(sales['slug_type'] == 't') & (sales['item_type'] == 't')].copy(),
        'album': sales[(sales['slug_type'] == 'a') & (sales['item_type'].isin(['a', 'p']))].copy(),
        'merchandise': sales[(sales['slug_type'] == 'p')].copy()
    }


def load_sales_csv() -> pd.DataFrame:
    """Returns the full sales DataFrame from the CSV file."""
    logger = get_logger()
    logger.info("Loading sales CSV data...")
    return pd.read_csv('data/clean_sales3.csv', parse_dates=['utc_date'])


def get_existing_entities(cursor, sales: pd.DataFrame, content_dfs: dict[str, pd.DataFrame]) -> dict:
    """Returns a dictionary of existing database values like countries, artists, tags, and content."""
    logger = get_logger()
    logger.info("Checking existing records...")
    existing = {}

    country_names = sales['country_name'].dropna().unique().tolist()
    cursor.execute(
        "SELECT country_id, country_name FROM country WHERE country_name = ANY(%s)",
        (country_names,)
    )
    existing['country'] = {row['country_name']: row['country_id'] for row in cursor.fetchall()}

    all_artists = pd.concat(content_dfs.values(), ignore_index=True)
    artist_names = all_artists['artist_name'].dropna().unique().tolist()
    cursor.execute(
        "SELECT artist_id, artist_name FROM artist WHERE artist_name = ANY(%s)",
        (artist_names,)
    )
    existing['artist'] = {row['artist_name']: row['artist_id'] for row in cursor.fetchall()}

    combined_tags = pd.concat([content_dfs['track']['tag_names'], content_dfs['album']['tag_names']])
    tags = extract_tags(combined_tags)
    cursor.execute(
        "SELECT tag_id, tag_name FROM tag WHERE tag_name = ANY(%s)",
        (tags,)
    )
    existing['tag'] = {row['tag_name']: row['tag_id'] for row in cursor.fetchall()}

    for key in ['track', 'album', 'merchandise']:
        urls = content_dfs[key]['url'].dropna().unique().tolist()
        cursor.execute(
            f"SELECT {key}_id, url FROM {key} WHERE url = ANY(%s)",
            (urls,)
        )
        existing[key] = {row['url']: row[f'{key}_id'] for row in cursor.fetchall()}

    return existing


def remove_existing_rows(sales: pd.DataFrame, existing: dict) -> pd.DataFrame:
    """Returns only the rows that aren't already in the database."""
    logger = get_logger()
    logger.info("Removing already loaded entries...")
    all_urls = set(existing['track']) | set(existing['album']) | set(existing['merchandise'])
    return sales[~sales['url'].isin(all_urls)]


def insert_entities(df: pd.DataFrame, entity_name: str, cursor: pg_cursor) -> dict:
    """Returns a dictionary mapping entity name to ID after inserting countries or artists."""
    logger = get_logger()
    names = df[[f'{entity_name}_name']].drop_duplicates().dropna()
    values = [(row[f'{entity_name}_name'],) for row in names.to_dict(orient='records')]

    if values:
        logger.info(f"Inserting new {entity_name}s...")
        execute_values(
            cursor,
            f"INSERT INTO {entity_name} ({entity_name}_name) VALUES %s ON CONFLICT DO NOTHING",
            values
        )
        cursor.execute(
            f"SELECT {entity_name}_id, {entity_name}_name FROM {entity_name} WHERE {entity_name}_name = ANY(%s)",
            ([val[0] for val in values],)
        )
        return {row[f'{entity_name}_name']: row[f'{entity_name}_id'] for row in cursor.fetchall()}
    return {}


def insert_tags(df: pd.DataFrame, cursor: pg_cursor) -> dict:
    """Returns a dictionary mapping tag name to ID after inserting new tags."""
    logger = get_logger()
    tags = extract_tags(df['tag_names'])
    if tags:
        logger.info("Inserting new tags...")
        execute_values(
            cursor,
            "INSERT INTO tag (tag_name) VALUES %s ON CONFLICT DO NOTHING",
            [(tag,) for tag in tags]
        )
        cursor.execute("SELECT tag_id, tag_name FROM tag WHERE tag_name = ANY(%s)", (tags,))
        return {row['tag_name']: row['tag_id'] for row in cursor.fetchall()}
    return {}


def insert_content(df: pd.DataFrame, content_type: str, cursor: pg_cursor) -> dict:
    """Returns a dictionary mapping content URL to its new or existing database ID."""
    logger = get_logger()
    records = df.to_dict(orient="records")

    if content_type == 'track':
        values = [
            (record.get('item_description') or 'Unknown Track', record['url'], record.get('art_url'),
             float(record['sold_for']) if pd.notna(record.get('sold_for')) else None, record.get('release_date'))
            for record in records
        ]
        query = "INSERT INTO track (track_name, url, art_url, sold_for, release_date) VALUES %s RETURNING track_id, url"

    elif content_type == 'album':
        values = [
            (record.get('album_title') or record.get('item_description') or 'Unknown Album', record['url'], record.get('art_url'),
             float(record['sold_for']) if pd.notna(record.get('sold_for')) else None, record.get('release_date'))
            for record in records
        ]
        query = "INSERT INTO album (album_name, url, art_url, sold_for, release_date) VALUES %s RETURNING album_id, url"

    elif content_type == 'merchandise':
        values = [
            (record.get('item_description') or 'Unknown Item', record['url'], record.get('art_url'),
             float(record['sold_for']) if pd.notna(record.get('sold_for')) else None)
            for record in records
        ]
        query = "INSERT INTO merchandise (merchandise_name, url, art_url, sold_for) VALUES %s RETURNING merchandise_id, url"

    else:
        return {}

    if values:
        logger.info(f"Inserting {content_type}s...")
        execute_values(cursor, query, values)
        return {row['url']: row[f'{content_type}_id'] for row in cursor.fetchall()}
    return {}


def insert_artist_assignments(df: pd.DataFrame, content_type: str, existing: dict, cursor: pg_cursor) -> None:
    """Returns None. Inserts links between artists and content (track, album, merch)."""
    values = [
        (existing['artist'][record['artist_name']], existing[content_type][record['url']])
        for record in df.to_dict(orient='records')
        if record['artist_name'] in existing['artist'] and record['url'] in existing[content_type]
    ]
    if values:
        execute_values(
            cursor,
            f"INSERT INTO artist_{content_type}_assignment (artist_id, {content_type}_id) VALUES %s",
            values
        )


def insert_tag_assignments(df: pd.DataFrame, content_type: str, existing: dict, tag_map: dict, cursor: pg_cursor) -> None:
    """Returns None. Inserts links between tags and content (track or album)."""
    values = []
    for record in df.to_dict(orient='records'):
        tags = parse_tag_list(record.get('tag_names', ''))
        for tag in tags:
            tag = tag.strip().lower()
            if tag in tag_map and record['url'] in existing[content_type]:
                values.append((tag_map[tag], existing[content_type][record['url']]))
    if values:
        execute_values(
            cursor,
            f"INSERT INTO {content_type}_tag_assignment (tag_id, {content_type}_id) VALUES %s ON CONFLICT DO NOTHING",
            values
        )


def insert_sales_and_assignments(sales: pd.DataFrame, existing: dict, cursor: pg_cursor) -> None:
    """Returns None. Inserts sales records and connects them to content (track, album, or merch)."""
    for row in sales.to_dict(orient='records'):
        country_id = existing['country'].get(row['country_name'])
        if not country_id:
            continue

        cursor.execute(
            "INSERT INTO sale (utc_date, country_id) VALUES (%s, %s) RETURNING sale_id",
            (row['utc_date'], country_id)
        )
        sale_id = cursor.fetchone()['sale_id']
        url = row['url']

        if row['slug_type'] == 't' and url in existing['track']:
            cursor.execute("INSERT INTO sale_track_assignment (track_id, sale_id) VALUES (%s, %s)",
                           (existing['track'][url], sale_id))
        elif row['slug_type'] == 'a' and url in existing['album']:
            cursor.execute("INSERT INTO sale_album_assignment (album_id, sale_id) VALUES (%s, %s)",
                           (existing['album'][url], sale_id))
        elif row['slug_type'] == 'p' and url in existing['merchandise']:
            cursor.execute("INSERT INTO sale_merchandise_assignment (merchandise_id, sale_id) VALUES (%s, %s)",
                           (existing['merchandise'][url], sale_id))


def upload_to_db(dataframe: pd.DataFrame, conn: connection) -> None:
    """Returns None. Uploads a pandas dataframe to a database."""
    logger = get_logger()

    if not isinstance(dataframe, pd.DataFrame):
        raise TypeError("Input must be a pandas DataFrame.")
    
    if not hasattr(conn, "cursor") or not callable(conn.cursor):
        raise TypeError("Invalid database connection.")

    if dataframe.empty:
        logger.info("Empty dataframe received, nothing to upload.")
        return

    cursor = conn.cursor()

    try:
        content_dfs = get_filtered(dataframe)
        existing = get_existing_entities(cursor, dataframe, content_dfs)
        filtered_sales = remove_existing_rows(dataframe, existing)

        country_map = insert_entities(filtered_sales, 'country', cursor)
        artist_map = insert_entities(pd.concat(content_dfs.values(), ignore_index=True), 'artist', cursor)
        tag_map = insert_tags(pd.concat([content_dfs['track'], content_dfs['album']], ignore_index=True), cursor)

        track_map = insert_content(content_dfs['track'], 'track', cursor)
        album_map = insert_content(content_dfs['album'], 'album', cursor)
        merch_map = insert_content(content_dfs['merchandise'], 'merchandise', cursor)

        existing.update({
            'country': country_map,
            'artist': artist_map,
            'tag': tag_map,
            'track': track_map,
            'album': album_map,
            'merchandise': merch_map
        })
        insert_artist_assignments(content_dfs['track'], 'track', existing, cursor)
        insert_artist_assignments(content_dfs['album'], 'album', existing, cursor)
        insert_artist_assignments(content_dfs['merchandise'], 'merchandise', existing, cursor)
        insert_tag_assignments(content_dfs['track'], 'track', existing, tag_map, cursor)
        insert_tag_assignments(content_dfs['album'], 'album', existing, tag_map, cursor)
        insert_sales_and_assignments(filtered_sales, existing, cursor)

        conn.commit()
        logger.info("Data uploaded successfully.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error during upload: {e}")
        raise
    finally:
        cursor.close()


def run_load(dataframe: pd.DataFrame = None, csv_path: str = None) -> None:
    set_logger()
    logger = get_logger()

    if dataframe is None and csv_path is None:
        raise ValueError("Either dataframe or csv_path must be provided.")

    if csv_path is not None:
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found: {csv_path}")
        dataframe = pd.read_csv(csv_path, parse_dates=['utc_date'])
        logger.info(f"CSV loaded from {csv_path}")

    if dataframe.empty:
        logger.info("Empty dataframe received, skipping upload.")
        return

    conn = get_db_connection()
    upload_to_db(dataframe, conn)
    conn.close()


if __name__ == "__main__":
    set_logger()
    run_load(csv_path="data/clean_sales3.csv")
