"""Artists page."""
from os import environ as ENV
import pandas as pd
from dotenv import load_dotenv
import streamlit as st
from Home import get_connection, load_sale_data


def load_album_data(_conn):
    load_dotenv()

    query = """
    SELECT a.*, ar.artist_name FROM album a
    LEFT JOIN artist_album_assignment aaa USING(album_id)
    LEFT JOIN artist ar USING(artist_id);
    """
    df = pd.read_sql(query, _conn)
    return df


if __name__ == "__main__":
    LOGO = "../documentation/tracktion_logo.png"
    st.logo(LOGO, size="large")

    conn = get_connection(
        ENV['DB_HOST'],
        ENV['DB_NAME'],
        ENV['DB_USER'],
        ENV['DB_PASSWORD'],
        ENV['DB_PORT']
    )

    album_df = load_album_data(conn)
    sale_df = load_sale_data(conn)

    st.markdown(
        "<h1 style='text-align: center;'>Album Art Gallery</h1>", unsafe_allow_html=True)

    sale_df = sale_df.loc[:, ~sale_df.columns.duplicated()]

    album_popularity = sale_df.groupby(
        'album_id').size().reset_index(name='num_sales')
    album_df = album_df.merge(album_popularity, on='album_id', how='left')
    album_df['num_sales'] = album_df['num_sales'].fillna(0).astype(int)

    album_df = album_df.sort_values(by='num_sales', ascending=False).head(250)

    album_df = album_df.dropna(
        subset=['art_url', 'url', 'album_name', 'artist_name'])
    album_df = album_df.sort_values(
        'num_sales', ascending=False).reset_index(drop=True)

    num_cols = 4
    cols = st.columns(num_cols)

    for idx, row in album_df.iterrows():
        col = cols[idx % num_cols]
        count = row['num_sales']
        caption = (
            f"{row['artist_name']} – {row['album_name']} "
            f"<span style='color: #FF8C00 ;'>({count} sale{'s' if count != 1 else ''})</span>"
        )

        with col:
            st.markdown(
                f"""
                <div>
                    <a href="{row['url']}" target="_blank">
                        <img src="{row['art_url']}" style="width:100%;" alt="{caption}" />
                    </a>
                    <div style="text-align:center; font-style:italic;">{caption}</div>
                </div>
                """,
                unsafe_allow_html=True
            )
    conn.close()
