"""Page for alerts data."""
import re
from os import environ as ENV
import os
from dotenv import load_dotenv
import streamlit as st
import streamlit_phone_number

import pandas as pd
from Home import get_connection


def local_css(file_name):
    """Connects to the style.css script to add a font."""
    base_dir = os.path.dirname(__file__)  # directory of current script
    css_path = os.path.join(base_dir, file_name)
    with open(css_path) as f:
        st.markdown(f"<style>{f.read()}</style>",
                    unsafe_allow_html=True)


def get_fresh_connection():
    """Reconnect with connection if closed or not usable."""
    conn = get_connection(
        ENV['DB_HOST'],
        ENV['DB_NAME'],
        ENV['DB_USER'],
        ENV['DB_PASSWORD'],
        ENV['DB_PORT']
    )
    if conn.closed != 0:
        st.cache_resource.clear()
        conn = get_connection(
            ENV['DB_HOST'],
            ENV['DB_NAME'],
            ENV['DB_USER'],
            ENV['DB_PASSWORD'],
            ENV['DB_PORT']
        )
    return conn


@st.cache_data
def load_all_tags() -> pd.DataFrame:
    """Get all tag names from the database."""
    with get_fresh_connection() as conn:
        query = """
            SELECT tag_name FROM tag;
            """
        df = pd.read_sql(query, conn)
        return df


@st.cache_data
def load_all_artists() -> pd.DataFrame:
    """Get all artist names from the database."""

    with get_fresh_connection() as conn:
        query = """
            SELECT artist_name FROM artist;
            """
        df = pd.read_sql(query, conn)
        return df


def check_email_address(email: str) -> bool:
    """Check email address to see if it matches standard regex."""
    return bool(re.fullmatch(r"^\S+@\S+\.\S+$", email))


def check_phone_number(phone: str) -> bool:
    """Check phone number to see if it is valid."""
    return bool(re.fullmatch(r"^\+?[1-9]\d{7,14}$", phone['number']))


@st.dialog("Alerts")
def submit_alert_request(artist_or_genre: str, export_topic: str, frequency: str) -> None:
    """Dialog for submitting user's email/phone number along with alert customisation."""
    choice = st.selectbox("How would you like to receive alerts?",
                          options=['Email', 'Text'])
    if choice == 'Email':
        user_input = st.text_input("Please enter your email")
    if choice == 'Text':
        user_input = streamlit_phone_number.st_phone_number(
            "Phone", placeholder="Please enter your phone number", default_country="GB")
    if st.button("Submit"):
        if choice == 'Email':
            if check_email_address(user_input):
                st.session_state.submit_alert_request = {
                    "selection": artist_or_genre, "topic": export_topic, "frequency": frequency, "format": choice, "contact": user_input}
                st.rerun()
            else:
                st.error("Email not valid", icon="🚨", width="stretch")
        else:
            if check_phone_number(user_input):
                st.session_state.submit_alert_request = {
                    "selection": artist_or_genre, "topic": export_topic, "frequency": frequency, "format": choice, "contact": user_input['number']}
                st.rerun()
            else:
                st.error("Phone number not valid", icon="🚨", width="stretch")


def return_submit_alert_request() -> dict:
    """Returns the dictionary status for the submit request.
    Use with lambda handler."""
    return st.session_state.submit_alert_request


def generate_header() -> None:
    """Generates the header of the alerts dashboard."""
    local_css("../style.css")
    logo = os.path.join(os.path.dirname(__file__),
                        "../../documentation/tracktion_logo.png")
    st.logo(logo, size="large")

    st.markdown(
        "<h1 style='text-align: center;'>Sign up to receive alerts</h1>", unsafe_allow_html=True)


if __name__ == "__main__":
    load_dotenv()
    local_css("../style.css")

    generate_header()

    all_tags = load_all_tags()
    all_artists = load_all_artists()

    st.markdown("""
    <style>
    div.stButton > button:first-child:hover {
        background-color: #FF8C00;
        color: white;
    }    
    div.stButton > button {
        display: block;
        margin: 0 auto;
    }
    </style>
                
                
    """, unsafe_allow_html=True)

    with st.container(height=475):

        headercol1, headercol2, headercol3 = st.columns(3)
        with headercol3:
            artist_or_genre = st.selectbox("Alert topic",
                                           options=['Artists', 'Genres'])
        with headercol1:
            if artist_or_genre == 'Artists':
                st.title("Artists")
            else:
                st.title("Genres")

        summarycol1, summarycol2, summarycol3 = st.columns(
            3, vertical_alignment="bottom")

        with summarycol2:
            frequency_summary = st.selectbox("Frequency",
                                             options=['Daily', 'Weekly'], key='summaryselectbox')
        with summarycol1:
            st.subheader("Summary data")
            st.text("Top 10 artists and their sales/popularity")
        with summarycol3:
            if st.button("Sign up to alerts", key='summarybutton'):
                submit_alert_request(
                    artist_or_genre, 'Summary', frequency_summary)

        if artist_or_genre == 'Artists':
            trendingcol1, trendingcol2, trendingcol3 = st.columns(
                3, vertical_alignment="bottom")

            with trendingcol2:
                trending_summary = st.selectbox("Frequency",
                                                options=['Daily', 'Weekly'], key='trendingselectbox')
            with trendingcol1:
                st.subheader("Trending data")
                st.text(
                    "Artists that have recently experienced a burst in popularity")
            with trendingcol3:
                if st.button("Sign up to alerts", key='trendingbutton'):
                    submit_alert_request(
                        artist_or_genre, 'Trending', frequency_summary)

        selectcol1, selectcol2, selectcol3 = st.columns(
            3, vertical_alignment="bottom")
        with selectcol1:
            if artist_or_genre == 'Artists':
                unique_artists = all_artists['artist_name'].unique()
                selected_artist = st.selectbox(
                    "Search artist name", placeholder="Search artist name", options=unique_artists)
            else:
                unique_tags = all_tags['tag_name'].unique()
                selected_genre = st.selectbox(
                    "Search genre name", placeholder="Search genre name", options=unique_tags)

        if artist_or_genre == 'Artists':
            with selectcol2:
                artist_report = st.selectbox("Frequency",
                                             options=['Daily', 'Weekly'], key='artistselectbox')

            with selectcol3:
                if st.button("Sign up to alerts", key='artistsummarybutton'):
                    submit_alert_request(
                        artist_or_genre, selected_artist, frequency_summary)
                if "submit_alert_request" in st.session_state:
                    return_submit_alert_request()

        else:
            with selectcol2:
                genres_report = st.selectbox("Frequency",
                                             options=['Daily', 'Weekly'], key='genreselectbox')

            with selectcol3:
                if st.button("Sign up to alerts", key='genresummarybutton'):
                    submit_alert_request(
                        artist_or_genre, selected_genre, frequency_summary)
                if "submit_alert_request" in st.session_state:
                    return_submit_alert_request()

    endcol1, endcol2, endcol3 = st.columns(3)
    with endcol1:
        if "submit_alert_request" in st.session_state:
            print(return_submit_alert_request())

            st.text("Successfully sent alert!")
    with endcol3:
        st.button("Unsubscribe from alerts")
