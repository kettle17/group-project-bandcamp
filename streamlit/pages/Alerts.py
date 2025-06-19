"""Main Streamlit dashboard for live data."""
from os import environ as ENV
from dotenv import load_dotenv
import psycopg2
import streamlit as st
import streamlit_phone_number
import re
import pandas as pd


def local_css(file_name):
    """Connects to the style.css script to add a font."""
    with open(file_name) as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)


def check_email_address(email: str) -> bool:
    """Check email address to see if it matches standard regex."""
    return re.search("\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", email)


@st.dialog("Alerts")
def submit_alert_request(artist_or_genre: str, export_topic: str, frequency: str):
    choice = st.selectbox("How would you like to receive alerts?",
                          options=['Email', 'Text'])
    if choice == 'Email':
        input = st.text_input("Please enter your email")
    if choice == 'Text':
        input = streamlit_phone_number.st_phone_number(
            "Phone", placeholder="Please enter your phone number", default_country="GB")
    if st.button("Submit"):
        return [artist_or_genre, export_topic, frequency]


def generate_header() -> None:
    """Generates the header of the alerts dashboard."""
    local_css("style.css")
    LOGO = "../documentation/tracktion_logo.png"

    left_co, cent_co, last_co = st.columns(3)
    with cent_co:
        st.image(LOGO)

    st.markdown(
        "<h1 style='text-align: center;;'>Sign up to receive alerts</h1>", unsafe_allow_html=True)


if __name__ == "__main__":
    load_dotenv()

    generate_header()

    container1 = st.container()
    container1.markdown("<div class='container1'>", unsafe_allow_html=True)

    headercol1, headercol2, headercol3 = st.columns(3)
    with headercol3:
        artist_or_genre = st.selectbox("",
                                       options=['Artists', 'Genres'])
    with headercol1:
        if artist_or_genre == 'Artists':
            st.title("Artists")
        else:
            st.title("Genres")

    summarycol1, summarycol2, summarycol3 = st.columns(3)

    with summarycol2:
        frequency_summary = st.selectbox("",
                                         options=['Daily', 'Weekly'], key='summaryselectbox')
    with summarycol1:
        st.subheader("Summary data")
        st.text("Top 10 artists and their sales/popularity")
    with summarycol3:
        if st.button("Sign up to alerts", key='summarybutton'):
            submit_alert_request(artist_or_genre, 'Summary', frequency_summary)

    selectcol1, selectcol2, selectcol3 = st.columns(3)

    container1.markdown("</div>", unsafe_allow_html=True)
