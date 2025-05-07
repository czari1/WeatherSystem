import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime, timedelta

from repositories.weather_repositories import WeatherRepository
from services.transform_services import TransformService
from database.database import init_db

init_db()

st.set_page_config(
    page_title= "Weather ETL",
    layout="wide"
)

st.title("Weather ETL")

cities = WeatherRepository.get_cities_with_data()

if not cities:
    st.error("No data in database. Execute ETL process to dowload weather data")
    st.stop()

selected_city = st.sidebar.selectbox("Choose city", cities)

time_range = st.sidebar.radio(
    "Choose time range",
    ["Last 24 hours", "Last 7 days", "Last 30 days"]
)