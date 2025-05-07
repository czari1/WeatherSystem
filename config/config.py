import os
from dotenv import load_dotenv

load_dotenv()

WEATHER_API_KEY = os.getenv("WEATHER_API_KEY", "twój_klucz_api")  
WEATHER_API_BASE_URL = "https://api.openweathermap.org/data/2.5"


CITIES = [
    {"name": "Warsaw", "country": "PL"},
    {"name": "Berlin", "country": "DE"},
    {"name": "London", "country": "GB"},
    {"name": "Paris", "country": "FR"},
    {"name": "Barcelona", "country": "ES"}
]

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./weather_data.db")

FETCH_INTERVAL = int(os.getenv("FETCH_INTERVAL", 3600))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE = os.getenv("LOG_FILE", "weather_etl.log")

STREAMLIT_PORT = int(os.getenv("STREAMLIT_PORT", 8501))
