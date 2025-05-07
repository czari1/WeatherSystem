import requests
from typing import Dict, Any, List, Optional
import logging

from config.config import WEATHER_API_BASE_URL, WEATHER_API_KEY, CITIES
from models.weather_data import WeatherData
from utils.logger import get_logger

logger = get_logger(__name__)

class ExtractService:
    
    @staticmethod
    def fetch_weather_data(city: Dict[str, str]) -> Optional[Dict[str, Any]]:

        try:
            url = f"{WEATHER_API_BASE_URL}/weather"
            params = {
                "q": f"{city['name']}, {city['country']}",
                "appid": WEATHER_API_KEY
            }

            response = requests.get(url, params=params)
            response.raise_for_status()

            logger.info(f"Downloaded weather data for {city['name']}, {city['country']}")

            return response.json
        except requests.exceptions.RequestException as e:
            logger.error(f"Error for downloading weather data for {city['name']}, {city['country']}")

            return None
        
    @staticmethod
    def extract_all_cities() -> List[WeatherData]:
        weather_data_list = []

        for city in CITIES:
            raw_data = ExtractService.fetch_weather_data(city)

            if raw_data:
                weather_data = WeatherData.from_api_response(raw_data, city)
                weather_data_list.append(weather_data)
        
        logger.info(f"Dowloaded weather data for {len(weather_data_list)} from {len(CITIES)} configurated cities")
        
        return weather_data_list
