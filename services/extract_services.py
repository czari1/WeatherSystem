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
            url = f"{WEATHER_API_BASE_URL}{city['name']}"
            params = {
                "unitGroup": "metric",
                "key": WEATHER_API_KEY,
                "contentType": "json"
            }

            response = requests.get(url, params=params)
            response.raise_for_status()

            logger.info(f"Downloaded weather data for {city['name']}, {city['country']}")

            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error for downloading weather data for {city['name']}, {city['country']}")

            return None
        
    @staticmethod
    def extract_all_cities() -> List[WeatherData]:
        weather_data_list = []

        for city in CITIES:
            raw_data = ExtractService.fetch_weather_data(city)

            if raw_data:
                try:

                    current_conditions = raw_data.get('currentConditions', {})

                    weather_data = WeatherData(
                        city_name=city['name'],
                        country=city['country'],
                        temperature=current_conditions.get('temp', 0),
                        feels_like=current_conditions.get('feelslike', 0),
                        humidity=current_conditions.get('humidity', 0),
                        pressure=current_conditions.get('pressure', 0),
                        wind_speed=current_conditions.get('windspeed', 0),
                        wind_direction=current_conditions.get('winddir', 0),
                        weather_condition=current_conditions.get('conditions', '').split(',')[0].strip(),
                        weather_description=current_conditions.get('conditions', ''),
                        clouds=current_conditions.get('cloudcover', 0),
                        rain_1h=current_conditions.get('precip', 0) if current_conditions.get('precip', 0) > 0 else None,
                        snow_1h=None, 
                    )
                    weather_data_list.append(weather_data)
                except Exception as e:
                    logger.error(f"Error parsing data for {city['name']}: {str(e)}")
        
        logger.info(f"Downloaded weather data for {len(weather_data_list)} from {len(CITIES)} configured cities")
        
        return weather_data_list