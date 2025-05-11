import requests
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta, date

from config.config import WEATHER_API_BASE_URL, WEATHER_API_KEY, CITIES
from models.weather_data import WeatherData
from utils.logger import get_logger
from services.transform_services import TransformService
from services.load_services import LoadService

logger = get_logger(__name__)

class HistoricalService:

    @staticmethod
    def fetch_historical_weather(city: Dict[str, str], start_date: date, end_date: date) -> Optional[Dict[str, Any]]:

        try:
            start_str = start_date.strftime("%Y-%m-%d")
            end_str = end_date.strftime("%Y-%m-%d")

            url = f"{WEATHER_API_BASE_URL}{city['name']}/{start_str}/{end_str}"
            params = {
                "unitGroup": "metric",
                "key": WEATHER_API_KEY,
                "contentType": "json",
                "include": "days,hours"
            }

            logger.info(f"Fetching historical data for {city['name']} from {start_str} to {end_str}")
            response = requests.get(url, params=params)
            response.raise_for_status()

            logger.info(f"Successfully downloaded historical weather data for {city['name']}, {city['country']}")
            
            return response.json()
        
        except requests.exceptions.RequestException as e:
            logger.error(f"Error downloading historical data for {city['name']}, {city['country']}: {str(e)} ")
            
            return None
        
    @staticmethod
    def process_historical_data(raw_data: Dict[str, Any], city: Dict[str, str]) -> List[WeatherData]:
        weather_data_list = []

        if not raw_data or "days" not in raw_data:
            logger.error(f"Invalid historical data format for {city['name']}")
            
            return weather_data_list
        
        try:

            for day in raw_data("days", []):

                for hour in day.get("hours", []):

                    try:
                        day_date = day.get("datetime", "")
                        hour_time = hour.get("datetime", "")
                        timestamp = datetime.strptime(f"{day_date} {hour_time}","%Y-%m-%d %H:%M:%S")

                        weather_data = WeatherData(
                            city_name=city['name'],
                            country=city['country'],
                            temperature=hour.get('temp', 0),
                            feels_like=hour.get('feelslike', 0),
                            humidity=hour.get('humidity', 0),
                            pressure=hour.get('pressure', 0),
                            wind_speed=hour.get('windspeed', 0),
                            wind_direction=hour.get('winddir', 0),
                            weather_condition=hour.get('conditions', '').split(',')[0].strip(),
                            weather_description=hour.get('conditions', ''),
                            clouds=hour.get('cloudcover', 0),
                            rain_1h=hour.get('precip', 0) if hour.get('precip', 0) > 0 else None,
                            snow_1h=None,
                            timestamp=timestamp
                        )
                        weather_data_list.append(weather_data)
                    
                    except Exception as e:
                        logger.error(f"Error processing hourly data for {city['name']} at {day_date} {hour_time}: {str(e)}")
                        
                        continue
        
        except Exception as e:
            logger.error(f"Error processing historical data for {city['name']}: {str(e)}")
        
        logger.info(f"Processed {len(weather_data_list)} historical records for {city['name']}")
        
        return weather_data_list
    
    @staticmethod
    def fetch_and_save_historical_data(city_name: str, start_date: date, end_date: date) -> int:
        city = next((c for c in CITIES if c['name'].lower() == city_name.lower()), None) 
        
        if not city:
            logger.error(f"City {city_name} not found in configured cities")
            
            return 0
        
        raw_data = HistoricalService.fetch_historical_weather(city, start_date, end_date)

        if not raw_data:
            logger.error(f"No historical data retrieved for {city_name}")
            
            return 0

        weather_data_list = HistoricalService.process_historical_data(raw_data, city)

        if not weather_data_list:
            logger.error(f"No historical data processed for {city_name}")
            
            return 0
        
        processed_data = TransformService.batch_process_cities(weather_data_list)

        record_ids = LoadService.batch_save_weather_data(processed_data)
        
        logger.info(f"Saved {len(record_ids)} historical records for {city_name}")
        
        return len(record_ids)