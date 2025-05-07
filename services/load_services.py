from typing import List, Dict, Any
import pandas as pd
from datetime import datetime, timedelta

from models.weather_data import WeatherData
from repositories.weather_repositories import WeatherRepository
from utils.logger import get_logger

logger = get_logger(__name__)

class LoadService:
    
    @staticmethod
    def save_weather_data(weather_data: WeatherData) -> int:

        try:
            record_id = WeatherRepository.save_weather_data(weather_data)
            logger.info(f"Saved weather data for {weather_data.city_name} with ID: {record_id}")

            return record_id
        except Exception as e:
            logger.error(f"Error while saving data for {weather_data.city_name}: {str(e)}")

            raise
    @staticmethod
    def batch_save_weather_data(weather_data_list: List[WeatherData]) -> List[int]:

        record_ids = []

        for weather_data in weather_data_list:

            try:
                record_id = LoadService.save_weather_data(weather_data)
                record_ids.append(record_id)
            except Exception as e:
                logger.error(f"Failed to save data for {weather_data.city_name}: {str(e)}")

        logger.info(f"Saved {len(record_ids)} from {len(weather_data_list)} records with weather data")

        return record_ids
    
    @staticmethod
    def export_to_csv(city_name: str, file_path: str) -> bool:

        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)

            historical_data = WeatherRepository. get_weather_data_by_data_range(city_name, start_date, end_date)

            if not historical_data:
                logger.warning(f"No data ready to export for {city_name}")

                return False
            df = pd.DataFrame(historical_data)
            df.to_csv(file_path, index=False)

            logger.info(f"Data exported for {city_name} to {file_path}")

            return True
        except Exception as e:
            logger.error(f"Error while exporting data for {city_name}: {str(e)}")

            return False
