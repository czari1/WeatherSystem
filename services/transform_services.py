import pandas as pd
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta

from models.weather_data import WeatherData
from repositories.weather_repositories import WeatherRepository
from utils.logger import get_logger

logger = get_logger(__name__)

class TransformService:

    @staticmethod
    def enrich_weather_data(weather_data: WeatherData) -> WeatherData:

        return weather_data
    
    @staticmethod
    def calculate_temperature_trend(city_name: str) -> Optional[Dict[str, Any]]:
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        historical_data = WeatherRepository.get_weather_data_by_data_range(city_name, start_date, end_date)

        if not historical_data or len(historical_data) < 2:
            logger.warning(f"Not enough data for {city_name} to calculate the trend")
            
            return None
        
        df = pd.DataFrame(historical_data)

        df['date'] = pd.to_datetime(df['timestamp']).dt.date
        daily_avg = df.groupby('date')['temperature'].mean().reset_index()

        x = range(len(daily_avg))
        y = daily_avg['temperature'].values

        if len(x) > 1:
            slope = pd.Series(y).diff().mean()

            if slope > 0.5:
                trend = 'rising'
            elif slope < -0.5:
                trend = 'failing'
            else:
                trend = 'stable'

            return {
                "city": city_name,
                "trend": trend,
                "slope": slope,
                "start_temp": daily_avg['temperature'].iloc[0],
                "end_temp": daily_avg['temperature'].iloc[-1],
                "period_days": len(daily_avg)
            }
        
        return None
    
    @staticmethod
    def calculate_weather_statistics(city_name: str) -> Dict[str, Any]:
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)

        historical_data = WeatherRepository.get_weather_data_by_data_range(city_name, start_date, end_date)

        if not historical_data:
            logger.warning(f"No data for {city_name}")

            return {
                "city": city_name,
                "status": "no_data"
            }
        
        df = pd.DataFrame(historical_data)

        stats = {
            "city": city_name,
            "status": "success",
            "period_start": start_date.strftime("%Y-%m-%d"),
            "period_end": end_date.strftime("%Y-%m-%d"),
            "data_points": len(df),
            "temperature": {
                "min": df['temperature'].min(),
                "max": df['temperature'].max(),
                "avg": df['temperature'].mean(),
                "std": df['temperature'].std()
            },
            "humidity": {
                "min": df['humidity'].min(),
                "max": df['humidity'].max(),
                "avg": df['humidity'].mean()
            },
            "pressure": {
                "min": df['pressure'].min(),
                "max": df['pressure'].max(),
                "avg": df['pressure'].mean()
            },
            "weather_conditions": df['weather_condition'].value_counts().to_dict()
        }

        return stats
    @staticmethod
    def batch_process_cities(weather_data_list: List[WeatherData]) -> List[WeatherData]:

        processed_data = []

        for weather_data in weather_data_list:

            try:
                enriched_data = TransformService.enrich_weather_data(weather_data)
                processed_data.append(enriched_data)
            except Exception as e:
                logger.error(f"Error while processing data for {weather_data.city_name}: {str(e)}")

        return processed_data
