from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from sqlalchemy import func, desc

from database.database import WeatherDataTable, get_session
from models.weather_data import WeatherData

class WeatherRepository:

    @staticmethod
    def save_weather_data(weather_data: WeatherData) -> int:
        session = get_session()

        try:
            db_weather_data = WeatherDataTable(
                city_name = weather_data.city_name,
                country = weather_data.country,
                temperature = weather_data.temperature,
                feels_like = weather_data.feels_like,
                humidity = weather_data.humidity,
                pressure = weather_data.pressure,
                wind_speed = weather_data.wind_speed,
                wind_direction = weather_data.wind_direction,
                weather_condition = weather_data.weather_condition,
                weather_description = weather_data.weather_description,
                clouds = weather_data.clouds,
                rain_1h = weather_data.rain_1h,
                snow_1h = weather_data.snow_1h,
                timestamp = weather_data.timestamp
            )

            session.add(db_weather_data)
            session.commit()

            return db_weather_data.id
        except Exception as e:
            session.rollback()
            raise
        finally:
            session.close()

    @staticmethod
    def get_latest_weather_data_by_city(city_name: str) -> Optional[Dict[str, Any]]:
        session= get_session()

        try:
            result = session.query(WeatherDataTable).filter(
                WeatherDataTable.city_name == city_name
            ).order_by(
                desc(WeatherDataTable.timestamp)
            ).first()

            if result:
                
                return {
                    'id': result.id,
                    'city_name': result.city_name,
                    'country': result.country,
                    'temperature': result.temperature,
                    'feels_like': result.feels_like,
                    'humidity': result.humidity,
                    'pressure': result.pressure,
                    'wind_speed': result.wind_speed,
                    'wind_direction': result.wind_direction,
                    'weather_condition': result.weather_condition,
                    'weather_description': result.weather_description,
                    'clouds': result.clouds,
                    'rain_1h': result.rain_1h,
                    'snow_1h': result.snow_1h,
                    'timestamp': result.timestamp
                }
            
            return None
        finally:
            session.close()

    @staticmethod
    def get_weather_data_by_data_range(
        city_name: str,
        start_date: datetime,
        end_time: datetime
    ) -> List[Dict[str, Any]]:
        
        session = get_session()

        try:
            results = session.query(WeatherDataTable).filter(
                WeatherDataTable.city_name == city_name,
                WeatherDataTable.timestamp >= start_date,
                WeatherDataTable.timestamp <= end_time
            ).order_by(
                WeatherDataTable.timestamp
            ).all()
            
            return [{
                'id': result.id,
                'city_name': result.city_name,
                'country': result.country,
                'temperature': result.temperature,
                'feels_like': result.feels_like,
                'humidity': result.humidity,
                'pressure': result.pressure,
                'wind_speed': result.wind_speed,
                 'wind_direction': result.wind_direction,
                'weather_condition': result.weather_condition,
                'weather_description': result.weather_description,
                'clouds': result.clouds,
                'rain_1h': result.rain_1h,
                'snow_1h': result.snow_1h,
                'timestamp': result.timestamp
            } for result in results]
        finally:
            session.close()

    @staticmethod
    def get_daily_avg_temperature(city_name: str, days: 7) -> List[Dict[str, Any]]:

        session = get_session()

        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)

            results = session.query (
                func.date(WeatherDataTable.timestamp).label('date'),
                func.avg(WeatherDataTable.temperature).label('avg_temp'),
            ).filter(
                WeatherDataTable.city_name == city_name,
                WeatherDataTable.timestamp >= start_date,
                WeatherDataTable.timestamp <= end_date
            ).group_by(
                func.date(WeatherDataTable.timestamp)
            ).order_by(
                func.date(WeatherDataTable.timestamp)
            ).all()

            return [{'date': str(date), 
                    'avg_temperature': float(avg_temp)}
                    for date , avg_temp in results]
        finally:
            session.close()
            
    @staticmethod
    def get_cities_with_data() -> List[str]:
        session = get_session()

        try:
            results = session.query(WeatherDataTable.city_name).distinct().all()
            
            return [result[0] for result in results]
        finally:
            session.close()
