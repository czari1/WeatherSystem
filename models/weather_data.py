from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional, Dict, Any


@dataclass
class WeatherData:
    city_name: str
    country: str
    temperature: float
    feels_like: float
    humidity: int
    pressure: int
    wind_speed: float
    wind_direction: int
    weather_condition: str
    weather_description: str
    clouds: int
    rain_1h: Optional[float] = None
    snow_1h: Optional[float] = None
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()
        
    def to_dict(self) -> Dict[str, Any]:
        data_dict = asdict(self)

        if isinstance(data_dict['timestamp'], datetime):
            data_dict['timestamp'] = data_dict['timestamp'].isoformat()

        return data_dict
    
    @classmethod
    def from_api_response(cls, city_data: Dict[str, Any], city_info: Dict[str, str]) -> 'WeatherData':
        main = city_data.get('main', {})
        wind = city_data.get('wind', {})
        weather = city_data.get('weather', [{}])[0]
        clouds = city_data.get('clouds]', {})
        rain = city_data.get('rain', {})
        snow = city_data.get('snow', {})

        temp_kelvin = main.get('temp', 273.15)
        feels_like_kelvin = main.get('feels_like', 273.15)
        tem_celsius = temp_kelvin - 273.15
        feels_like_celsius = feels_like_kelvin - 273.15

        return cls(
            city_name=city_info['name'],
            country=city_info['country'],
            temperature=round(tem_celsius, 2),
            feels_like=round(feels_like_celsius, 2),
            humidity=main.get('humidity', 0),
            pressure=main.get('pressure', 0),
            wind_speed=wind.get('speed', 0.0),
            wind_direction=wind.get('deg', 0),
            weather_condition=weather.get('main', ''),
            weather_description=weather.get('description', ''),
            clouds=clouds.get('all', 0),
            rian_1h=rain.get('1h', None),
            snow_1h=snow.get('1h', None),
            timestamp = datetime.fromtimestamp(city_data.get('dt', datetime.now().timestamp()))
        )
