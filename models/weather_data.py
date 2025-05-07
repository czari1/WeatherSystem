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
