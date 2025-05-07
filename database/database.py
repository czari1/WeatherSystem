from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, MetaData, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from config.config import DATABASE_URL

engine = create_engine(DATABASE_URL)
Base = declarative_base()

class WeatherDataTable(Base):
    __tablename__ = 'weather_data'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    city_name = Column(String(50),nullable=False )
    country = Column(String, nullable=False)
    temperature = Column(Float, nullable=False)
    feels_like = Column(Float, nullable=False)
    humidity = Column(Integer, nullable=False)
    pressure = Column(Integer, nullable=False)
    wind_speed = Column(Float, nullable=False)
    wind_direction = Column(Integer, nullable=False)
    weather_condition = Column(String(50), nullable=False)
    weather_description = Column(String(200), nullable=False)
    clouds = Column(Integer, nullable=False)
    rain_1h = Column(Float,nullable=False)
    snow_1h = Column(Float,nullable=False)
    timestamp = Column(DateTime, nullable=False, index=True)

def init_db():
    Base.metadata.create_all(engine)

def get_session():
    Session = sessionmaker(bind=engine)

    return Session()
def close_connection():
    engine.dispose()
