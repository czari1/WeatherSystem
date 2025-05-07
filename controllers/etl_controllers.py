import time
import schedule
from typing import List, Dict, Any

from services.extract_services import ExtractService
from services.load_services import LoadService
from services.transform_services import TransformService
from utils.logger import get_logger

logger = get_logger(__name__)

class ETLControllers:

    @staticmethod
    def run_etl_pipeline() -> bool:
        try:

            logger.info("Starting ETL process for weather data")
            raw_weather_data = ExtractService.extract_all_cities()

            if not raw_weather_data:
                logger.warning("No Weather data downloaded")

                return False
                
            logger.info(f"Downloaded data for {len(raw_weather_data)} cities")

            processed_data = TransformService.batch_process_cities(raw_weather_data)

            logger.info(f"Processed data for {len(processed_data)} cities")

            record_ids = LoadService.batch_save_weather_data(processed_data)

            logger.info(f"Saved {len(record_ids)} records of weather data")

            return True
        except Exception as e:
            logger.error(f"Error during ETL process: {str(e)}")

            return False
    
    @staticmethod
    def schedule_etl_job(interval_seconds: int) -> None:

        def job():
            ETLControllers.run_etl_pipeline()

        job()

        schedule.every(interval_seconds).seconds.do(job)

        logger.info(f"ETL execution for {interval_seconds} seconds")

        try:
            while True:
                schedule.run_pending()
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info(f"Stopped ETL execution")

    @staticmethod
    def get_city_statistics(city_name: str) -> Dict[str, Any]:

        return TransformService.calculate_weather_statistics(city_name)
    
    @staticmethod
    def export_city_data(city_name: str, file_path: str) -> bool:
        
        return LoadService.export_to_csv(city_name, file_path)
