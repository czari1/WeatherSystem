import argparse
import sys
from datetime import datetime

from config.config import FETCH_INTERVAL
from controllers.etl_controllers import ETLControllers
from database.database import init_db
from utils.logger import get_logger

logger = get_logger(__name__)

def main():
    """Main function that runs the application."""
    parser = argparse.ArgumentParser(description='ETL Pipeline for weather data')
    parser.add_argument('--run-once', action='store_true', help='Run the ETL process once')
    parser.add_argument('--interval', type=int, default=FETCH_INTERVAL,
                        help=f'Interval between ETL runs in seconds (default is {FETCH_INTERVAL})')
    parser.add_argument('--export', type=str, help='Export data for the specified city to a CSV file')
    parser.add_argument('--export-path', type=str, default='./exported_data.csv',
                        help='Path to the export file (default is ./exported_data.csv)')
    parser.add_argument('--historical', type=str, help='Fetch historical data for the specified city')
    parser.add_argument('--from-date', type=str, help='Start date in YYYY-MM-DD format')
    parser.add_argument('--to-date', type=str, help='End date in YYYY-MM-DD format (default is today)')
    
    args = parser.parse_args()
    
    try:
        logger.info("Initializing the database...")
        init_db()
        logger.info("Database initialized successfully.")
    except Exception as e:
        logger.error(f"Error while initializing the database: {str(e)}")
        sys.exit(1)
    
    if args.historical:
        city_name = args.historical
        from_date = args.from_date
        to_date = args.to_date or datetime.now().strftime('%Y-%m-%d')
        
        if not from_date:
            logger.error("You must provide a start date (--from-date) in YYYY-MM-DD format")
            sys.exit(1)
            
        logger.info(f"Fetching historical data for {city_name} from {from_date} to {to_date}...")
        success = ETLControllers.fetch_historical_data(city_name, from_date, to_date)
        
        if success:
            logger.info(f"Historical data for {city_name} fetched successfully.")
            sys.exit(0)
        else:
            logger.error(f"Failed to fetch historical data for {city_name}.")
            sys.exit(1)
    
    if args.export:
        city_name = args.export
        export_path = args.export_path
        
        logger.info(f"Exporting data for {city_name} to {export_path}...")
        success = ETLControllers.export_city_data(city_name, export_path)
        
        if success:
            logger.info(f"Data exported successfully to {export_path}")
            sys.exit(0)
        else:
            logger.error("Failed to export data.")
            sys.exit(1)
    
    if args.run_once:
        logger.info("Running a single ETL process...")
        success = ETLControllers.run_etl_pipeline()
        
        if success:
            logger.info("ETL process completed successfully.")
            sys.exit(0)
        else:
            logger.error("ETL process finished with errors.")
            sys.exit(1)
    
    interval = args.interval
    logger.info(f"Running scheduled ETL process every {interval} seconds...")
    
    try:
        ETLControllers.schedule_etl_job(interval)
    except KeyboardInterrupt:
        logger.info("Application stopped by user.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error occurred: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
