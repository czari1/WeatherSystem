import argparse
import os
import sys
import logging

from config.config import FETCH_INTERVAL
from controllers.etl_controllers import ETLControllers
from database.database import init_db
from utils.logger import get_logger

logger = get_logger(__name__)

def main():
    """Główna funkcja uruchamiająca aplikację."""
    parser = argparse.ArgumentParser(description='ETL Pipeline dla danych pogodowych')
    parser.add_argument('--run-once', action='store_true', help='Uruchom ETL jednokrotnie')
    parser.add_argument('--interval', type=int, default=FETCH_INTERVAL,
                        help=f'Interwał między uruchomieniami ETL w sekundach (domyślnie {FETCH_INTERVAL})')
    parser.add_argument('--export', type=str, help='Wyeksportuj dane dla wskazanego miasta do pliku CSV')
    parser.add_argument('--export-path', type=str, default='./exported_data.csv',
                        help='Ścieżka pliku do eksportu (domyślnie ./exported_data.csv)')
    
    args = parser.parse_args()
    
    # Inicjalizacja bazy danych
    try:
        logger.info("Inicjalizacja bazy danych...")
        init_db()
        logger.info("Baza danych została zainicjalizowana pomyślnie.")
    except Exception as e:
        logger.error(f"Błąd podczas inicjalizacji bazy danych: {str(e)}")
        sys.exit(1)
    
    # Eksport danych, jeśli wybrano tę opcję
    if args.export:
        city_name = args.export
        export_path = args.export_path
        
        logger.info(f"Eksportowanie danych dla miasta {city_name} do {export_path}...")
        success = ETLControllers.export_city_data(city_name, export_path)
        
        if success:
            logger.info(f"Dane zostały wyeksportowane pomyślnie do {export_path}")
            sys.exit(0)
        else:
            logger.error("Nie udało się wyeksportować danych.")
            sys.exit(1)
    
    # Jednorazowe uruchomienie ETL
    if args.run_once:
        logger.info("Uruchamianie pojedynczego procesu ETL...")
        success = ETLControllers.run_etl_pipeline()
        
        if success:
            logger.info("Proces ETL zakończony pomyślnie.")
            sys.exit(0)
        else:
            logger.error("Proces ETL zakończony z błędami.")
            sys.exit(1)
    
    # Cykliczne uruchamianie ETL
    interval = args.interval
    logger.info(f"Uruchamianie cyklicznego procesu ETL co {interval} sekund...")
    
    try:
        ETLControllers.schedule_etl_job(interval)
    except KeyboardInterrupt:
        logger.info("Aplikacja została zatrzymana przez użytkownika.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Wystąpił nieoczekiwany błąd: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()