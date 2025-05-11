# WeatherSystem

A Python ETL and dashboard project for collecting, storing, and visualizing weather data for multiple cities using the Visual Crossing API.

---

# Weather ETL Project - Historical Dashboard

## Overview

This project contains two Streamlit dashboards for visualizing weather data:

1. **Regular Dashboard (`dashboard.py`)**: Displays weather data stored in the local database, which is collected through the ETL pipeline.
2. **Historical Dashboard (`historical_dashboard.py`)**: Fetches historical weather data directly from the Visual Crossing API without storing it in the database.

## Historical Dashboard Features

The Historical Dashboard (`historical_dashboard.py`) allows users to:

- Select any city from the configured list
- Choose a custom date range for historical data
- View daily temperature trends with min/max ranges
- Analyze temperature distribution by hour of day
- See weather condition distributions
- Track humidity and precipitation over time
- Examine detailed hourly weather data for specific days
- Export data to CSV format

## How to Run the Historical Dashboard

1. Make sure all dependencies are installed:
   ```
   pip install -r requirements.txt
   ```

2. Run the historical dashboard:
   ```
   streamlit run historical_dashboard.py
   ```

3. Access the dashboard in your web browser at `http://localhost:8501`

## Key Differences from Regular Dashboard

| Feature | Regular Dashboard | Historical Dashboard |
|---------|------------------|----------------------|
| Data Source | Local SQLite Database | Direct API Calls |
| Time Range | Limited to data in DB | Any historical date range |
| Data Persistence | Stored locally | Not persisted (fetched on demand) |
| Update Frequency | Depends on ETL schedule | Real-time queries |
| API Usage | Minimal (ETL only) | One API call per query |

## API Endpoint Used

The historical dashboard uses the Visual Crossing Timeline API endpoint:
```
https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/[location]/[date1]/[date2]
```

## Configuration

The dashboard uses the same configuration variables as the main application:
- `WEATHER_API_KEY` - Your Visual Crossing API key
- `CITIES` - List of cities to select from

## Notes

- Long date ranges may take longer to load and consume more API credits
- For best performance, keep queries to 30 days or less
- The dashboard caches API responses to minimize duplicate requests

---

## Features

- **ETL Pipeline**: Extracts weather data for configured cities, transforms, and loads it into a SQLite database.
- **Historical Data**: Fetch and store historical weather data for any configured city.
- **Dashboards**: 
  - `dashboard.py`: Visualizes recent weather data from your database.
  - `historical_dashboard.py`: Fetches and visualizes historical weather data directly from the API.
- **Export**: Export weather data to CSV.
- **Logging**: All operations are logged for traceability.

## Setup

1. **Clone the repository**

   ```sh
   git clone <repo-url>
   cd WeatherSystem
   ```

2. **Create and activate a virtual environment**

   ```sh
   python3 -m venv venv
   source venv/bin/activate
   ```

3. **Install dependencies**

   ```sh
   pip install -r requirements.txt
   ```

4. **Configure environment variables**

   Create a `.env` file in the project root:

   ```
   WEATHER_API_KEY=your_visualcrossing_api_key
   DATABASE_URL=sqlite:///./weather_data.db
   FETCH_INTERVAL=3600
   LOG_LEVEL=INFO
   LOG_FILE=weather_etl.log
   STREAMLIT_PORT=8501
   ```

   > **Note:** Never commit your `.env` file or API key to version control.

## Usage

### ETL Pipeline

- **Run ETL once:**

  ```sh
  python app.py --run-once
  ```

- **Run ETL on a schedule (default interval from `.env`):**

  ```sh
  python app.py
  ```

- **Export data for a city:**

  ```sh
  python app.py --export Warsaw --export-path warsaw_data.csv
  ```

- **Fetch historical data for a city:**

  ```sh
  python app.py --historical Warsaw --from-date 2024-01-01 --to-date 2024-01-31
  ```

### Dashboards

- **Recent data dashboard:**

  ```sh
  streamlit run dashboard.py
  ```

- **Historical data dashboard:**

  ```sh
  streamlit run historical_dashboard.py
  ```

## Project Structure

```
WeatherSystem/
├── app.py
├── dashboard.py
├── historical_dashboard.py
├── config/
│   └── config.py
├── controllers/
│   └── etl_controllers.py
├── database/
│   └── database.py
├── models/
│   └── weather_data.py
├── repositories/
│   └── weather_repositories.py
├── services/
│   ├── extract_services.py
│   ├── load_services.py
│   ├── transform_services.py
│   └── historical_services.py
├── utils/
│   └── logger.py
├── requirements.txt
├── .env
└── .gitignore
```

## License

MIT License

---

**Author:** Cezary Jaros