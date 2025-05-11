import streamlit as st
import pandas as pd
import altair as alt
import requests
from datetime import datetime, timedelta
import time
from io import StringIO
import json

from config.config import WEATHER_API_KEY, WEATHER_API_BASE_URL, CITIES
from utils.logger import get_logger

logger = get_logger(__name__)

st.set_page_config(
    page_title="Historical Weather Data",
    layout="wide"
)

st.title("Historical Weather Dashboard")
st.markdown("""
This dashboard retrieves historical weather data directly from the Visual Crossing API.
Select a city, date range, and view historical weather patterns.
""")

st.sidebar.header("Data Selection")

city_options = [city["name"] for city in CITIES]
default_city_index = 0 

selected_city = st.sidebar.selectbox(
    "Select City", 
    city_options,
    index=default_city_index
)

today = datetime.now().date()
default_end_date = today - timedelta(days=1)  
default_start_date = default_end_date - timedelta(days=14)  

start_date = st.sidebar.date_input(
    "Start Date",
    value=default_start_date,
    max_value=default_end_date
)

end_date = st.sidebar.date_input(
    "End Date",
    value=default_end_date,
    min_value=start_date,
    max_value=today
)

date_diff = (end_date - start_date).days
if date_diff > 30:
    st.sidebar.warning(f"You selected {date_diff} days. Long date ranges may take longer to load.")

@st.cache_data(ttl=3600)
def fetch_historical_weather_data(city_name, start_date, end_date):
    """Fetch historical weather data directly from the Visual Crossing API"""
    
    try:
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        
        url = f"{WEATHER_API_BASE_URL}{city_name}/{start_str}/{end_str}"
        params = {
            "unitGroup": "metric",
            "key": WEATHER_API_KEY,
            "contentType": "json",
            "include": "days,hours"
        }
        
        with st.spinner(f"Fetching data for {city_name} from {start_str} to {end_str}..."):
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            return data
    
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching weather data: {str(e)}")
        logger.error(f"API Error: {str(e)}")
        
        return None
    
    except Exception as e:
        st.error(f"An unexpected error occurred: {str(e)}")
        logger.error(f"Unexpected error: {str(e)}")
        
        return None

def process_weather_data(raw_data):
    """Process the raw API data into a structured DataFrame"""
    if not raw_data or "days" not in raw_data:
        return None
    
    hourly_data = []
    daily_data = []
    
    for day in raw_data.get("days", []):
        day_date = day.get("datetime")

        daily_data.append({
            "date": day_date,
            "tempmax": day.get("tempmax"),
            "tempmin": day.get("tempmin"),
            "temp": day.get("temp"),
            "humidity": day.get("humidity"),
            "pressure": day.get("pressure"),
            "windspeed": day.get("windspeed"),
            "conditions": day.get("conditions"),
            "description": day.get("description"),
            "precip": day.get("precip", 0),
            "cloudcover": day.get("cloudcover", 0)
        })

        for hour in day.get("hours", []):
            hour_time = hour.get("datetime")
            
            timestamp = f"{day_date} {hour_time}"
            
            hourly_data.append({
                "timestamp": timestamp,
                "datetime": hour_time,
                "date": day_date,
                "temperature": hour.get("temp"),
                "feels_like": hour.get("feelslike"),
                "humidity": hour.get("humidity"),
                "pressure": hour.get("pressure"),
                "wind_speed": hour.get("windspeed"),
                "wind_direction": hour.get("winddir"),
                "weather_condition": hour.get("conditions", "").split(",")[0].strip(),
                "weather_description": hour.get("conditions", ""),
                "clouds": hour.get("cloudcover", 0),
                "precipitation": hour.get("precip", 0)
            })
    
    hourly_df = pd.DataFrame(hourly_data)
    daily_df = pd.DataFrame(daily_data)
    
    if not hourly_df.empty:
        hourly_df["timestamp"] = pd.to_datetime(hourly_df["timestamp"])
        hourly_df["datetime"] = pd.to_datetime(hourly_df["datetime"], format="%H:%M:%S").dt.time
        hourly_df["date"] = pd.to_datetime(hourly_df["date"])
    
    if not daily_df.empty:
        daily_df["date"] = pd.to_datetime(daily_df["date"])
    
    return {
        "hourly": hourly_df,
        "daily": daily_df,
        "metadata": {
            "city": raw_data.get("address"),
            "latitude": raw_data.get("latitude"),
            "longitude": raw_data.get("longitude"),
            "timezone": raw_data.get("timezone"),
            "start_date": start_date,
            "end_date": end_date
        }
    }

if st.sidebar.button("Fetch Data"):
    with st.spinner("Fetching weather data..."):
        raw_data = fetch_historical_weather_data(selected_city, start_date, end_date)
        
        if raw_data:
            processed_data = process_weather_data(raw_data)
            
            if processed_data:
                st.session_state.weather_data = processed_data
                st.success(f"Successfully loaded data for {selected_city} from {start_date} to {end_date}")
            
            else:
                st.error("Failed to process the weather data")
        
        else:
            st.error("Failed to fetch weather data")

if 'weather_data' in st.session_state:
    data = st.session_state.weather_data
    hourly_df = data["hourly"]
    daily_df = data["daily"]
    metadata = data["metadata"]
    
    st.header(f"Weather data for {metadata['city']}")
    st.subheader(f"Period: {metadata['start_date']} to {metadata['end_date']}")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Latitude", f"{metadata['latitude']:.4f}")
    with col2:
        st.metric("Longitude", f"{metadata['longitude']:.4f}")
    with col3:
        st.metric("Timezone", metadata['timezone'])
    
    st.header("Daily Temperature Trends")
    temp_chart = alt.Chart(daily_df).mark_line(point=True).encode(
        x=alt.X('date:T', title='Date'),
        y=alt.Y('temp:Q', title='Average Temperature (°C)', scale=alt.Scale(zero=False)),
        tooltip=['date:T', 'temp:Q', 'tempmin:Q', 'tempmax:Q', 'conditions:N']
    ).properties(
        height=300
    )
    
    temp_range = alt.Chart(daily_df).mark_area(opacity=0.2).encode(
        x=alt.X('date:T'),
        y=alt.Y('tempmin:Q', title='Min Temperature'),
        y2=alt.Y2('tempmax:Q', title='Max Temperature'),
        tooltip=['date:T', 'tempmin:Q', 'tempmax:Q']
    )
    
    st.altair_chart(temp_chart + temp_range, use_container_width=True)
    
    st.header("Temperature Distribution by Hour")
    
    hourly_df['hour'] = hourly_df['timestamp'].dt.hour
    
    hourly_stats = hourly_df.groupby('hour')['temperature'].agg(['mean', 'min', 'max']).reset_index()

    hourly_temp_chart = alt.Chart(hourly_stats).mark_line().encode(
        x=alt.X('hour:O', title='Hour of Day', axis=alt.Axis(labelAngle=0)),
        y=alt.Y('mean:Q', title='Average Temperature (°C)', scale=alt.Scale(zero=False)),
        tooltip=['hour:O', 'mean:Q', 'min:Q', 'max:Q']
    ).properties(
        height=300,
        title='Average Temperature by Hour of Day'
    )

    hourly_range = alt.Chart(hourly_stats).mark_area(opacity=0.2).encode(
        x=alt.X('hour:O'),
        y=alt.Y('min:Q'),
        y2=alt.Y2('max:Q'),
        tooltip=['hour:O', 'min:Q', 'max:Q']
    )
    
    st.altair_chart(hourly_temp_chart + hourly_range, use_container_width=True)

    st.header("Weather Conditions")
    
    col1, col2 = st.columns(2)
    
    with col1:
        condition_counts = daily_df['conditions'].value_counts().reset_index()
        condition_counts.columns = ['condition', 'count']
        
        conditions_chart = alt.Chart(condition_counts).mark_bar().encode(
            x=alt.X('count:Q', title='Days'),
            y=alt.Y('condition:N', title='Weather Condition', sort='-x'),
            color=alt.Color('condition:N', legend=None),
            tooltip=['condition:N', 'count:Q']
        ).properties(
            height=200,
            title='Weather Conditions Distribution'
        )
        st.altair_chart(conditions_chart, use_container_width=True)
    
    with col2:
        cloud_chart = alt.Chart(daily_df).mark_bar().encode(
            x=alt.X('date:T', title='Date'),
            y=alt.Y('cloudcover:Q', title='Cloud Cover (%)'),
            color=alt.Color('cloudcover:Q', scale=alt.Scale(scheme='blues')),
            tooltip=['date:T', 'cloudcover:Q', 'conditions:N']
        ).properties(
            height=200,
            title='Daily Cloud Cover'
        )
        st.altair_chart(cloud_chart, use_container_width=True)

    st.header("Humidity and Precipitation")
    
    col1, col2 = st.columns(2)
    
    with col1:
        humidity_chart = alt.Chart(daily_df).mark_area(opacity=0.7).encode(
            x=alt.X('date:T', title='Date'),
            y=alt.Y('humidity:Q', title='Humidity (%)', scale=alt.Scale(domain=[0, 100])),
            tooltip=['date:T', 'humidity:Q']
        ).properties(
            height=250,
            title='Daily Average Humidity'
        )
        st.altair_chart(humidity_chart, use_container_width=True)
    
    with col2:
        precip_chart = alt.Chart(daily_df).mark_bar().encode(
            x=alt.X('date:T', title='Date'),
            y=alt.Y('precip:Q', title='Precipitation (mm)'),
            color=alt.Color('precip:Q', scale=alt.Scale(scheme='blues')),
            tooltip=['date:T', 'precip:Q', 'conditions:N']
        ).properties(
            height=250,
            title='Daily Precipitation'
        )
        st.altair_chart(precip_chart, use_container_width=True)

    st.header("Hourly Weather Data")

    selected_date = st.selectbox(
        "Select date for hourly details",
        options=hourly_df['date'].dt.date.unique()
    )

    selected_day_df = hourly_df[hourly_df['date'].dt.date == selected_date]
    
    if not selected_day_df.empty:

        hourly_detail_chart = alt.Chart(selected_day_df).mark_line(point=True).encode(
            x=alt.X('datetime:T', title='Hour', axis=alt.Axis(format='%H:%M')),
            y=alt.Y('temperature:Q', title='Temperature (°C)', scale=alt.Scale(zero=False)),
            tooltip=['datetime:T', 'temperature:Q', 'feels_like:Q', 'weather_condition:N']
        ).properties(
            height=300,
            title=f'Hourly Temperature on {selected_date}'
        )
        st.altair_chart(hourly_detail_chart, use_container_width=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            humidity_detail_chart = alt.Chart(selected_day_df).mark_line(color='blue').encode(
                x=alt.X('datetime:T', title='Hour', axis=alt.Axis(format='%H:%M')),
                y=alt.Y('humidity:Q', title='Humidity (%)', scale=alt.Scale(domain=[0, 100])),
                tooltip=['datetime:T', 'humidity:Q']
            ).properties(
                height=250,
                title=f'Hourly Humidity on {selected_date}'
            )
            st.altair_chart(humidity_detail_chart, use_container_width=True)
        
        with col2:
            wind_detail_chart = alt.Chart(selected_day_df).mark_line(color='green').encode(
                x=alt.X('datetime:T', title='Hour', axis=alt.Axis(format='%H:%M')),
                y=alt.Y('wind_speed:Q', title='Wind Speed (m/s)'),
                tooltip=['datetime:T', 'wind_speed:Q', 'wind_direction:Q']
            ).properties(
                height=250,
                title=f'Hourly Wind Speed on {selected_date}'
            )
            st.altair_chart(wind_detail_chart, use_container_width=True)

    st.header("Detailed Data")
    
    view_type = st.radio(
        "Select data view",
        ["Daily Summary", "Hourly Detail"]
    )
    
    if view_type == "Daily Summary":
        st.dataframe(
            daily_df.sort_values('date', ascending=False)[
                ['date', 'temp', 'tempmin', 'tempmax', 'humidity', 'pressure', 
                 'windspeed', 'conditions', 'precip', 'cloudcover']
            ].reset_index(drop=True),
            use_container_width=True
        )
    else:
        if selected_date:
            st.dataframe(
                selected_day_df[
                    ['datetime', 'temperature', 'feels_like', 'humidity', 'pressure', 
                     'wind_speed', 'weather_condition', 'precipitation', 'clouds']
                ].reset_index(drop=True),
                use_container_width=True
            )

    st.header("Export Data")
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("Export Daily Data (CSV)"):
            csv = daily_df.to_csv(index=False)
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name=f"{selected_city}_daily_{start_date}_to_{end_date}.csv",
                mime="text/csv"
            )
    
    with col2:
        if st.button("Export Hourly Data (CSV)"):
            csv = hourly_df.to_csv(index=False)
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name=f"{selected_city}_hourly_{start_date}_to_{end_date}.csv",
                mime="text/csv"
            )
else:
    st.info("Please select a city and date range, then click 'Fetch Data' to retrieve historical weather information.")

    st.markdown("""
    ## How to use this dashboard:
    
    1. Select a city from the dropdown menu in the sidebar
    2. Choose a start date and end date for the data range
    3. Click the "Fetch Data" button to retrieve historical weather data
    4. Explore various visualizations and statistics
    5. Export the data as CSV if needed
    
    **Note**: The API has limitations on how much historical data can be retrieved at once. 
    For best performance, try requesting data for periods of 30 days or less.
    """)