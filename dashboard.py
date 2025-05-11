import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime, timedelta

from repositories.weather_repositories import WeatherRepository
from services.transform_services import TransformService
from database.database import init_db

init_db()

st.set_page_config(
    page_title= "Weather ETL",
    layout="wide"
)

st.title("Weather ETL")

cities = WeatherRepository.get_cities_with_data()

if not cities:
    st.error("No data in database. Execute ETL process to download weather data")
    st.stop()

selected_city = st.sidebar.selectbox("Choose city", cities)

time_range = st.sidebar.radio(
    "Choose time range",
    ["Last 24 hours", "Last 7 days", "Last 30 days"]
)

now = datetime.now()

if time_range == "Last 24 hours":
    start_date = now - timedelta(days=1)
elif time_range == "Last 7 days":
    start_date = now - timedelta(days=7)
else:
    start_date = now - timedelta(days=30)

weather_data = WeatherRepository.get_weather_data_by_data_range(selected_city, start_date, now)

if not weather_data:
    st.warning(f"No data for {selected_city} in chosen time range")
    st.stop()

df = pd.DataFrame(weather_data)

df['timestamp'] = pd.to_datetime(df['timestamp'])
df['date'] = df['timestamp'].dt.date
df['time'] = df["timestamp"].dt.time

stats = TransformService.calculate_weather_statistics(selected_city)

st.header(f"Current data for {selected_city}")

latest_data = df.iloc[-1] if not df.empty else None

if latest_data is not None:

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Temperature", f"{latest_data['temperature']:.1f}°C", 
                  f"{latest_data['temperature'] - stats['temperature']['avg']:.1f}°C")
    
    with col2:
        st.metric("Humidity", f"{latest_data['humidity']}%")

    with col3:
        st.metric("Pressure", f"{latest_data['pressure']} hPa")

    with col4:
        st.metric("Wind", f"{latest_data['wind_speed']:.1f} m/s")

    st.caption(f"Latest actualization: {latest_data['timestamp']}")
    st.caption(f"Description: {latest_data['weather_description']}")

st.header("Temperature graph")
temp_chart = alt.Chart(df).mark_line().encode(
    x=alt.X('timestamp:T', title='Date and hour'),
    y=alt.Y('temperature:Q', title='Temperature(°C)', scale=alt.Scale(zero=False)),
    tooltip=['timestamp:T', 'temperature:Q', 'weather_condition:N']
).properties(
    height=400
)

st.altair_chart(temp_chart, use_container_width=True)

st.header("Humidity and pressure")
col1, col2 = st.columns(2)

with col1:
    humidity_chart = alt.Chart(df).mark_area(opacity=0.7).encode(
        x=alt.X('timestamp:T', title='Date and hour'),
        y=alt.Y('humidity:Q', title='Humidity(%)', scale=alt.Scale(domain=[0,100])),
        tooltip=['timestamp:T', 'humidity:Q']
    ).properties(
        height=300,
        title='Humidity'
    )
    st.altair_chart(humidity_chart, use_container_width=True)

with col2:
    pressure_chart = alt.Chart(df).mark_line(color='red').encode(
        x=alt.X('timestamp:T', title='Date and hour'),
        y=alt.Y('pressure:Q', title='Pressure(hPa)', scale=alt.Scale(zero=False)),  
        tooltip=['timestamp:T', 'pressure:Q']
    ).properties(
        height=300,
        title='Pressure'
    )
    st.altair_chart(pressure_chart, use_container_width=True)

st.header("Weather condition")
weather_counts = df['weather_condition'].value_counts().reset_index()
weather_counts.columns = ['weather_condition', 'count']

weather_chart = alt.Chart(weather_counts).mark_bar().encode(
    x=alt.X('weather_condition:N', title='Weather condition'),
    y=alt.Y('count:Q', title='Count'),
    color=alt.Color('weather_condition:N', legend=None),
    tooltip=['weather_condition:N', 'count:Q']
).properties(
    height=300
)
st.altair_chart(weather_chart, use_container_width=True)

st.header("Detailed data")
st.dataframe(
    df.sort_values('timestamp', ascending=False)[
        ['timestamp', 'temperature', 'feels_like', 'humidity', 'pressure', 
         'wind_speed', 'weather_condition', 'weather_description']
    ].reset_index(drop=True),
    use_container_width=True
)
st.header("Statistics summary")
col1, col2, col3 = st.columns(3)

with col1:
    st.subheader("Temperature")
    st.write(f"Average: {stats['temperature']['avg']:.1f}°C")
    st.write(f"Minimal: {stats['temperature']['min']:.1f}°C")
    st.write(f"Maximum: {stats['temperature']['max']:.1f}°C")

with col2:
    st.subheader("Humidity")
    st.write(f"Average: {stats['humidity']['avg']:.1f}%")
    st.write(f"Minimal: {stats['humidity']['min']}%")
    st.write(f"Maximum: {stats['humidity']['max']}%")

with col3:
    st.subheader("Pressure")
    st.write(f"Average: {stats['pressure']['avg']:.1f} hPa")
    st.write(f"Minimal: {stats['pressure']['min']} hPa")
    st.write(f"Maximum: {stats['pressure']['max']} hPa")