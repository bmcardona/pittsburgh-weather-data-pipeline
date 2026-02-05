import streamlit as st
import pandas as pd
import plotly.express as px
import psycopg2
import os
from datetime import datetime, timedelta

# Page config
st.set_page_config(page_title="Pittsburgh Weather Dashboard", layout="wide")

# Database connection
@st.cache_resource
def get_db_connection():
    return psycopg2.connect(
        host=os.getenv('WEATHER_DB_HOST', 'postgres'),
        port=os.getenv('WEATHER_DB_PORT', '5432'),
        database=os.getenv('WEATHER_DB_NAME', 'weather_db'),
        user=os.getenv('WEATHER_DB_USER', 'weather_user'),
        password=os.getenv('WEATHER_DB_PASSWORD')
    )

# Load data
@st.cache_data(ttl=3600)
def load_forecast_data():
    conn = get_db_connection()
    schema = os.getenv('WEATHER_DB_ANALYTICS_SCHEMA', 'pittsburgh_analytics')
    
    query = f"""
        SELECT
            neighborhood_name,
            forecast_time,
            temp_fahrenheit_2m,
            feels_like_fahrenheit,
            rain,
            showers,
            snowfall,
            snow_depth,
            surface_pressure,
            cloud_cover,
            visibility,
            precipitation_probability,
            updated_at
        FROM {schema}.forecast
        ORDER BY neighborhood_name, forecast_time
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Main app
st.title("Pittsburgh Weather Analytics")
st.markdown("Hourly forecast for Pittsburgh neighborhoods")

# Load data with error handling
try:
    df = load_forecast_data()
    
    # Ensure forecast_time is datetime
    df['forecast_time'] = pd.to_datetime(df['forecast_time'])
    
    # Layout: two columns
    col1, col2 = st.columns([3, 1])
    
    with col2:
        st.subheader("Filters")
        
        # Neighborhood selector
        neighborhoods = sorted(df['neighborhood_name'].unique())
        selected_neighborhood = st.selectbox(
            "Select Neighborhood",
            neighborhoods,
            index=0
        )
        
        # Day range selector
        forecast_days = st.selectbox(
            "Forecast Range",
            options=[1, 2, 3, 4, 5, 6, 7],
            index=6,  # Default to 7 days
            format_func=lambda x: f"{x} Day{'s' if x > 1 else ''}"
        )
        
        # Metric selector
        metric_options = {
            'Temperature (°F)': 'temp_fahrenheit_2m',
            'Feels Like (°F)': 'feels_like_fahrenheit',
            'Precipitation Probability (%)': 'precipitation_probability',
            'Rain (mm)': 'rain',
            'Snowfall (mm)': 'snowfall',
            'Cloud Cover (%)': 'cloud_cover',
            'Visibility (m)': 'visibility',
            'Surface Pressure (hPa)': 'surface_pressure'
        }
        
        selected_metric_label = st.selectbox(
            "Select Metric",
            list(metric_options.keys()),
            index=0
        )
        selected_metric = metric_options[selected_metric_label]
        
        st.divider()
        
        if not df.empty and 'updated_at' in df.columns:
            last_updated = df['updated_at'].max()
            if pd.notna(last_updated):
                # Convert to datetime if it's not already
                last_updated_dt = pd.to_datetime(last_updated)
                
                # Format the date as "Month Day, Year"
                formatted_date = last_updated_dt.strftime("%B %d, %Y")
                
                # Format the time as "12:01 p.m."
                hour = last_updated_dt.strftime("%I").lstrip("0")  # Remove leading zero
                minute = last_updated_dt.strftime("%M")
                am_pm = last_updated_dt.strftime("%p").lower()
                formatted_time = f"{hour}:{minute} {am_pm}"
                
                # Combine with timezone
                formatted_datetime = f"{formatted_date} at {formatted_time} EST"
                
                st.caption(f"**Last Updated:** {formatted_datetime}")
            else:
                st.caption("**Last Updated:** Not available")
        else:
            st.caption("**Last Updated:** Not available")
    
    with col1:
        # Filter data for selected neighborhood
        filtered_df = df[df['neighborhood_name'] == selected_neighborhood].copy()
        
        # Filter by day range
        if len(filtered_df) > 0:
            min_time = filtered_df['forecast_time'].min()
            cutoff_time = min_time + timedelta(days=forecast_days)
            filtered_df = filtered_df[filtered_df['forecast_time'] < cutoff_time]
        
        # Create plot
        fig = px.line(
            filtered_df,
            x='forecast_time',
            y=selected_metric,
            title=f"{selected_metric_label} - {selected_neighborhood} ({forecast_days}-Day Forecast)",
            labels={
                'forecast_time': 'Time',
                selected_metric: selected_metric_label
            }
        )
        
        fig.update_layout(
            height=500,
            hovermode='x unified',
            xaxis_title="Time",
            yaxis_title=selected_metric_label,
            showlegend=False
        )
        
        fig.update_traces(line_color='gold', line_width=2)
        
        st.plotly_chart(fig, use_container_width=True)
        

except Exception as e:
    st.error(f"Error loading data: {str(e)}")
    st.info("Please check your database connection and ensure the forecast table exists.")
    
    # Debug info
    with st.expander("🔍 Debug Info"):
        st.write("Environment Variables:")
        st.write(f"- DB_HOST: {os.getenv('WEATHER_DB_HOST', 'NOT SET')}")
        st.write(f"- DB_PORT: {os.getenv('WEATHER_DB_PORT', 'NOT SET')}")
        st.write(f"- DB_NAME: {os.getenv('WEATHER_DB_NAME', 'NOT SET')}")
        st.write(f"- DB_USER: {os.getenv('WEATHER_DB_USER', 'NOT SET')}")
        st.write(f"- DB_SCHEMA: {os.getenv('WEATHER_DB__ANALYTICS_SCHEMA', 'NOT SET')}")