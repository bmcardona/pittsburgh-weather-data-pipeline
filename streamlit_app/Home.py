import streamlit as st
import pandas as pd
import plotly.express as px
import psycopg2
import os

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
    schema = os.getenv('WEATHER_DB_SCHEMA', 'pittsburgh_analytics')
    
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
st.title("🌤️ Pittsburgh Weather Forecast")
st.markdown("7-day hourly forecast for Pittsburgh neighborhoods")

# Load data with error handling
try:
    df = load_forecast_data()
    
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
        
        # Data info
        st.caption(f"**Total Records:** {len(df):,}")
        st.caption(f"**Neighborhoods:** {len(neighborhoods)}")
        st.caption(f"**Last Updated:** {df['updated_at'].max()}")
    
    with col1:
        # Filter data for selected neighborhood
        filtered_df = df[df['neighborhood_name'] == selected_neighborhood].copy()
        
        # Create plot
        fig = px.line(
            filtered_df,
            x='forecast_time',
            y=selected_metric,
            title=f"{selected_metric_label} - {selected_neighborhood}",
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
        
        fig.update_traces(line_color='#1f77b4', line_width=2)
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Show summary stats
        st.subheader("7-Day Summary")
        col_a, col_b, col_c, col_d = st.columns(4)
        
        with col_a:
            current_val = filtered_df[selected_metric].iloc[0]
            st.metric("Current", f"{current_val:.1f}")
        with col_b:
            avg_val = filtered_df[selected_metric].mean()
            st.metric("Average", f"{avg_val:.1f}")
        with col_c:
            min_val = filtered_df[selected_metric].min()
            st.metric("Min", f"{min_val:.1f}")
        with col_d:
            max_val = filtered_df[selected_metric].max()
            st.metric("Max", f"{max_val:.1f}")
        
        # Optional: Show data table
        with st.expander("📊 View Raw Data"):
            st.dataframe(
                filtered_df[['forecast_time', selected_metric]],
                hide_index=True,
                use_container_width=True
            )

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
        st.write(f"- DB_SCHEMA: {os.getenv('WEATHER_DB_SCHEMA', 'NOT SET')}")