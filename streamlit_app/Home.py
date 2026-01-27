import streamlit as st
import psycopg2
import pandas as pd
import plotly.graph_objects as go
import os
from datetime import datetime

from styles import apply_custom_styling, get_plotly_layout_config, get_color_palette

st.set_page_config(
    page_title="NYC Weather",
    page_icon="🌤️",
    layout="wide",
    initial_sidebar_state="expanded"  
)

apply_custom_styling()

@st.cache_resource
def get_connection():
    """Create database connection with error handling"""
    try:
        conn = psycopg2.connect(
            host=os.getenv('WEATHER_DB_HOST', 'postgres'),
            port=os.getenv('WEATHER_DB_PORT', '5432'),
            database=os.getenv('WEATHER_DB_NAME', 'weather_db'),
            user=os.getenv('WEATHER_DB_USER', 'weather_user'),
            password=os.getenv('WEATHER_DB_PASSWORD', 'weather_password'),
            connect_timeout=5
        )
        return conn
    except Exception as e:
        st.error(f"Cannot connect to weather database: {str(e)}")
        return None

@st.cache_data(ttl=300)
def load_current_weather(_conn):
    """Load current weather with proper null handling"""
    query = """
        SELECT 
            temp_fahrenheit,
            feels_like_fahrenheit,
            weather_condition,
            humidity_percent,
            wind_speed_mph,
            is_precipitating,
            is_snowing,
            neighborhood_name,
            community_board
        FROM weather_analytics.mart_current_conditions
        WHERE temp_fahrenheit IS NOT NULL
        ORDER BY neighborhood_name
    """
    try:
        df = pd.read_sql(query, _conn)
        if df.empty:
            return None
        return df
    except Exception as e:
        st.error(f"Error loading weather data: {str(e)}")
        return None

@st.cache_data(ttl=300)
def load_borough_summary(_conn):
    """Load borough averages"""
    query = """
        SELECT 
            SPLIT_PART(community_board, ' ', 1) as borough,
            ROUND(AVG(temp_fahrenheit)::numeric, 1) as avg_temp,
            ROUND(MIN(temp_fahrenheit)::numeric, 1) as min_temp,
            ROUND(MAX(temp_fahrenheit)::numeric, 1) as max_temp,
            COUNT(*) as area_count
        FROM weather_analytics.mart_current_conditions
        WHERE temp_fahrenheit IS NOT NULL
        GROUP BY SPLIT_PART(community_board, ' ', 1)
        ORDER BY avg_temp DESC
    """
    try:
        return pd.read_sql(query, _conn)
    except Exception:
        return None

def get_weather_advice(temp, feels_like, is_precip, is_snow, wind_speed):
    """Generate practical weather advice"""
    advice = []
    
    # Clothing advice
    if feels_like < 32:
        advice.append("🧥 Heavy winter coat, gloves, and hat recommended")
    elif feels_like < 50:
        advice.append("🧥 Jacket or coat needed")
    elif feels_like < 65:
        advice.append("👕 Light jacket or sweater")
    elif feels_like < 75:
        advice.append("👕 Perfect weather - comfortable clothing")
    else:
        advice.append("🩳 Light, breathable clothing")
    
    # Precipitation advice
    if is_snow:
        advice.append("⛄ Snowfall - allow extra travel time")
    elif is_precip:
        advice.append("☔ Bring an umbrella")
    
    # Wind advice
    if wind_speed > 25:
        advice.append("💨 Very windy - secure loose items")
    elif wind_speed > 15:
        advice.append("💨 Breezy conditions")
    
    return advice

def main():
    # Header
    st.markdown("# New York City Weather")
    st.markdown(f'<p class="subtitle">Real-time conditions • Updated {datetime.now().strftime("%I:%M %p")}</p>', 
                unsafe_allow_html=True)
    
    st.markdown("<hr>", unsafe_allow_html=True)
    
    # Connect to database
    conn = get_connection()
    if not conn:
        st.error("Unable to load weather data. Please try again later.")
        st.stop()
    
    # Load data
    with st.spinner("Loading current conditions..."):
        df = load_current_weather(conn)
        df_borough = load_borough_summary(conn)
    
    if df is None or df.empty:
        st.warning("No weather data available at this time.")
        st.stop()
    
    # Calculate city-wide stats
    city_temp = df['temp_fahrenheit'].mean()
    city_feels = df['feels_like_fahrenheit'].mean()
    precip_count = df['is_precipitating'].sum()
    snow_count = df['is_snowing'].sum()
    avg_wind = df['wind_speed_mph'].mean()
    
    # Main weather display
    st.markdown("## Current Conditions")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric(
            "Temperature",
            f"{city_temp:.0f}°F",
            help="Average across all NYC neighborhoods"
        )
        st.metric(
            "Feels Like",
            f"{city_feels:.0f}°F"
        )
    
    with col2:
        # Most common condition
        top_condition = df['weather_condition'].mode()[0] if len(df) > 0 else "Unknown"
        st.metric("Conditions", top_condition)
        
        if precip_count > 0:
            precip_pct = (precip_count / len(df) * 100)
            if snow_count > 0:
                st.metric("Precipitation", f"{precip_pct:.0f}% (Snow)")
            else:
                st.metric("Precipitation", f"{precip_pct:.0f}%")
        else:
            st.metric("Precipitation", "None")
    
    with col3:
        st.metric("Wind", f"{avg_wind:.0f} mph")
        
        # Temperature range
        temp_range = df['temp_fahrenheit'].max() - df['temp_fahrenheit'].min()
        st.metric("Temp Variation", f"{temp_range:.0f}°F")
    
    # Practical advice section
    st.markdown("<hr>", unsafe_allow_html=True)
    st.markdown("## What to Know")
    
    advice = get_weather_advice(
        city_temp,
        city_feels,
        precip_count > 0,
        snow_count > 0,
        avg_wind
    )
    
    for tip in advice:
        st.info(tip)
    
    # Borough comparison
    if df_borough is not None and not df_borough.empty:
        st.markdown("<hr>", unsafe_allow_html=True)
        st.markdown("## By Borough")
        
        colors = get_color_palette()
        layout_config = get_plotly_layout_config()
        
        fig = go.Figure()
        
        fig.add_trace(go.Bar(
            x=df_borough['borough'],
            y=df_borough['avg_temp'],
            marker_color=colors['primary'],
            marker_line_color='white',
            marker_line_width=2,
            text=df_borough['avg_temp'].apply(lambda x: f"{x:.0f}°F"),
            textposition='outside',
            textfont=dict(size=14, weight=600),
            hovertemplate='<b>%{x}</b><br>Temperature: %{y:.0f}°F<extra></extra>'
        ))
        
        fig.update_layout(
            **layout_config,
            title={'text': 'Average Temperature by Borough', 'font': {'size': 16}},
            xaxis_title='',
            yaxis_title='Temperature (°F)',
            showlegend=False,
            height=350
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Borough table
        st.markdown('<div class="custom-card">', unsafe_allow_html=True)
        borough_display = df_borough.copy()
        borough_display.columns = ['Borough', 'Avg Temp (°F)', 'Low (°F)', 'High (°F)', 'Areas']
        st.dataframe(borough_display, hide_index=True, use_container_width=True, height=220)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Extremes
    st.markdown("<hr>", unsafe_allow_html=True)
    st.markdown("## Temperature Extremes")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="custom-card">', unsafe_allow_html=True)
        st.markdown('<div class="custom-card-header">Warmest Areas</div>', unsafe_allow_html=True)
        warmest = df.nlargest(5, 'temp_fahrenheit')[['neighborhood_name', 'temp_fahrenheit']].copy()
        warmest.columns = ['Neighborhood', 'Temp (°F)']
        warmest['Temp (°F)'] = warmest['Temp (°F)'].round(0).astype(int)
        st.dataframe(warmest, hide_index=True, use_container_width=True, height=200)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="custom-card">', unsafe_allow_html=True)
        st.markdown('<div class="custom-card-header">Coldest Areas</div>', unsafe_allow_html=True)
        coldest = df.nsmallest(5, 'temp_fahrenheit')[['neighborhood_name', 'temp_fahrenheit']].copy()
        coldest.columns = ['Neighborhood', 'Temp (°F)']
        coldest['Temp (°F)'] = coldest['Temp (°F)'].round(0).astype(int)
        st.dataframe(coldest, hide_index=True, use_container_width=True, height=200)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Footer
    st.markdown("<hr>", unsafe_allow_html=True)
    st.markdown(
        '<p style="text-align: center; color: #94a3b8; font-size: 0.875rem;">Data updates every 5 minutes</p>',
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()