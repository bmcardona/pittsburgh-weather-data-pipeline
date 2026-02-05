import streamlit as st
import pandas as pd
import psycopg2
import os
import json
import folium
from folium import GeoJson, Popup
from streamlit_folium import st_folium

# Page config
st.set_page_config(page_title="Pittsburgh Weather Map", layout="wide")

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

# Load current weather data
@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_current_weather():
    try:
        conn = psycopg2.connect(
            host=os.getenv('WEATHER_DB_HOST', 'postgres'),
            port=os.getenv('WEATHER_DB_PORT', '5432'),
            database=os.getenv('WEATHER_DB_NAME', 'weather_db'),
            user=os.getenv('WEATHER_DB_USER', 'weather_user'),
            password=os.getenv('WEATHER_DB_PASSWORD')
        )
        schema = os.getenv('WEATHER_DB_SCHEMA', 'pittsburgh_analytics')
        
        query = f"""
            SELECT DISTINCT ON (neighborhood_name)
                neighborhood_name,
                temp_fahrenheit,
                feels_like_fahrenheit,
                weather_condition,
                cloud_cover_percent,
                wind_speed_mph,
                wind_direction_degrees,
                wind_gusts_mps,
                humidity_percent,
                pressure_sea_level_hpa,
                observation_time_est,
                latitude,
                longitude,
                weather_code,
                day_night,
                total_precip_mm,
                is_precipitating
            FROM {schema}.weather_readings
            ORDER BY neighborhood_name, observation_time_est DESC
        """
        
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Database error: {str(e)}")
        return pd.DataFrame()

# Load GeoJSON
@st.cache_data
def load_geojson():
    # In Docker, files are at /app
    # Try multiple possible locations
    possible_paths = [
        '/app/etl/pittsburgh_neighborhoods.geojson',  # Docker container path
        os.path.join(os.getcwd(), 'etl', 'pittsburgh_neighborhoods.geojson'),  # Relative to /app
        os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'etl', 'pittsburgh_neighborhoods.geojson'),  # Relative to script
    ]
    
    for geojson_path in possible_paths:
        geojson_path = os.path.abspath(geojson_path)
        if os.path.exists(geojson_path):
            with open(geojson_path, 'r') as f:
                return json.load(f)
    
    # If we get here, file wasn't found
    tried_paths = '\n'.join([f"{i+1}. {os.path.abspath(p)}" for i, p in enumerate(possible_paths)])
    raise FileNotFoundError(
        f"Could not find GeoJSON file. Tried:\n{tried_paths}\n\n"
        f"Please ensure the file is copied into your Docker container.\n"
        f"Add this to your Dockerfile:\nCOPY ../etl/pittsburgh_neighborhoods.geojson /app/etl/pittsburgh_neighborhoods.geojson"
    )

# Function to get wind direction name
def get_wind_direction(degrees):
    if degrees is None:
        return "N/A"
    directions = ['N', 'NNE', 'NE', 'ENE', 'E', 'ESE', 'SE', 'SSE', 
                  'S', 'SSW', 'SW', 'WSW', 'W', 'WNW', 'NW', 'NNW']
    idx = int((degrees + 11.25) / 22.5) % 16
    return directions[idx]

# Function to get temperature color with fine gradient
def get_temp_color(temp):
    if temp is None:
        return '#808080'  # Gray for no data
    
    # Purple range: < 32°F (dark purple to light purple)
    if temp < 32:
        # From dark purple #4a148c to medium purple #7e57c2
        ratio = max(0, min(1, (temp + 20) / 52))  # Normalize from -20°F to 32°F
        r = int(74 + (126 - 74) * ratio)
        g = int(20 + (87 - 20) * ratio)
        b = int(140 + (194 - 140) * ratio)
        return f'#{r:02x}{g:02x}{b:02x}'
    
    # Blue range: 32-49°F (light blue to dark blue)
    elif temp < 50:
        # From light blue #64b5f6 to dark blue #1565c0
        ratio = (temp - 32) / 17
        r = int(100 + (21 - 100) * ratio)
        g = int(181 + (101 - 181) * ratio)
        b = int(246 + (192 - 246) * ratio)
        return f'#{r:02x}{g:02x}{b:02x}'
    
    # Green range: 50-64°F (light green to dark green)
    elif temp < 65:
        # From light green #81c784 to dark green #2e7d32
        ratio = (temp - 50) / 14
        r = int(129 + (46 - 129) * ratio)
        g = int(199 + (125 - 199) * ratio)
        b = int(132 + (50 - 132) * ratio)
        return f'#{r:02x}{g:02x}{b:02x}'
    
    # Orange range: 65-79°F (light orange to dark orange)
    elif temp < 80:
        # From light orange #ffb74d to dark orange #e65100
        ratio = (temp - 65) / 14
        r = int(255 + (230 - 255) * ratio)
        g = int(183 + (81 - 183) * ratio)
        b = int(77 + (0 - 77) * ratio)
        return f'#{r:02x}{g:02x}{b:02x}'
    
    # Red range: ≥ 80°F (orange-red to deep red)
    else:
        # From orange-red #d32f2f to deep red #b71c1c
        ratio = min(1, (temp - 80) / 20)  # Cap at 100°F for gradient
        r = int(211 + (183 - 211) * ratio)
        g = int(47 + (28 - 47) * ratio)
        b = int(47 + (28 - 47) * ratio)
        return f'#{r:02x}{g:02x}{b:02x}'

# Main app
st.title("Pittsburgh Weather Map")
st.markdown("Interactive map showing current weather conditions across Pittsburgh neighborhoods")

try:
    # Load data
    weather_df = load_current_weather()
    
    # Debug: Show what we're looking for
    if len(weather_df) == 0:
        st.warning("No weather data found in database")
    
    geojson_data = load_geojson()
    
    # Create a dictionary for quick weather lookup
    weather_dict = weather_df.set_index('neighborhood_name').to_dict('index')
    
    # Neighborhood selector and current weather display
    col_selector, col_weather = st.columns([1, 3])
    
    with col_selector:
        st.subheader("Select Neighborhood")
        neighborhoods = sorted(weather_df['neighborhood_name'].unique().tolist())
        selected_neighborhood = st.selectbox(
            "Neighborhood",
            neighborhoods,
            index=0,
            label_visibility="collapsed"
        )
    
    with col_weather:
        if selected_neighborhood and selected_neighborhood in weather_dict:
            data = weather_dict[selected_neighborhood]
            
            # Get weather condition for background
            temp = data.get('temp_fahrenheit', 'N/A')
            condition = data.get('weather_condition', 'N/A')
            feels_like = data.get('feels_like_fahrenheit', 'N/A')
            weather_code = data.get('weather_code', 0)
            is_day = data.get('day_night', 'day') == 'day'
            
            # Determine background gradient based on weather condition
            # Weather codes: 0=Clear, 1-3=Cloudy, 45-48=Fog, 51-67=Rain, 71-77=Snow, 80-99=Rain/Thunderstorm
            if weather_code in [71, 73, 75, 77, 85, 86]:  # Snow
                if is_day:
                    background = "linear-gradient(to bottom, #e3f2fd 0%, #bbdefb 50%, #90caf9 100%)"
                else:
                    background = "linear-gradient(to bottom, #263238 0%, #37474f 50%, #546e7a 100%)"
                icon = "❄️"
            elif weather_code in [51, 53, 55, 61, 63, 65, 66, 67, 80, 81, 82]:  # Rain
                if is_day:
                    background = "linear-gradient(to bottom, #78909c 0%, #90a4ae 50%, #b0bec5 100%)"
                else:
                    background = "linear-gradient(to bottom, #263238 0%, #455a64 50%, #607d8b 100%)"
                icon = "🌧️"
            elif weather_code in [95, 96, 99]:  # Thunderstorm
                background = "linear-gradient(to bottom, #37474f 0%, #546e7a 50%, #78909c 100%)"
                icon = "⛈️"
            elif weather_code in [45, 48]:  # Fog
                background = "linear-gradient(to bottom, #cfd8dc 0%, #b0bec5 50%, #90a4ae 100%)"
                icon = "🌫️"
            elif weather_code in [2, 3]:  # Cloudy
                if is_day:
                    background = "linear-gradient(to bottom, #b0bec5 0%, #cfd8dc 50%, #eceff1 100%)"
                else:
                    background = "linear-gradient(to bottom, #263238 0%, #37474f 50%, #455a64 100%)"
                icon = "☁️"
            elif weather_code == 1:  # Partly cloudy
                if is_day:
                    background = "linear-gradient(to bottom, #81d4fa 0%, #b3e5fc 50%, #e1f5fe 100%)"
                else:
                    background = "linear-gradient(to bottom, #1a237e 0%, #283593 50%, #3949ab 100%)"
                icon = "⛅"
            else:  # Clear
                if is_day:
                    background = "linear-gradient(to bottom, #4fc3f7 0%, #81d4fa 50%, #b3e5fc 100%)"
                else:
                    background = "linear-gradient(to bottom, #0d47a1 0%, #1565c0 50%, #1976d2 100%)"
                icon = "☀️" if is_day else "🌙"
            
            # Create the weather card with background
            weather_card = f"""
            <div style='
                background: {background};
                padding: 30px;
                border-radius: 15px;
                box-shadow: 0 4px 6px rgba(0,0,0,0.1);
                color: {"#212121" if is_day and weather_code not in [95, 96, 99, 51, 53, 55, 61, 63, 65, 66, 67, 80, 81, 82] else "#ffffff"};
            '>
                <h2 style='margin: 0 0 10px 0; font-size: 1.8em;'>{selected_neighborhood} {icon}</h2>
                <h1 style='font-size: 5em; margin: 10px 0; font-weight: bold;'>{temp}°</h1>
                <p style='font-size: 1.5em; margin: 5px 0;'><strong>{condition}</strong></p>
                <p style='font-size: 1.2em; margin: 5px 0;'>Feels Like {feels_like}°</p>
            </div>
            """
            st.markdown(weather_card, unsafe_allow_html=True)
            
            st.write("")  # Spacing
            
            # Weather details in columns - now 6 columns to include visibility and dew point
            detail_cols = st.columns(6)
            
            with detail_cols[0]:
                wind_speed = data.get('wind_speed_mph', 'N/A')
                wind_dir_deg = data.get('wind_direction_degrees', None)
                wind_dir = get_wind_direction(wind_dir_deg)
                st.metric("Wind", f"{wind_speed} mph {wind_dir}")
                
            with detail_cols[1]:
                humidity = data.get('humidity_percent', 'N/A')
                st.metric("Humidity", f"{humidity}%")
                
            with detail_cols[2]:
                pressure = data.get('pressure_sea_level_hpa', 'N/A')
                if pressure != 'N/A':
                    # Convert hPa to inches
                    pressure_in = round(pressure * 0.02953, 2)
                    st.metric("Pressure", f"{pressure_in} in")
                else:
                    st.metric("Pressure", "N/A")
                    
            with detail_cols[3]:
                cloud_cover = data.get('cloud_cover_percent', 'N/A')
                st.metric("Cloud Cover", f"{cloud_cover}%")
            
            with detail_cols[4]:
                # Visibility - will show if column exists
                visibility = data.get('visibility', None)
                if visibility is not None:
                    # Convert meters to miles if needed
                    vis_miles = round(visibility * 0.000621371, 1) if visibility > 100 else round(visibility, 1)
                    st.metric("Visibility", f"{vis_miles} mi")
                else:
                    st.metric("Visibility", "N/A")
            
            with detail_cols[5]:
                # Dew Point - will show if column exists
                dew_point = data.get('dew_point_fahrenheit', data.get('dew_point', None))
                if dew_point is not None:
                    st.metric("Dew Point", f"{dew_point}°")
                else:
                    st.metric("Dew Point", "N/A")
    
    st.divider()
    
    # Create base map centered on Pittsburgh
    pittsburgh_center = [40.4406, -79.9959]
    m = folium.Map(
        location=pittsburgh_center,
        zoom_start=11,
        tiles='CartoDB positron',
        prefer_canvas=True,
        zoom_control=True
    )
    
    # Build all features at once for better performance
    features_to_add = []
    
    for feature in geojson_data['features']:
        hood_name = feature['properties'].get('hood', 'Unknown')
        weather_data = weather_dict.get(hood_name, {})
        temp = weather_data.get('temp_fahrenheit')
        
        # Style for this feature
        style = {
            'fillColor': get_temp_color(temp),
            'color': '#333333',
            'weight': 1,
            'fillOpacity': 0.7
        }
        
        # Create popup content
        if weather_data:
            temp_val = weather_data.get('temp_fahrenheit', 'N/A')
            feels_like = weather_data.get('feels_like_fahrenheit', 'N/A')
            condition = weather_data.get('weather_condition', 'N/A')
            cloud_cover = weather_data.get('cloud_cover_percent', 'N/A')
            wind_speed = weather_data.get('wind_speed_mph', 'N/A')
            wind_dir = weather_data.get('wind_direction_degrees', None)
            humidity = weather_data.get('humidity_percent', 'N/A')
            obs_time = weather_data.get('observation_time_est', 'N/A')
            
            wind_dir_name = get_wind_direction(wind_dir)
            
            popup_html = f"""
            <div style='width: 250px; font-family: sans-serif;'>
                <h4 style='margin: 0 0 10px 0; color: #333;'>{hood_name}</h4>
                <table style='width: 100%; border-collapse: collapse;'>
                    <tr style='background-color: #f0f0f0;'>
                        <td style='padding: 5px; font-weight: bold;'>Temperature</td>
                        <td style='padding: 5px;'>{temp_val}°F</td>
                    </tr>
                    <tr>
                        <td style='padding: 5px; font-weight: bold;'>Feels Like</td>
                        <td style='padding: 5px;'>{feels_like}°F</td>
                    </tr>
                    <tr style='background-color: #f0f0f0;'>
                        <td style='padding: 5px; font-weight: bold;'>Condition</td>
                        <td style='padding: 5px;'>{condition}</td>
                    </tr>
                    <tr>
                        <td style='padding: 5px; font-weight: bold;'>Cloud Cover</td>
                        <td style='padding: 5px;'>{cloud_cover}%</td>
                    </tr>
                    <tr style='background-color: #f0f0f0;'>
                        <td style='padding: 5px; font-weight: bold;'>Wind</td>
                        <td style='padding: 5px;'>{wind_speed} mph {wind_dir_name}</td>
                    </tr>
                    <tr>
                        <td style='padding: 5px; font-weight: bold;'>Humidity</td>
                        <td style='padding: 5px;'>{humidity}%</td>
                    </tr>
                </table>
                <p style='margin: 10px 0 0 0; font-size: 11px; color: #666;'>
                    Updated: {obs_time}
                </p>
            </div>
            """
        else:
            popup_html = f"""
            <div style='width: 200px; font-family: sans-serif;'>
                <h4 style='margin: 0 0 10px 0; color: #333;'>{hood_name}</h4>
                <p>No weather data available</p>
            </div>
            """
        
        # Add individual polygon with popup
        folium.GeoJson(
            feature,
            style_function=lambda x, s=style: s,
            highlight_function=lambda x: {
                'fillColor': '#ffff00',
                'color': '#000000',
                'weight': 3,
                'fillOpacity': 0.8
            },
            popup=folium.Popup(popup_html, max_width=300),
            tooltip=hood_name  # Always show labels
        ).add_to(m)
    
    # Display map
    st_folium(m, width=None, height=600, returned_objects=[])
    
    # Show data table below map
    with st.expander("View All Current Weather Data"):
        display_df = weather_df[[
            'neighborhood_name', 'temp_fahrenheit', 'feels_like_fahrenheit',
            'weather_condition', 'cloud_cover_percent', 'wind_speed_mph',
            'humidity_percent', 'observation_time_est'
        ]].copy()
        
        display_df.columns = [
            'Neighborhood', 'Temp (°F)', 'Feels Like (°F)',
            'Condition', 'Cloud Cover (%)', 'Wind (mph)',
            'Humidity (%)', 'Observation Time'
        ]
        
        st.dataframe(
            display_df,
            hide_index=True,
            use_container_width=True
        )

except Exception as e:
    st.error(f"Error loading data: {str(e)}")
    
    # Debug info
    with st.expander("Debug Info", expanded=True):
        st.write("**Error details:**")
        st.code(str(e))
        
        st.write("**Path debugging:**")
        script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
        st.write(f"Script directory: `{script_dir}`")
        st.write(f"Current working directory: `{os.getcwd()}`")
        
        path1 = os.path.join(script_dir, '..', 'etl', 'pittsburgh_neighborhoods.geojson')
        path1 = os.path.abspath(path1)
        st.write(f"Path 1 (relative): `{path1}`")
        st.write(f"Path 1 exists: {os.path.exists(path1)}")
        
        path2 = os.path.join(os.getcwd(), 'etl', 'pittsburgh_neighborhoods.geojson')
        st.write(f"Path 2 (from cwd): `{path2}`")
        st.write(f"Path 2 exists: {os.path.exists(path2)}")
        
        st.write("**Environment Variables:**")
        st.write(f"- DB_HOST: {os.getenv('WEATHER_DB_HOST', 'NOT SET')}")
        st.write(f"- DB_PORT: {os.getenv('WEATHER_DB_PORT', 'NOT SET')}")
        st.write(f"- DB_NAME: {os.getenv('WEATHER_DB_NAME', 'NOT SET')}")
        st.write(f"- DB_USER: {os.getenv('WEATHER_DB_USER', 'NOT SET')}")
        st.write(f"- DB_SCHEMA: {os.getenv('WEATHER_DB_SCHEMA', 'NOT SET')}")