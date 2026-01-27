import streamlit as st
import pandas as pd
import psycopg2
import plotly.graph_objects as go
import os
from datetime import datetime, timedelta

st.set_page_config(
    page_title="Hourly Temperature Trends",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("Hourly Temperature Trends")
st.markdown("Track temperature changes by neighborhood throughout the day")

# Database connection
@st.cache_resource
def get_connection():
    """Connect to the database"""
    return psycopg2.connect(
        host=os.getenv('WEATHER_DB_HOST', 'postgres'),
        port=os.getenv('WEATHER_DB_PORT', '5432'),
        database=os.getenv('WEATHER_DB_NAME', 'weather_db'),
        user=os.getenv('WEATHER_DB_USER', 'weather_user'),
        password=os.getenv('WEATHER_DB_PASSWORD', 'weather_password')
    )

# Load data
@st.cache_data(ttl=300)
def load_hourly_data(_conn):
    """Load today's hourly weather data"""
    query = """
        SELECT *
        FROM weather_analytics.mart_hourly_weather
    """
    df = pd.read_sql(query, _conn)
    df['observation_time_est'] = pd.to_datetime(df['observation_time_est'])
    return df

# Connect and load data
try:
    conn = get_connection()
    df = load_hourly_data(conn)
except Exception as e:
    st.error(f"Error loading data: {e}")
    st.stop()

if df.empty:
    st.warning("No data available for today. Please check back later.")
    st.stop()

# Sidebar filters
st.sidebar.header("Filters")

# Get unique neighborhoods and community boards
all_neighborhoods = sorted(df['neighborhood_name'].unique())
all_boards = sorted(df['community_board'].unique())

# Neighborhood selector
selected_neighborhoods = st.sidebar.multiselect(
    "Select Neighborhoods",
    options=all_neighborhoods,
    default=all_neighborhoods[:5]  # Default to first 5
)

# Community board selector
selected_boards = st.sidebar.multiselect(
    "Select Community Boards",
    options=all_boards,
    default=[]
)

# Filter data based on selections
if selected_neighborhoods or selected_boards:
    # Combine filters
    mask = pd.Series([False] * len(df))
    
    if selected_neighborhoods:
        mask = mask | df['neighborhood_name'].isin(selected_neighborhoods)
    
    if selected_boards:
        mask = mask | df['community_board'].isin(selected_boards)
    
    filtered_df = df[mask]
else:
    filtered_df = df

# Show summary stats
col1, col2, col3, col4 = st.columns(4)
col1.metric("Neighborhoods Shown", filtered_df['neighborhood_name'].nunique())
col2.metric("Current Avg Temp", f"{filtered_df['temp_fahrenheit'].mean():.1f}°F")
col3.metric("High Today", f"{filtered_df['temp_fahrenheit'].max():.1f}°F")
col4.metric("Low Today", f"{filtered_df['temp_fahrenheit'].min():.1f}°F")

# Create the line chart
st.markdown("### Temperature Throughout the Day")

fig = go.Figure()

# Generate unique colors for all neighborhoods using a colormap
import plotly.express as px
num_neighborhoods = filtered_df['neighborhood_name'].nunique()
colors = px.colors.sample_colorscale("turbo", [n/(num_neighborhoods-1) for n in range(num_neighborhoods)])

# Plot each neighborhood as a separate line
for idx, neighborhood in enumerate(sorted(filtered_df['neighborhood_name'].unique())):
    neighborhood_data = filtered_df[filtered_df['neighborhood_name'] == neighborhood].sort_values('observation_time_est')
    
    fig.add_trace(go.Scatter(
        x=neighborhood_data['observation_time_est'],
        y=neighborhood_data['temp_fahrenheit'],
        mode='lines+markers',
        name=neighborhood,
        line=dict(color=colors[idx], width=2),
        marker=dict(size=6),
        hovertemplate='<b>%{fullData.name}</b><br>Time: %{x|%I:%M %p}<br>Temp: %{y:.1f}°F<extra></extra>'
    ))

# Update layout
fig.update_layout(
    xaxis_title="Time",
    yaxis_title="Temperature (°F)",
    hovermode='x unified',
    legend=dict(
        orientation="v",
        yanchor="top",
        y=1,
        xanchor="left",
        x=1.02
    ),
    height=600,
    template='plotly_white',
    margin=dict(r=200)  # Make room for legend
)

# Format x-axis to show time nicely
fig.update_xaxes(tickformat='%I:%M %p')

st.plotly_chart(fig, use_container_width=True)

# Show data table
with st.expander("View Raw Data"):
    display_df = filtered_df[['observation_time_est', 'neighborhood_name', 'community_board', 'temp_fahrenheit']].copy()
    display_df['observation_time_est'] = display_df['observation_time_est'].dt.strftime('%I:%M %p')
    display_df.columns = ['Time', 'Neighborhood', 'Community Board', 'Temperature (°F)']
    st.dataframe(display_df, width='stretch', height=400)

# Footer
st.markdown("---")
st.caption(f"Showing data for {datetime.now().strftime('%B %d, %Y')}")