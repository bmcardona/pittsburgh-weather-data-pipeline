"""
Custom styling module for NYC Weather Analytics Dashboard
Implements a Stripe-inspired fintech aesthetic
"""

import streamlit as st

def apply_custom_styling():
    """
    Apply custom CSS styling to create a professional, Stripe-inspired UI.
    This function should be called once at the top of each page.
    """
    st.markdown("""
    <style>
        /* Import Google Fonts */
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
        
        /* Global Styles */
        * {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
        }
        
        /* Hide Streamlit Branding */
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {visibility: hidden;}
        
        /* Main Container */
        .main {
            background: linear-gradient(135deg, #f8fafc 0%, #f1f5f9 100%);
            padding: 2rem 3rem;
        }
        
        .block-container {
            max-width: 1400px;
            padding-top: 2rem;
            padding-bottom: 2rem;
        }
        
        /* Typography */
        h1 {
            font-weight: 700;
            font-size: 2.5rem;
            color: #0f172a;
            letter-spacing: -0.025em;
            margin-bottom: 0.5rem;
            line-height: 1.2;
        }
        
        h2 {
            font-weight: 600;
            font-size: 1.875rem;
            color: #1e293b;
            letter-spacing: -0.02em;
            margin-top: 2rem;
            margin-bottom: 1rem;
        }
        
        h3 {
            font-weight: 600;
            font-size: 1.25rem;
            color: #334155;
            margin-bottom: 0.75rem;
        }
        
        p, .subtitle {
            font-size: 1.0625rem;
            color: #64748b;
            line-height: 1.6;
            font-weight: 400;
        }
        
        /* Sidebar Styling */
        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #ffffff 0%, #f8fafc 100%) !important;
            border-right: 1px solid #e2e8f0 !important;
            padding: 1.5rem 1rem !important;
        }
        
        [data-testid="stSidebar"] > div:first-child {
            background: linear-gradient(180deg, #ffffff 0%, #f8fafc 100%) !important;
        }
        
        [data-testid="stSidebar"] h1,
        [data-testid="stSidebar"] h2,
        [data-testid="stSidebar"] h3 {
            color: #0f172a !important;
        }
        
        /* Custom Card Component */
        .custom-card {
            background: white;
            border-radius: 12px;
            padding: 1.5rem;
            box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.1), 0 1px 2px 0 rgba(0, 0, 0, 0.06);
            border: 1px solid #e2e8f0;
            margin-bottom: 1.5rem;
            transition: all 0.3s ease;
        }
        
        .custom-card:hover {
            box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -2px rgba(0, 0, 0, 0.05);
            transform: translateY(-2px);
        }
        
        .custom-card-header {
            font-size: 0.875rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            color: #64748b;
            margin-bottom: 1rem;
            border-bottom: 2px solid #e2e8f0;
            padding-bottom: 0.5rem;
        }
        
        /* Metric Cards */
        [data-testid="stMetric"] {
            background: white;
            border-radius: 12px;
            padding: 1.25rem 1.5rem;
            box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.1), 0 1px 2px 0 rgba(0, 0, 0, 0.06);
            border: 1px solid #e2e8f0;
            transition: all 0.3s ease;
        }
        
        [data-testid="stMetric"]:hover {
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
            border-color: #6366f1;
        }
        
        [data-testid="stMetric"] label {
            font-size: 0.875rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            color: #64748b;
        }
        
        [data-testid="stMetric"] [data-testid="stMetricValue"] {
            font-size: 2rem;
            font-weight: 700;
            color: #0f172a;
        }
        
        [data-testid="stMetric"] [data-testid="stMetricDelta"] {
            font-size: 0.875rem;
            font-weight: 500;
        }
        
        /* Buttons */
        .stButton button {
            background: linear-gradient(135deg, #6366f1 0%, #8b5cf6 100%);
            color: white;
            border: none;
            border-radius: 8px;
            padding: 0.625rem 1.25rem;
            font-weight: 600;
            font-size: 0.9375rem;
            letter-spacing: 0.025em;
            transition: all 0.3s ease;
            box-shadow: 0 1px 3px 0 rgba(99, 102, 241, 0.3);
        }
        
        .stButton button:hover {
            background: linear-gradient(135deg, #4f46e5 0%, #7c3aed 100%);
            box-shadow: 0 4px 6px -1px rgba(99, 102, 241, 0.4);
            transform: translateY(-1px);
        }
        
        /* Select Boxes and Inputs */
        .stSelectbox, .stMultiSelect, .stTextInput, .stDateInput {
            border-radius: 8px;
        }
        
        .stSelectbox > div > div,
        .stMultiSelect > div > div,
        .stTextInput > div > div > input,
        .stDateInput > div > div > input {
            border-radius: 8px;
            border: 1px solid #e2e8f0;
            background: white;
            transition: all 0.2s ease;
        }
        
        .stSelectbox > div > div:focus-within,
        .stMultiSelect > div > div:focus-within,
        .stTextInput > div > div > input:focus,
        .stDateInput > div > div > input:focus {
            border-color: #6366f1;
            box-shadow: 0 0 0 3px rgba(99, 102, 241, 0.1);
        }
        
        /* Data Tables */
        [data-testid="stDataFrame"] {
            border-radius: 12px;
            overflow: hidden;
            border: 1px solid #e2e8f0;
        }
        
        [data-testid="stDataFrame"] table {
            font-size: 0.9375rem;
        }
        
        [data-testid="stDataFrame"] thead tr th {
            background: linear-gradient(180deg, #f8fafc 0%, #f1f5f9 100%);
            color: #0f172a;
            font-weight: 600;
            text-transform: uppercase;
            font-size: 0.8125rem;
            letter-spacing: 0.05em;
            padding: 1rem;
            border-bottom: 2px solid #e2e8f0;
        }
        
        [data-testid="stDataFrame"] tbody tr {
            border-bottom: 1px solid #f1f5f9;
            transition: background-color 0.2s ease;
        }
        
        [data-testid="stDataFrame"] tbody tr:hover {
            background-color: #f8fafc;
        }
        
        [data-testid="stDataFrame"] tbody td {
            padding: 0.875rem 1rem;
            color: #334155;
        }
        
        /* Tabs */
        .stTabs [data-baseweb="tab-list"] {
            gap: 0.5rem;
            background: transparent;
            border-bottom: 2px solid #e2e8f0;
        }
        
        .stTabs [data-baseweb="tab"] {
            border-radius: 8px 8px 0 0;
            padding: 0.75rem 1.5rem;
            background: transparent;
            border: none;
            color: #64748b;
            font-weight: 600;
            font-size: 0.9375rem;
        }
        
        .stTabs [data-baseweb="tab"]:hover {
            background: #f8fafc;
            color: #334155;
        }
        
        .stTabs [aria-selected="true"] {
            background: white;
            color: #6366f1;
            border-bottom: 2px solid #6366f1;
        }
        
        /* Expander */
        .streamlit-expanderHeader {
            background: white;
            border-radius: 8px;
            border: 1px solid #e2e8f0;
            padding: 1rem;
            font-weight: 600;
            color: #0f172a;
        }
        
        .streamlit-expanderHeader:hover {
            border-color: #6366f1;
            background: #f8fafc;
        }
        
        /* Spinner */
        .stSpinner > div {
            border-top-color: #6366f1;
        }
        
        /* Divider */
        hr {
            margin: 2.5rem 0;
            border: none;
            border-top: 1px solid #e2e8f0;
        }
        
        /* Info/Warning/Error Boxes */
        .stAlert {
            border-radius: 8px;
            border: 1px solid;
            padding: 1rem 1.25rem;
        }
        
        [data-testid="stNotification"] [data-testid="stNotificationContentInfo"] {
            background: #eff6ff;
            border-color: #3b82f6;
            color: #1e40af;
        }
        
        [data-testid="stNotification"] [data-testid="stNotificationContentSuccess"] {
            background: #ecfdf5;
            border-color: #10b981;
            color: #065f46;
        }
        
        [data-testid="stNotification"] [data-testid="stNotificationContentWarning"] {
            background: #fffbeb;
            border-color: #f59e0b;
            color: #92400e;
        }
        
        [data-testid="stNotification"] [data-testid="stNotificationContentError"] {
            background: #fef2f2;
            border-color: #ef4444;
            color: #991b1b;
        }
        
        /* Radio Buttons */
        .stRadio > div {
            gap: 0.5rem;
        }
        
        .stRadio label {
            background: white;
            border: 1px solid #e2e8f0;
            border-radius: 8px;
            padding: 0.625rem 1rem;
            transition: all 0.2s ease;
        }
        
        .stRadio label:hover {
            border-color: #6366f1;
            background: #f8fafc;
        }
        
        /* Charts */
        .js-plotly-plot {
            border-radius: 12px;
            overflow: hidden;
        }
        
        /* Loading Animation */
        @keyframes shimmer {
            0% { background-position: -1000px 0; }
            100% { background-position: 1000px 0; }
        }
        
        .loading-shimmer {
            animation: shimmer 2s infinite linear;
            background: linear-gradient(to right, #f1f5f9 0%, #e2e8f0 20%, #f1f5f9 40%, #f1f5f9 100%);
            background-size: 1000px 100%;
        }
    </style>
    """, unsafe_allow_html=True)


def get_color_palette():
    """
    Return the color palette for consistent theming across the dashboard.
    Inspired by Tailwind CSS and Stripe's design system.
    """
    return {
        # Primary colors
        'primary': '#6366f1',  # Indigo 500
        'primary_dark': '#4f46e5',  # Indigo 600
        'primary_light': '#818cf8',  # Indigo 400
        
        # Secondary colors
        'secondary': '#8b5cf6',  # Violet 500
        'secondary_dark': '#7c3aed',  # Violet 600
        
        # Accent colors
        'accent': '#ec4899',  # Pink 500
        'accent_light': '#f472b6',  # Pink 400
        
        # Status colors
        'success': '#10b981',  # Emerald 500
        'warning': '#f59e0b',  # Amber 500
        'error': '#ef4444',  # Red 500
        'info': '#3b82f6',  # Blue 500
        
        # Neutral colors
        'slate_50': '#f8fafc',
        'slate_100': '#f1f5f9',
        'slate_200': '#e2e8f0',
        'slate_300': '#cbd5e1',
        'slate_400': '#94a3b8',
        'slate_500': '#64748b',
        'slate_600': '#475569',
        'slate_700': '#334155',
        'slate_800': '#1e293b',
        'slate_900': '#0f172a',
        
        # Chart colors (for multi-series charts)
        'chart_colors': [
            '#6366f1',  # Indigo
            '#8b5cf6',  # Violet
            '#ec4899',  # Pink
            '#10b981',  # Emerald
            '#f59e0b',  # Amber
            '#3b82f6',  # Blue
        ]
    }


def get_plotly_layout_config():
    """
    Return a standardized Plotly layout configuration for consistent chart styling.
    Note: Does not include 'title', 'hovermode', or 'legend' to avoid conflicts.
    Set these on a per-chart basis.
    """
    colors = get_color_palette()
    
    return {
        'template': 'plotly_white',
        'font': {
            'family': 'Inter, sans-serif',
            'size': 13,
            'color': colors['slate_700']
        },
        'plot_bgcolor': 'white',
        'paper_bgcolor': 'white',
        'margin': {'l': 60, 'r': 40, 't': 60, 'b': 60},
        'hoverlabel': {
            'bgcolor': 'white',
            'bordercolor': colors['slate_200'],
            'font': {
                'family': 'Inter, sans-serif',
                'size': 12,
                'color': colors['slate_900']
            }
        },
        'xaxis': {
            'showgrid': False,
            'showline': True,
            'linewidth': 1,
            'linecolor': colors['slate_200'],
            'tickfont': {'size': 12, 'color': colors['slate_600']},
            'title': {'font': {'size': 13, 'weight': 600, 'color': colors['slate_700']}}
        },
        'yaxis': {
            'showgrid': True,
            'gridwidth': 1,
            'gridcolor': colors['slate_100'],
            'showline': False,
            'tickfont': {'size': 12, 'color': colors['slate_600']},
            'title': {'font': {'size': 13, 'weight': 600, 'color': colors['slate_700']}}
        }
    }


def create_metric_card(label, value, delta=None, delta_color="normal"):
    """
    Create a custom metric card with enhanced styling.
    
    Args:
        label: The metric label
        value: The metric value
        delta: Optional delta/change value
        delta_color: Color of delta ("normal", "inverse", "off")
    """
    st.metric(label=label, value=value, delta=delta, delta_color=delta_color)


def create_section_header(title, description=None):
    """
    Create a consistent section header with optional description.
    
    Args:
        title: Section title
        description: Optional description text
    """
    st.markdown(f"## {title}")
    if description:
        st.markdown(f'<p class="subtitle">{description}</p>', unsafe_allow_html=True)