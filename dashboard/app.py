#!/usr/bin/env python3
"""
SentiCheck Dashboard - Main Streamlit Application

A modern, minimal dashboard for sentiment analysis insights.
Following the SentiCheck UI Style Guide for clean, scannable visualizations.
"""

import streamlit as st
import pandas as pd
import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

# Add parent directory to path for imports
parent_dir = Path(__file__).parent.parent
sys.path.insert(0, str(parent_dir))

try:
    from models.db_manager import get_db_manager
    from dashboard.data_service import get_dashboard_data_service
    from dashboard.config import DASHBOARD_CONFIG
except ImportError as e:
    st.error(f"Import error: {e}")
    st.error("Please ensure all required modules are available.")
    st.stop()


def configure_page():
    """Configure Streamlit page settings following the style guide."""
    st.set_page_config(
        page_title="SentiCheck",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="collapsed",
        menu_items={"About": "SentiCheck - Modern sentiment analysis dashboard"},
    )


def render_page_title():
    """Render the main page title with clean, minimal styling and last updated timestamp."""
    # Create a container with the timestamp in top right
    col1, col2 = st.columns([3, 1])

    with col2:
        st.markdown(
            f"""
            <div style="text-align: right; margin-bottom: 16px;">
                <span style="color: #6b7280; font-size: 14px;">
                    🕒 Last Updated: {datetime.now().strftime('%H:%M:%S')}
                </span>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown(
        """
        <div style="text-align: center; margin-bottom: 32px;">
            <h1 style="font-size: 48px; font-weight: 600; margin-bottom: 8px; color: #111827;">
                📊 SentiCheck
            </h1>
            <div style="color: #6b7280; font-size: 18px; line-height: 1.5;">
                Real-time sentiment insights from social media posts
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_metric_card(
    title: str,
    value: str,
    delta: Optional[str] = None,
    delta_color: str = "normal",
    help_text: Optional[str] = None,
):
    """
    Render a KPI metric card following the style guide.

    Args:
        title: The metric label
        value: The primary value to display
        delta: Optional delta value with sign and unit
        delta_color: Color for delta ('normal', 'inverse', 'off')
        help_text: Optional help text for the metric
    """
    # Create card container with white background and subtle border
    card_html = f"""
    <div class="metric-card">
        <div class="metric-header">
            <span class="metric-title">{title}</span>
            {f'<div class="metric-help-container"><span class="metric-help">?</span><div class="metric-tooltip">{help_text}</div></div>' if help_text else ''}
        </div>
        <div class="metric-value">{value}</div>
        {f'<div class="metric-delta metric-delta-{delta_color}">{delta}</div>' if delta else ''}
    </div>
    """

    st.markdown(card_html, unsafe_allow_html=True)


def render_kpi_section():
    """Render the KPI section with key sentiment metrics."""
    st.header("Key Performance Indicators")
    st.markdown("---")

    # Get data service
    try:
        data_service = get_dashboard_data_service()
        kpi_data = data_service.get_kpi_metrics()

        # Create 3-column layout for first row (follows max 3 columns rule)
        col1, col2, col3 = st.columns(3, gap="medium")

        with col1:
            render_metric_card(
                title="Total Posts Analyzed",
                value=f"{kpi_data['total_posts']:,}",
                delta=(
                    f"↑ +{kpi_data['posts_today']:,} today"
                    if kpi_data["posts_today"] > 0
                    else None
                ),
                delta_color="normal",
                help_text="Total number of posts processed through sentiment analysis",
            )

        with col2:
            render_metric_card(
                title="Positive Sentiment",
                value=f"{kpi_data['positive_percentage']:.1f}%",
                delta=(
                    f"{'↑' if kpi_data['positive_trend'] >= 0 else '↓'} {kpi_data['positive_trend']:+.1f}% vs yesterday"
                    if kpi_data["positive_trend"] != 0
                    else "→ No change"
                ),
                delta_color="normal" if kpi_data["positive_trend"] >= 0 else "inverse",
                help_text="Percentage of posts with positive sentiment - increases are good (green), decreases are concerning (red)",
            )

        with col3:
            render_metric_card(
                title="Average Confidence",
                value=f"{kpi_data['avg_confidence']:.1f}%",
                delta=(
                    f"{'↑' if kpi_data['confidence_trend'] >= 0 else '↓'} {kpi_data['confidence_trend']:+.1f}% vs yesterday"
                    if kpi_data["confidence_trend"] != 0
                    else "→ Stable"
                ),
                delta_color=(
                    "normal" if kpi_data["confidence_trend"] >= 0 else "inverse"
                ),
                help_text="Average confidence score of sentiment predictions",
            )

        # Add spacing between rows following 8px scale (24px)
        st.markdown("<div style='margin-top: 24px;'></div>", unsafe_allow_html=True)

        # Second row of KPIs
        col4, col5, col6 = st.columns(3, gap="medium")

        with col4:
            # For negative sentiment: decreasing is good (green), increasing is bad (red)
            negative_delta_color = (
                "normal" if kpi_data["negative_trend"] <= 0 else "inverse"
            )
            render_metric_card(
                title="Negative Sentiment",
                value=f"{kpi_data['negative_percentage']:.1f}%",
                delta=(
                    f"{'↓' if kpi_data['negative_trend'] <= 0 else '↑'} {kpi_data['negative_trend']:+.1f}% vs yesterday"
                    if kpi_data["negative_trend"] != 0
                    else "→ No change"
                ),
                delta_color=negative_delta_color,
                help_text="Percentage of posts with negative sentiment - decreases are good (green), increases are concerning (red)",
            )

        with col5:
            # For neutral sentiment: direction is less important, use off/gray for trends
            render_metric_card(
                title="Neutral Sentiment",
                value=f"{kpi_data['neutral_percentage']:.1f}%",
                delta=(
                    f"{'↑' if kpi_data['neutral_trend'] >= 0 else '↓'} {kpi_data['neutral_trend']:+.1f}% vs yesterday"
                    if kpi_data["neutral_trend"] != 0
                    else "→ No change"
                ),
                delta_color="off",  # Always neutral/gray for neutral sentiment trends
                help_text="Percentage of posts with neutral sentiment",
            )

        with col6:
            # For posts today: fewer posts might be concerning, but could be normal daily variation
            posts_delta_color = (
                "inverse"
                if kpi_data["daily_trend"] < -50
                else ("normal" if kpi_data["daily_trend"] >= 0 else "off")
            )
            render_metric_card(
                title="Posts Today",
                value=f"{kpi_data['posts_today']:,}",
                delta=(
                    f"{'↑' if kpi_data['daily_trend'] >= 0 else '↓'} {kpi_data['daily_trend']:+.1f}% vs yesterday"
                    if kpi_data["daily_trend"] != 0
                    else "→ No change"
                ),
                delta_color=posts_delta_color,
                help_text="Number of posts processed today - large changes may indicate data collection issues",
            )

    except Exception as e:
        st.error(f"Error loading KPI data: {e}")
        # Show placeholder KPIs for demonstration
        st.info(
            "Showing placeholder data - database connection needed for real metrics"
        )

        col1, col2, col3 = st.columns(3, gap="medium")

        with col1:
            render_metric_card(
                title="Total Posts Analyzed",
                value="1,247",
                delta="↑ +23 today",
                delta_color="normal",
                help_text="Total number of posts processed through sentiment analysis",
            )

        with col2:
            render_metric_card(
                title="Positive Sentiment",
                value="64.2%",
                delta="↑ +2.1% vs yesterday",
                delta_color="normal",
                help_text="Percentage of posts with positive sentiment",
            )

        with col3:
            render_metric_card(
                title="Average Confidence",
                value="87.3%",
                delta="↑ +1.2% vs yesterday",
                delta_color="normal",
                help_text="Average confidence score of sentiment predictions",
            )

        # Add spacing between rows following 8px scale (24px)
        st.markdown("<div style='margin-top: 24px;'></div>", unsafe_allow_html=True)

        # Second row of placeholder KPIs
        col4, col5, col6 = st.columns(3, gap="medium")

        with col4:
            render_metric_card(
                title="Negative Sentiment",
                value="18.7%",
                delta="↓ -1.3% vs yesterday",
                delta_color="normal",  # Negative sentiment decreasing is good (green)
                help_text="Percentage of posts with negative sentiment",
            )

        with col5:
            render_metric_card(
                title="Neutral Sentiment",
                value="17.1%",
                delta="↓ -0.8% vs yesterday",
                delta_color="off",
                help_text="Percentage of posts with neutral sentiment",
            )

        with col6:
            render_metric_card(
                title="Posts Today",
                value="23",
                delta="↑ +15.0% vs yesterday",
                delta_color="normal",
                help_text="Number of posts processed today",
            )


def main():
    """Main dashboard application."""
    # Configure page
    configure_page()

    # Render page title
    render_page_title()

    # Add custom CSS for better styling and force light theme
    st.markdown(
        """
    <style>
    /* Force light theme - override browser dark mode preferences */
    :root {
        color-scheme: light !important;
    }
    
    html, body {
        background-color: #ffffff !important;
        color: #1f2937 !important;
    }
    
    /* Force Streamlit app container to be white */
    .stApp, .main, .block-container {
        background-color: #ffffff !important;
        color: #1f2937 !important;
    }
    
    /* Force sidebar to be light - comprehensive fix */
    .css-1d391kg, .sidebar .sidebar-content, section[data-testid="stSidebar"], 
    .css-1lcbmhc, .css-1cypcdb, .css-17ziqus, .stSidebar, 
    .css-1aumxhk, section[data-testid="stSidebar"] > div {
        background-color: #ffffff !important;
        color: #1f2937 !important;
    }
    
    /* Sidebar header and content styling */
    .stSidebar h2, .stSidebar h3, .stSidebar p, .stSidebar span,
    .stSidebar label, .stSidebar div {
        color: #1f2937 !important;
        background-color: transparent !important;
    }
    
    /* Follow style guide - minimal, white, clean */
    .main > div {
        padding-top: 2rem;
        background-color: #ffffff !important;
    }
    
    /* Custom metric cards - following style guide with proper proportions */
    .metric-card {
        background-color: #ffffff !important;
        border: 1px solid #e5e7eb !important;
        border-radius: 8px;
        padding: 20px;
        margin-bottom: 16px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.05);
        transition: box-shadow 0.2s ease;
        display: flex;
        flex-direction: column;
        justify-content: flex-start;
        align-items: center;
        text-align: center;
        height: 140px;
        width: 100%;
        margin: 0 auto 16px auto;
    }
    
    .metric-card:hover {
        box-shadow: 0 4px 12px rgba(0,0,0,0.08);
    }
    
    .metric-header {
        display: flex;
        justify-content: center;
        align-items: center;
        margin-bottom: 12px;
        width: 100%;
        position: relative;
    }
    
    .metric-title {
        font-size: 14px;
        font-weight: 500;
        color: #6b7280 !important;
        text-transform: none;
        text-align: center;
        flex-grow: 1;
        line-height: 1.4;
    }
    
    .metric-help-container {
        position: absolute;
        right: 0;
        top: 50%;
        transform: translateY(-50%);
    }
    
    .metric-help {
        width: 16px;
        height: 16px;
        border-radius: 50%;
        background-color: #f3f4f6;
        color: #6b7280 !important;
        font-size: 10px;
        font-weight: bold;
        text-align: center;
        line-height: 16px;
        cursor: help;
        opacity: 0.7;
        display: block;
        transition: opacity 0.2s ease;
    }
    
    .metric-help:hover {
        opacity: 1;
        background-color: #e5e7eb;
    }
    
    .metric-tooltip {
        position: absolute;
        bottom: 20px;
        right: -50px;
        background-color: #1f2937;
        color: #ffffff !important;
        padding: 8px 12px;
        border-radius: 6px;
        font-size: 12px;
        line-height: 1.4;
        width: 220px;
        z-index: 1000;
        opacity: 0;
        visibility: hidden;
        transform: translateY(5px);
        transition: all 0.2s ease;
        box-shadow: 0 4px 12px rgba(0,0,0,0.15);
        text-align: left;
    }
    
    .metric-help-container:hover .metric-tooltip {
        opacity: 1;
        visibility: visible;
        transform: translateY(0);
    }
    
    .metric-value {
        font-size: 28px;
        font-weight: 600;
        color: #111827 !important;
        margin-bottom: 12px;
        line-height: 1.1;
        text-align: center;
    }
    
    .metric-delta {
        font-size: 12px;
        font-weight: 500;
        padding: 4px 8px;
        border-radius: 12px;
        display: inline-block;
        text-align: center;
        line-height: 1.2;
    }
    
    .metric-delta-normal {
        background-color: #d1fae5;
        color: #059669 !important;
    }
    
    .metric-delta-inverse {
        background-color: #fee2e2;
        color: #dc2626 !important;
    }
    
    .metric-delta-off {
        background-color: #f3f4f6;
        color: #6b7280 !important;
    }
    
    /* Legacy metric container styling for fallback */
    div[data-testid="metric-container"] {
        background-color: #ffffff !important;
        border: 1px solid #e0e0e0 !important;
        padding: 1rem;
        border-radius: 0.5rem;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        color: #1f2937 !important;
    }
    
    /* Force metric values to be dark text */
    div[data-testid="metric-container"] * {
        color: #1f2937 !important;
    }
    
    /* Header styling - force dark text */
    h1, h2, h3, h4, h5, h6 {
        color: #1f2937 !important;
        background-color: transparent !important;
    }
    
    h1 {
        font-size: 24px;
        font-weight: 600;
        margin-bottom: 8px;
    }
    
    /* Target h2 elements with maximum specificity to override Streamlit emotion-cache styles */
    .stApp h2, 
    .main h2, 
    .block-container h2, 
    div[data-testid="stMarkdownContainer"] h2,
    .stMarkdown h2,
    .element-container h2,
    .st-emotion-cache-r44huj h2,
    .st-emotion-cache-gf1xsr h2 {
        font-size: 28px !important;
        font-weight: 500 !important;
        margin: 2rem 0 0 0 !important;
        padding: 0 !important;
        color: #111827 !important;
        line-height: 1.2 !important;
    }
    
    /* Special targeting for sidebar h2 to keep its original styling */
    section[data-testid="stSidebar"] h2,
    section[data-testid="stSidebar"] .st-emotion-cache-r44huj h2 {
        font-size: 2.25rem !important;
        font-weight: 600 !important;
        margin-top: 0 !important;
        padding: 1rem 0 !important;
    }
    

    p, span, div, label {
        color: #1f2937 !important;
    }
    
    hr, .stMarkdown hr, div[data-testid="stMarkdownContainer"] hr {
        margin: 0.5rem 0 2rem 0px !important;
        border: none !important;
        border-top: 1px solid #e5e7eb !important;
        background-color: transparent !important;
        height: 1px !important;
        padding: 0 !important;
    }
    
    /* Alert/status boxes - ensure proper colors */
    .stAlert, div[data-testid="stAlert"] {
        background-color: #ffffff !important;
        border: 1px solid #e0e0e0 !important;
        color: #1f2937 !important;
    }
    
    /* Success alerts */
    .stAlert[data-baseweb="notification"] div {
        color: #059669 !important;
    }
    
    /* Error alerts */
    .stAlert[data-baseweb="notification"][kind="error"] div {
        color: #dc2626 !important;
    }
    
    /* Warning alerts */
    .stAlert[data-baseweb="notification"][kind="warning"] div {
        color: #d97706 !important;
    }
    
    /* Info alerts */
    .stAlert[data-baseweb="notification"][kind="info"] div {
        color: #2563eb !important;
    }
    
    /* Checkbox and other inputs */
    .stCheckbox, .stSelectbox, input {
        background-color: #ffffff !important;
        color: #1f2937 !important;
        border: 1px solid #e0e0e0 !important;
    }
    
    /* Hide Streamlit branding for cleaner look */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    
    /* Override any remaining dark mode elements */
    * {
        border-color: #e0e0e0 !important;
    }
    
    /* Ensure white backgrounds for all containers */
    .element-container, .stMarkdown, .stMetric {
        background-color: #ffffff !important;
    }
    </style>
    """,
        unsafe_allow_html=True,
    )

    render_kpi_section()

    st.markdown("<div style='margin-top: 32px;'></div>", unsafe_allow_html=True)

    with st.sidebar:
        st.header("Dashboard Settings")
        st.markdown("Dashboard configuration options will be available here.")


if __name__ == "__main__":
    main()
