#!/usr/bin/env python3

import streamlit as st
import sys
from pathlib import Path
from typing import Optional

# Add parent directory to path for imports
parent_dir = Path(__file__).parent.parent
sys.path.insert(0, str(parent_dir))


from dashboard.charts import render_sentiment_over_time_chart
from dashboard.styles import apply_styles, apply_dropdown_styles
from dashboard.data_service import get_dashboard_data_service


def configure_page():
    """Configure Streamlit page settings."""
    st.set_page_config(
        page_title="SentiCheck",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="collapsed",
        menu_items={"About": "SentiCheck - Sentiment analysis dashboard"},
    )


def render_page_title():
    """Render the main page title"""
    st.markdown(
        """
        <div style="text-align: center; margin-bottom: 32px;"> #TODO: Check if these styles can be moved to styles.py
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
    Render a KPI metric card.

    Args:
        title: The metric label
        value: The primary value to display
        delta: Optional delta value with sign and unit
        delta_color: Color for delta ('normal', 'inverse', 'off')
        help_text: Optional help text for the metric
    """
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
                delta_color="positive",
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
                delta_color=(
                    "positive" if kpi_data["positive_trend"] >= 0 else "negative"
                ),
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
                    "positive" if kpi_data["confidence_trend"] >= 0 else "inverse"
                ),
                help_text="Average confidence score of sentiment predictions",
            )

        st.markdown("<div style='margin-top: 24px;'></div>", unsafe_allow_html=True)

        col4, col5, col6 = st.columns(3, gap="medium")

        with col4:
            # For negative sentiment: decreasing is good (green), increasing is bad (red)
            negative_delta_color = (
                "positive" if kpi_data["negative_trend"] <= 0 else "negative"
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
                delta_color="neutral",  # Always neutral/gray for neutral sentiment trends
                help_text="Percentage of posts with neutral sentiment",
            )

        with col6:
            # For posts today: fewer posts might be concerning, but could be normal daily variation
            posts_delta_color = (
                "negative"
                if kpi_data["daily_trend"] < -50
                else ("positive" if kpi_data["daily_trend"] >= 0 else "neutral")
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


def render_chart_section():
    """Render the charts section."""
    render_sentiment_over_time_chart()


def main():
    """Main dashboard application."""
    configure_page()
    apply_styles()
    apply_dropdown_styles()

    render_page_title()
    render_kpi_section()
    render_chart_section()


if __name__ == "__main__":
    main()
