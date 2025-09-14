#!/usr/bin/env python3

import streamlit as st
import sys
from pathlib import Path
from typing import Optional

parent_dir = Path(__file__).parent.parent
sys.path.insert(0, str(parent_dir))


from dashboard.charts import (
    render_sentiment_over_time_chart,
    render_sentiment_distribution_chart,
    render_volume_analysis_chart,
)
from dashboard.styles import apply_all_styles
from dashboard.data_service_api import get_dashboard_data_service
from dashboard.components import (
    render_sidebar_controls,
    update_session_state_from_sidebar,
)
from dashboard.components.wordcloud_section import render_wordcloud_section


def configure_page():
    """Configure Streamlit page settings."""
    st.set_page_config(
        page_title="SentiCheck",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded",
        menu_items={"About": "SentiCheck - Sentiment analysis dashboard"},
    )


def render_page_title():
    """Render the main page title"""
    st.markdown(
        """
        <div class="page-title-container">
            <h1 class="page-title">
                📊 SentiCheck
            </h1>
            <div class="page-subtitle">
                Sentiment insights from social media posts
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


def render_kpi_section(selected_keyword: str):
    """Render the KPI section with keyword-specific metrics."""
    try:
        st.header(f"Key Performance Indicators - {selected_keyword}")

        st.markdown("---")

        data_service = get_dashboard_data_service()
        kpi_data = data_service.get_kpi_metrics(keyword=selected_keyword, days=30)

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
                help_text="Percentage of posts with positive sentiment",
            )

        with col3:
            render_metric_card(
                title="Keyword Confidence",
                value=f"{kpi_data['confidence_score']:.1f}%",
                delta=(
                    f"{'↑' if kpi_data['confidence_trend'] >= 0 else '↓'} {kpi_data['confidence_trend']:+.1f}% trend"
                    if kpi_data["confidence_trend"] != 0
                    else "→ Stable"
                ),
                delta_color=(
                    "positive" if kpi_data["confidence_trend"] >= 0 else "negative"
                ),
                help_text=f"Average confidence score for {selected_keyword} sentiment predictions",
            )

        st.markdown('<div class="spacing-top-2xl"></div>', unsafe_allow_html=True)

        col4, col5, col6 = st.columns(3, gap="medium")

        with col4:
            week_trend_color = "positive" if kpi_data["week_trend"] >= 0 else "negative"
            render_metric_card(
                title="Posts This Week",
                value=f"{kpi_data['posts_this_week']:,}",
                delta=(
                    f"{'↑' if kpi_data['week_trend'] >= 0 else '↓'} {kpi_data['week_trend']:+.1f}% vs last week"
                    if kpi_data["week_trend"] != 0
                    else "→ No change"
                ),
                delta_color=week_trend_color,
                help_text=f"Weekly posts for {selected_keyword} with trend analysis",
            )

        with col5:
            momentum = kpi_data["sentiment_momentum"]
            momentum_icons = {"improving": "📈", "declining": "📉", "stable": "➡️"}
            momentum_colors = {
                "improving": "positive",
                "declining": "negative",
                "stable": "neutral",
            }

            render_metric_card(
                title="Sentiment Momentum",
                value=f"{momentum_icons.get(momentum, '➡️')} {momentum.title()}",
                delta=(
                    f"{kpi_data['momentum_change']:+.1f}% change"
                    if kpi_data["momentum_change"] != 0
                    else "No change"
                ),
                delta_color=momentum_colors.get(momentum, "neutral"),
                help_text="Recent sentiment trend direction for this keyword",
            )

        with col6:
            rank = kpi_data["keyword_rank"]
            total = kpi_data["total_keywords"]
            render_metric_card(
                title="Keyword Rank",
                value=f"#{rank} of {total}",
                delta=f"By post volume",
                delta_color="neutral",
                help_text=f"{selected_keyword} ranks #{rank} by total post count",
            )

        st.markdown('<div class="spacing-top-2xl"></div>', unsafe_allow_html=True)

        col7, col8, col9 = st.columns(3, gap="medium")

        with col7:
            render_metric_card(
                title="Daily Average",
                value=f"{kpi_data['daily_average']:.1f}",
                delta="posts per day",
                delta_color="neutral",
                help_text=f"Average daily posts for {selected_keyword} (30-day avg)",
            )

        with col8:
            peak_date = kpi_data["peak_date"]
            peak_value = (
                float(kpi_data["peak_sentiment"]) if kpi_data["peak_sentiment"] else 0.0
            )
            render_metric_card(
                title="Peak Performance",
                value=f"{peak_value:.1f}%",
                delta=f"on {peak_date}" if peak_date else "No data",
                delta_color="positive" if peak_value > 50 else "neutral",
                help_text="Best sentiment day in the last 30 days",
            )

        with col9:
            render_metric_card(
                title="Posts Today",
                value=f"{kpi_data['posts_today']:,}",
                delta=(
                    f"{'↑' if kpi_data['daily_trend'] >= 0 else '↓'} {kpi_data['daily_trend']:+.1f}% vs yesterday"
                    if kpi_data["daily_trend"] != 0
                    else "→ No change"
                ),
                delta_color=(
                    "negative"
                    if kpi_data["daily_trend"] < -50
                    else ("positive" if kpi_data["daily_trend"] >= 0 else "neutral")
                ),
                help_text="Number of posts processed today",
            )

    except Exception as e:
        st.error(f"Error loading KPI data: {e}")
        import traceback

        st.text(traceback.format_exc())


def render_chart_section(selected_keyword: str):
    """Render the charts section."""

    render_sentiment_over_time_chart(selected_keyword)

    col1, col2 = st.columns(2, gap="large")

    with col1:
        render_sentiment_distribution_chart(selected_keyword)

    with col2:
        render_volume_analysis_chart(selected_keyword)

    render_wordcloud_section(selected_keyword)


def main():
    """Main dashboard application."""
    configure_page()
    apply_all_styles()

    render_page_title()

    sidebar_data = render_sidebar_controls()
    update_session_state_from_sidebar(sidebar_data)

    selected_keyword = sidebar_data["selected_keyword"]

    render_kpi_section(selected_keyword)

    render_chart_section(selected_keyword)


if __name__ == "__main__":
    main()
