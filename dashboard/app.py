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
from dashboard.data_service import get_dashboard_data_service
from dashboard.components import render_keyword_selector, render_insights_section


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
        <div class="page-title-container">
            <h1 class="page-title">
                📊 SentiCheck
            </h1>
            <div class="page-subtitle">
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


def render_kpi_section(selected_keywords=None):
    """Render the KPI section with keyword-specific metrics."""
    try:
        if selected_keywords and len(selected_keywords) > 0:
            selected_keyword = selected_keywords[0]
            keyword_text = selected_keyword
            st.header(f"Key Performance Indicators - {keyword_text}")
        else:
            selected_keyword = "AI"  # Default keyword
            st.header("Key Performance Indicators - All Keywords")

        st.markdown("---")

        data_service = get_dashboard_data_service()
        if (selected_keywords and len(selected_keywords) == 1) or (
            not selected_keywords
        ):
            kpi_data = data_service.get_kpi_metrics([selected_keyword])
            keyword_kpis = data_service.get_keyword_specific_kpis(selected_keyword)
        else:
            kpi_data = data_service.get_kpi_metrics(selected_keywords)
            keyword_kpis = None

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
            if keyword_kpis:
                render_metric_card(
                    title="Keyword Confidence",
                    value=f"{keyword_kpis['confidence_score']:.1f}%",
                    delta="→ Keyword-specific",
                    delta_color="positive",
                    help_text=f"Average confidence score for {selected_keyword} sentiment predictions",
                )
            else:
                render_metric_card(
                    title="Average Confidence",
                    value=f"{kpi_data['avg_confidence']:.1f}%",
                    delta="→ Stable",
                    delta_color="positive",
                    help_text="Average confidence score of sentiment predictions",
                )

        st.markdown('<div class="spacing-top-2xl"></div>', unsafe_allow_html=True)

        if keyword_kpis:
            col4, col5, col6 = st.columns(3, gap="medium")

            with col4:
                week_trend_color = (
                    "positive" if keyword_kpis["week_trend"] >= 0 else "negative"
                )
                render_metric_card(
                    title="Posts This Week",
                    value=f"{keyword_kpis['posts_this_week']:,}",
                    delta=(
                        f"{'↑' if keyword_kpis['week_trend'] >= 0 else '↓'} {keyword_kpis['week_trend']:+.1f}% vs last week"
                        if keyword_kpis["week_trend"] != 0
                        else "→ No change"
                    ),
                    delta_color=week_trend_color,
                    help_text=f"Weekly posts for {selected_keyword} with trend analysis",
                )

            with col5:
                momentum = keyword_kpis["sentiment_momentum"]
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
                        f"{keyword_kpis['momentum_change']:+.1f}% change"
                        if keyword_kpis["momentum_change"] != 0
                        else "No change"
                    ),
                    delta_color=momentum_colors.get(momentum, "neutral"),
                    help_text="Recent sentiment trend direction for this keyword",
                )

            with col6:
                rank = keyword_kpis["keyword_rank"]
                total = keyword_kpis["total_keywords"]
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
                    value=f"{keyword_kpis['daily_average']:.1f}",
                    delta="posts per day",
                    delta_color="neutral",
                    help_text=f"Average daily posts for {selected_keyword} (30-day avg)",
                )

            with col8:
                peak_date = keyword_kpis["peak_date"]
                peak_value = (
                    float(keyword_kpis["peak_sentiment"])
                    if keyword_kpis["peak_sentiment"]
                    else 0.0
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
        else:
            col4, col5, col6 = st.columns(3, gap="medium")

            with col4:
                render_metric_card(
                    title="Negative Sentiment",
                    value=f"{kpi_data['negative_percentage']:.1f}%",
                    delta=(
                        f"{'↓' if kpi_data['negative_trend'] <= 0 else '↑'} {kpi_data['negative_trend']:+.1f}% vs yesterday"
                        if kpi_data["negative_trend"] != 0
                        else "→ No change"
                    ),
                    delta_color=(
                        "positive" if kpi_data["negative_trend"] <= 0 else "negative"
                    ),
                    help_text="Percentage of posts with negative sentiment",
                )

            with col5:
                render_metric_card(
                    title="Neutral Sentiment",
                    value=f"{kpi_data['neutral_percentage']:.1f}%",
                    delta=(
                        f"{'↑' if kpi_data['neutral_trend'] >= 0 else '↓'} {kpi_data['neutral_trend']:+.1f}% vs yesterday"
                        if kpi_data["neutral_trend"] != 0
                        else "→ No change"
                    ),
                    delta_color="neutral",
                    help_text="Percentage of posts with neutral sentiment",
                )

            with col6:
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


def render_chart_section():
    """Render the charts section."""
    render_sentiment_over_time_chart()

    col1, col2 = st.columns(2, gap="large")

    with col1:
        render_sentiment_distribution_chart()

    with col2:
        render_volume_analysis_chart()


def main():
    """Main dashboard application."""
    configure_page()
    apply_all_styles()

    render_page_title()

    selected_keywords = render_keyword_selector()

    if selected_keywords:
        st.session_state["selected_keywords"] = selected_keywords
    else:
        st.session_state["selected_keywords"] = None

    render_kpi_section(selected_keywords)

    render_insights_section(selected_keywords)

    render_chart_section()


if __name__ == "__main__":
    main()
