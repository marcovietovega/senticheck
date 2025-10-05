#!/usr/bin/env python3

import streamlit as st
import base64
from pathlib import Path


from charts import (
    render_sentiment_over_time_chart,
    render_sentiment_distribution_chart,
    render_volume_analysis_chart,
)
from styles import apply_all_styles
from data_service_api import get_dashboard_data_service
from utils import render_metric_card
from components import (
    render_sidebar_controls,
    update_session_state_from_sidebar,
    render_wordcloud_section,
)


def load_svg_icon():
    """Load SVG icon and convert to base64 for Streamlit page icon."""
    try:
        icon_path = Path(__file__).parent / "assets" / "icons" / "logo.svg"
        if icon_path.exists():
            with open(icon_path, "r", encoding="utf-8") as f:
                svg_content = f.read()
            svg_base64 = base64.b64encode(svg_content.encode()).decode()
            return f"data:image/svg+xml;base64,{svg_base64}"
    except Exception:
        pass
    return None


def load_svg_for_display():
    """Load SVG icon for direct display in the dashboard."""
    try:
        icon_path = Path(__file__).parent / "assets" / "icons" / "logo.svg"
        if icon_path.exists():
            with open(icon_path, "r", encoding="utf-8") as f:
                return f.read()
    except Exception:
        pass
    return None


def configure_page():
    """Configure Streamlit page settings."""
    icon_data = load_svg_icon()
    st.set_page_config(
        page_title="SentiCheck",
        page_icon=icon_data,
        layout="wide",
        initial_sidebar_state="expanded",
        menu_items={"About": "SentiCheck - Sentiment analysis dashboard"},
    )


def render_page_title():
    """Render the main page title with SVG icon support."""
    svg_icon = load_svg_for_display()
    styled_svg = svg_icon.replace(
        "<svg",
        '<svg style="width: 80px; height: 80px; vertical-align: middle"',
    )

    st.markdown(
        f"""
        <div class="page-title-container">
            <h1 class="page-title" style="display: flex; align-items: center; justify-content: center;">
                {styled_svg}SentiCheck
            </h1>
            <div class="page-subtitle">
                Sentiment insights from social media posts
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_kpi_section(selected_keyword: str, days: int = 30):
    """Render the KPI section with keyword-specific metrics."""
    try:
        st.header(f"Key Performance Indicators - {selected_keyword}")

        st.markdown("---")

        data_service = get_dashboard_data_service()
        kpi_data = data_service.get_kpi_metrics(keyword=selected_keyword, days=days)

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
                help_text="Posts classified as having a positive tone",
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
                help_text="How confident the model is about these sentiment scores (higher is better)",
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
                help_text="Total posts collected this week",
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
                help_text="Whether sentiment is getting more positive or negative over the past 3 days",
            )

        with col6:
            rank = kpi_data["keyword_rank"]
            total = kpi_data["total_keywords"]
            render_metric_card(
                title="Keyword Rank",
                value=f"#{rank} of {total}",
                delta=f"By post volume",
                delta_color="neutral",
                help_text="Ranking by volume compared to other tracked keywords",
            )

        st.markdown('<div class="spacing-top-2xl"></div>', unsafe_allow_html=True)

        col7, col8, col9 = st.columns(3, gap="medium")

        with col7:
            render_metric_card(
                title="Daily Average",
                value=f"{kpi_data['daily_average']:.1f}",
                delta="posts per day",
                delta_color="neutral",
                help_text="Typical number of posts per day over the last 30 days",
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


def render_chart_section(selected_keyword: str, days: int = 30):
    """Render the charts section."""

    render_sentiment_over_time_chart(selected_keyword, days=days)

    col1, col2 = st.columns(2, gap="large")

    with col1:
        render_sentiment_distribution_chart(selected_keyword, days=days)

    with col2:
        render_volume_analysis_chart(selected_keyword, days=days)

    render_wordcloud_section(selected_keyword, days=days)


def main():
    """Main dashboard application."""
    configure_page()
    apply_all_styles()

    render_page_title()

    sidebar_data = render_sidebar_controls()
    update_session_state_from_sidebar(sidebar_data)

    selected_keyword = sidebar_data["selected_keyword"]
    days = sidebar_data["time_range_days"]

    render_kpi_section(selected_keyword, days)

    render_chart_section(selected_keyword, days)


if __name__ == "__main__":
    main()
