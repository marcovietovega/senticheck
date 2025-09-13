"""
Insights section component for the SentiCheck dashboard.
Provides keyword-specific insights based on sentiment analysis data.
"""

import streamlit as st
from typing import Optional, List, Dict, Any
import logging

from dashboard.data_service import get_dashboard_data_service
from dashboard.styles import (
    get_rank_style,
    get_confidence_quality,
    format_time_12h,
    format_date_short,
)

logger = logging.getLogger(__name__)


def render_insights_section(selected_keywords: Optional[List[str]]):
    """
    Render the insights section based on selected keywords.

    Args:
        selected_keywords: List of selected keywords, None for all keywords
    """
    if not selected_keywords:
        render_platform_insights()
    elif len(selected_keywords) == 1:
        render_single_keyword_insights(selected_keywords[0])
    else:
        render_platform_insights()


def render_single_keyword_insights(keyword: str):
    """Render insights for a single keyword."""
    st.markdown("## Keyword Insights")
    st.markdown(f"Detailed analysis for **{keyword}** keyword")
    st.markdown("---")

    try:
        data_service = get_dashboard_data_service()

        time_range = st.session_state.get("time_range_selector", "7 days")
        days_map = {"7 days": 7, "15 days": 15, "30 days": 30}
        days = days_map.get(time_range, 7)

        insights_data = data_service.get_keyword_insights([keyword], days)

        if not insights_data:
            st.warning("No insights data available for this keyword.")
            return

        col1, col2 = st.columns(2, gap="large")

        with col1:
            render_trend_analysis_card(insights_data.get("trend_analysis", {}))
            render_volume_stats_card(insights_data.get("volume_stats", {}))

        with col2:
            render_performance_metrics_card(
                insights_data.get("performance_metrics", {})
            )
            render_activity_patterns_card(insights_data.get("activity_patterns", {}))

    except Exception as e:
        logger.error(f"Error rendering single keyword insights for {keyword}: {e}")
        st.error(f"Error loading insights for {keyword}: {e}")




def render_platform_insights():
    """Render platform-wide insights."""
    st.markdown("## Platform Insights")
    st.markdown("Overall platform performance and trends")
    st.markdown("---")

    try:
        data_service = get_dashboard_data_service()

        time_range = st.session_state.get("time_range_selector", "7 days")
        days_map = {"7 days": 7, "15 days": 15, "30 days": 30}
        days = days_map.get(time_range, 7)

        insights_data = data_service.get_keyword_insights(None, days)

        if not insights_data:
            st.warning("No platform insights data available.")
            return

        col1, col2 = st.columns(2, gap="large")

        with col1:
            render_platform_trend_card(insights_data.get("trend_analysis", {}))
            render_platform_volume_card(insights_data.get("volume_stats", {}))

        with col2:
            render_platform_performance_card(
                insights_data.get("performance_metrics", {})
            )
            render_platform_activity_card(insights_data.get("activity_patterns", {}))

    except Exception as e:
        logger.error(f"Error rendering platform insights: {e}")
        st.error("Error loading platform insights.")


def render_trend_analysis_card(trend_data: Dict[str, Any]):
    """Render trend analysis card for single keyword."""
    current_sentiment = trend_data.get("current_sentiment", 0)
    sentiment_change = trend_data.get("sentiment_change", 0)

    if sentiment_change > 2:
        trend_direction = "Improving"
    elif sentiment_change < -2:
        trend_direction = "Declining"
    else:
        trend_direction = "Stable"

    best_day = format_date_short(trend_data.get("best_day", "N/A"))
    worst_day = format_date_short(trend_data.get("worst_day", "N/A"))

    card_html = f"""
    <div class="metric-card metric-card-insights">
        <div class="metric-header">
            <span class="metric-title">Sentiment Score</span>
            <div class="metric-help-container"><span class="metric-help">?</span><div class="metric-tooltip">Shows how positive people feel about this keyword. Higher numbers mean more positive posts. The trend shows if sentiment is getting better or worse over time.</div></div>
        </div>
        <div class="metric-value">{current_sentiment:.1f}%</div>
        <div class="metric-delta">{sentiment_change:+.1f}% {trend_direction.lower()}</div>
        <div class="insight-card-details">
            <div class="insight-best-worst">📅 Best: {best_day} ({trend_data.get('best_sentiment', 0):.1f}%)</div>
            <div>📉 Worst: {worst_day} ({trend_data.get('worst_sentiment', 0):.1f}%)</div>
        </div>
    </div>
    """
    st.markdown(card_html, unsafe_allow_html=True)


def render_volume_stats_card(volume_data: Dict[str, Any]):
    """Render volume stats card for single keyword."""
    total_posts = volume_data.get("total_posts", 0)
    keyword_rank = volume_data.get("keyword_rank", 0)
    total_keywords = volume_data.get("total_keywords", 0)
    daily_average = volume_data.get("daily_average", 0)

    rank_info = get_rank_style(keyword_rank, total_keywords)
    rank_icon = rank_info["icon"]
    rank_context = rank_info["context"]
    rank_color = rank_info["color"]

    card_html = f"""
    <div class="metric-card metric-card-insights">
        <div class="metric-header">
            <span class="metric-title">Volume Statistics</span>
            <div class="metric-help-container"><span class="metric-help">?</span><div class="metric-tooltip">Shows how many posts mention this keyword and how it ranks compared to other keywords. Higher ranks mean more people are talking about this topic.</div></div>
        </div>
        <div class="metric-value">{total_posts:,} posts</div>
        <div class="metric-delta">Rank #{keyword_rank} of {total_keywords}</div>
        <div class="insight-card-details insight-rank-info" style="color: {rank_color};">
            {rank_icon} {rank_context} • {daily_average:.1f}/day
        </div>
    </div>
    """
    st.markdown(card_html, unsafe_allow_html=True)


def render_performance_metrics_card(performance_data: Dict[str, Any]):
    """Render performance metrics card for single keyword."""
    avg_confidence = performance_data.get("avg_confidence", 0)

    quality_info = get_confidence_quality(avg_confidence)
    quality_rating = quality_info["rating"]
    quality_icon = quality_info["icon"]
    quality_color = quality_info["color"]

    card_html = f"""
    <div class="metric-card metric-card-insights">
        <div class="metric-header">
            <span class="metric-title">Model Performance</span>
            <div class="metric-help-container"><span class="metric-help">?</span><div class="metric-tooltip">Shows how confident our AI is about the sentiment scores. Higher numbers mean the AI is more sure about whether posts are positive or negative.</div></div>
        </div>
        <div class="metric-value">{avg_confidence:.1f}%</div>
        <div class="metric-delta">Confidence Score</div>
        <div class="insight-card-details insight-rank-info" style="color: {quality_color};">
            {quality_icon} Quality: {quality_rating.title()}
        </div>
    </div>
    """
    st.markdown(card_html, unsafe_allow_html=True)


def render_activity_patterns_card(activity_data: Dict[str, Any]):
    """Render activity patterns card for single keyword."""
    peak_hours = activity_data.get("peak_hours", [])

    formatted_times = [format_time_12h(hour) for hour in peak_hours]
    peak_text = (
        ", ".join(formatted_times)
        if formatted_times and formatted_times != ["N/A"]
        else "No pattern"
    )

    card_html = f"""
    <div class="metric-card metric-card-insights">
        <div class="metric-header">
            <span class="metric-title">Activity Patterns</span>
            <div class="metric-help-container"><span class="metric-help">?</span><div class="metric-tooltip">Shows when people post most about this keyword. These are the best times to engage with your audience or monitor discussions.</div></div>
        </div>
        <div class="metric-value">{peak_text}</div>
        <div class="metric-delta">Peak posting times</div>
        <div class="insight-card-details">
            📊 Period: {activity_data.get('analysis_period', 'N/A')}
        </div>
    </div>
    """
    st.markdown(card_html, unsafe_allow_html=True)










def render_platform_trend_card(trend_data: Dict[str, Any]):
    """Render platform trend card."""
    card_html = f"""
    <div class="metric-card metric-card-insights">
        <div class="metric-header">
            <span class="metric-title">📈 Platform Sentiment</span>
            <div class="metric-help-container"><span class="metric-help">?</span><div class="metric-tooltip">Shows the overall mood across all keywords and posts. This gives you a bird's eye view of how people feel about all topics you're tracking.</div></div>
        </div>
        <div class="metric-value">{trend_data.get('platform_sentiment', 0):.1f}%</div>
        <div class="metric-delta">Overall positive sentiment</div>
        <div class="insight-card-details">
            🌐 Analysis: {trend_data.get('analysis_type', 'platform_wide').replace('_', ' ').title()}
        </div>
    </div>
    """
    st.markdown(card_html, unsafe_allow_html=True)


def render_platform_volume_card(volume_data: Dict[str, Any]):
    """Render platform volume card."""
    card_html = f"""
    <div class="metric-card metric-card-insights">
        <div class="metric-header">
            <span class="metric-title">📊 Platform Volume</span>
            <div class="metric-help-container"><span class="metric-help">?</span><div class="metric-tooltip">Shows the total number of posts across all keywords. This helps you understand your overall reach and which topics are generating the most discussion.</div></div>
        </div>
        <div class="metric-value">{volume_data.get('total_posts', 0):,} posts</div>
        <div class="metric-delta">Daily avg: {volume_data.get('daily_average', 0):.1f}</div>
        <div class="insight-card-details">
            🔥 Top: {', '.join(volume_data.get('top_keywords', [])[:2])}
        </div>
    </div>
    """
    st.markdown(card_html, unsafe_allow_html=True)


def render_platform_performance_card(performance_data: Dict[str, Any]):
    """Render platform performance card."""
    health_icon = "✅" if performance_data.get("platform_health") == "healthy" else "⚠️"

    card_html = f"""
    <div class="metric-card metric-card-insights">
        <div class="metric-header">
            <span class="metric-title">⭐ Platform Health</span>
            <div class="metric-help-container"><span class="metric-help">?</span><div class="metric-tooltip">Shows how well your sentiment analysis is working across all topics. A healthy status means the AI is confident in its predictions.</div></div>
        </div>
        <div class="metric-value">{performance_data.get('platform_health', 'unknown').title()}</div>
        <div class="metric-delta">{health_icon} Overall status</div>
        <div class="insight-card-details">
            🏷️ Keywords: {performance_data.get('total_keywords', 0)} active
        </div>
    </div>
    """
    st.markdown(card_html, unsafe_allow_html=True)


def render_platform_activity_card(activity_data: Dict[str, Any]):
    """Render platform activity card."""
    card_html = f"""
    <div class="metric-card metric-card-insights">
        <div class="metric-header">
            <span class="metric-title">⏰ Platform Activity</span>
            <div class="metric-help-container"><span class="metric-help">?</span><div class="metric-tooltip">Shows activity patterns across all your tracked keywords. This helps you understand when your audience is most active overall.</div></div>
        </div>
        <div class="metric-value">All Keywords</div>
        <div class="metric-delta">Platform-wide analysis</div>
        <div class="insight-card-details">
            📊 Period: {activity_data.get('analysis_period', 'N/A')}
        </div>
    </div>
    """
    st.markdown(card_html, unsafe_allow_html=True)
