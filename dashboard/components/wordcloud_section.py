"""
Word cloud section component for the SentiCheck dashboard.
Provides word frequency visualization with sentiment-based coloring for single keywords.
"""

import streamlit as st
from typing import Optional, List
import logging

from dashboard.charts.chart_templates import create_chart_template
from dashboard.data_service import get_dashboard_data_service

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from dashboard.app import render_metric_card

logger = logging.getLogger(__name__)


def render_wordcloud_section(selected_keywords: Optional[List[str]]):
    """
    Render the word cloud section (only for single keyword selection).

    Args:
        selected_keywords: List of selected keywords
    """
    if not selected_keywords or len(selected_keywords) != 1:
        return

    keyword = selected_keywords[0]

    try:
        chart_template = create_chart_template(selected_keywords)
        wordcloud_img = chart_template.render_wordcloud(days=30)

        if wordcloud_img:
            st.markdown(f"### 📝 Word Analysis - {keyword}")
            st.markdown(f"Most frequent words in **{keyword}** discussions")
            st.image(wordcloud_img, use_container_width=True)
            render_wordcloud_stats(keyword)
        else:
            st.markdown(f"### 📝 Word Analysis - {keyword}")
            st.info(
                "📊 No text data available for word cloud generation. Please check if there are posts for this keyword."
            )

    except Exception as e:
        logger.error(f"Error rendering word cloud section: {e}")
        st.error(f"Error loading word cloud: {str(e)}")


def render_wordcloud_stats(keyword: str):
    """
    Render statistics and metadata for the word cloud.

    Args:
        keyword: The keyword for which to show stats
    """
    try:
        data_service = get_dashboard_data_service()
        wc_data = data_service.get_wordcloud_data([keyword], days=30)

        if wc_data and wc_data.get("total_posts", 0) > 0:
            col1, col2 = st.columns(2)

            with col1:
                render_metric_card(
                    title="📊 Total Posts",
                    value=f"{wc_data['total_posts']:,}",
                    help_text="Posts analyzed from the last 30 days for this keyword's word cloud generation",
                )

            with col2:
                render_metric_card(
                    title="📅 Time Range",
                    value="Last 30 days",
                    help_text="Fixed 30-day analysis window for consistent word frequency analysis",
                )

            st.markdown("---")
            st.markdown("**Color Legend:**")

            col1, col2, col3 = st.columns(3)
            with col1:
                st.markdown(
                    '<span style="color: #2E7D32; font-weight: bold;">🟢 Green:</span> Positive sentiment words',
                    unsafe_allow_html=True,
                )
            with col2:
                st.markdown(
                    '<span style="color: #C62828; font-weight: bold;">🔴 Red:</span> Negative sentiment words',
                    unsafe_allow_html=True,
                )
            with col3:
                st.markdown(
                    '<span style="color: #1976D2; font-weight: bold;">🔵 Blue:</span> Neutral sentiment words',
                    unsafe_allow_html=True,
                )

            st.caption(
                "💡 Word size indicates frequency. Only the most commonly used words are displayed based on available space."
            )

        else:
            st.caption("No data available for statistics.")

    except Exception as e:
        logger.error(f"Error rendering word cloud stats: {e}")
        st.caption("Unable to load word cloud statistics.")
