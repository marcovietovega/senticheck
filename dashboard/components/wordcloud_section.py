"""
Word cloud section component for the SentiCheck dashboard.
Provides word frequency visualization with sentiment-based coloring.
"""

import streamlit as st
import logging

from dashboard.charts.chart_templates import create_chart_template
from dashboard.data_service_api import get_dashboard_data_service
from dashboard.utils import render_metric_card

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

logger = logging.getLogger(__name__)


def render_wordcloud_section(selected_keyword: str, days: int = 30):
    """
    Render the word cloud section.

    Args:
        selected_keyword: The selected keyword
    """

    try:
        chart_template = create_chart_template(selected_keyword)
        wordcloud_img = chart_template.render_wordcloud(days)

        if wordcloud_img:
            st.markdown(f"### Word Analysis - {selected_keyword}")
            st.markdown(f"Most frequent words in **{selected_keyword}** discussions")
            st.image(wordcloud_img, use_container_width=True)
            render_wordcloud_stats(selected_keyword, days)
        else:
            st.markdown(f"### Word Analysis - {selected_keyword}")
            st.info(
                "No text data available for word cloud generation. Please check if there are posts for this keyword."
            )

    except Exception as e:
        logger.error(f"Error rendering word cloud section: {e}")
        st.error(f"Error loading word cloud: {str(e)}")


def render_wordcloud_stats(keyword: str, days: int = 30):
    """
    Render statistical insights for the word cloud with sentiment-focused metrics.

    Args:
        keyword: The selected keyword
    """

    try:
        data_service = get_dashboard_data_service()
        wordcloud_stats = data_service.get_wordcloud_stats(keyword, days)

        if not wordcloud_stats:
            st.info("No statistical data available for word analysis.")
            return

        st.markdown("#### Sentiment Analysis Insights")

        st.markdown(
            """
        <div style="margin-bottom: 20px; padding: 10px; background-color: #f8f9fa; border-radius: 5px;">
            <small style="color: #666;">
                <span style="color: #28a745; font-weight: bold;">🟢 Green = Positive</span> | 
                <span style="color: #007bff; font-weight: bold;">🔵 Blue = Neutral</span> | 
                <span style="color: #dc3545; font-weight: bold;">🔴 Red = Negative</span>
            </small>
        </div>
        """,
            unsafe_allow_html=True,
        )

        col1, col2, col3 = st.columns(3, gap="medium")

        with col1:
            positive_word = wordcloud_stats["most_positive_word"]
            positive_score = wordcloud_stats["positive_sentiment_score"]
            positive_freq = wordcloud_stats["positive_frequency"]

            render_metric_card(
                title="🟢 Most Positive Word",
                value=f"{positive_word}",
                delta=(
                    f"Score: {positive_score} ({positive_freq}x)"
                    if positive_word != "None"
                    else "No positive words found"
                ),
                help_text="Word with strongest positive sentiment in discussions",
            )

        with col2:
            neutral_word = wordcloud_stats["most_neutral_word"]
            neutral_score = wordcloud_stats["neutral_sentiment_score"]
            neutral_freq = wordcloud_stats["neutral_frequency"]

            render_metric_card(
                title="🔵 Most Neutral Word",
                value=f"{neutral_word}",
                delta=(
                    f"Score: {neutral_score} ({neutral_freq}x)"
                    if neutral_word != "None"
                    else "No neutral words found"
                ),
                help_text="Word closest to neutral sentiment (0.5) in discussions",
            )

        with col3:
            negative_word = wordcloud_stats["most_negative_word"]
            negative_score = wordcloud_stats["negative_sentiment_score"]
            negative_freq = wordcloud_stats["negative_frequency"]

            render_metric_card(
                title="🔴 Most Negative Word",
                value=f"{negative_word}",
                delta=(
                    f"Score: {negative_score} ({negative_freq}x)"
                    if negative_word != "None"
                    else "No negative words found"
                ),
                help_text="Word with strongest negative sentiment in discussions",
            )

    except Exception as e:
        logger.error(f"Error rendering word cloud stats: {e}")
        st.error(f"Error loading word analysis stats: {str(e)}")
