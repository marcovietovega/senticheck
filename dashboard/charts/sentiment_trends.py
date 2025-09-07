#!/usr/bin/env python3

import streamlit as st
import sys
from pathlib import Path

parent_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(parent_dir))

from dashboard.charts.chart_templates import create_chart_template


def render_sentiment_over_time_chart():
    """Render the sentiment over time using template system."""
    st.markdown("## Sentiment Analysis")
    st.markdown("---")
    
    selected_keywords = st.session_state.get("selected_keywords", None)
    
    if selected_keywords:
        keyword_text = ", ".join(selected_keywords) if len(selected_keywords) <= 3 else f"{len(selected_keywords)} keywords"
        st.markdown(f"Sentiment trends and patterns for {keyword_text}")
    else:
        st.markdown("Sentiment trends and patterns for all keywords")

    col1, col2 = st.columns([3, 1])

    with col2:
        time_range = st.selectbox(
            "Time range:",
            options=["7 days", "15 days", "30 days"],
            index=0,
            key="time_range_selector",
        )

    try:
        days_map = {"7 days": 7, "15 days": 15, "30 days": 30}
        days = days_map[time_range]

        chart_template = create_chart_template(selected_keywords)
        fig = chart_template.render_sentiment_trends(days=days)

        st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.error(f"Error loading chart data: {e}")
