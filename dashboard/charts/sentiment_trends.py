#!/usr/bin/env python3

import streamlit as st
import sys
from pathlib import Path

parent_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(parent_dir))

from dashboard.charts.chart_templates import create_chart_template


def render_sentiment_over_time_chart(keyword: str):
    """Render the sentiment over time."""
    st.markdown("## Sentiment Analysis")
    st.markdown("---")

    st.markdown("Sentiment trends and patterns for the selected keyword")

    try:
        days = st.session_state.get("time_range_days", 7)

        chart_template = create_chart_template(keyword)
        fig = chart_template.render_sentiment_trends(days=days)

        st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.error(f"Error loading chart data: {e}")
