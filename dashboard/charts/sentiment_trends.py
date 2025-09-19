#!/usr/bin/env python3

import streamlit as st


from charts.chart_templates import create_chart_template


def render_sentiment_over_time_chart(keyword: str, days: int = 30):
    """Render the sentiment over time."""
    st.markdown("## Sentiment Analysis")
    st.markdown("---")

    st.markdown("Sentiment trends and patterns for the selected keyword")

    try:

        chart_template = create_chart_template(keyword)
        fig = chart_template.render_sentiment_trends(days=days)

        st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.error(f"Error loading chart data: {e}")
