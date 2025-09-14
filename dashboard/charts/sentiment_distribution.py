#!/usr/bin/env python3

import streamlit as st
import sys
from pathlib import Path

parent_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(parent_dir))

from dashboard.charts.chart_templates import create_chart_template


def render_sentiment_distribution_chart(selected_keyword: str):
    """Render the sentiment distribution chart"""
    st.markdown("## Sentiment Distribution")
    st.markdown("---")

    st.markdown(f"Distribution breakdown for {selected_keyword}")

    try:
        chart_template = create_chart_template(selected_keyword)
        fig = chart_template.render_sentiment_distribution()

        st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.error(f"Error loading distribution chart: {e}")
