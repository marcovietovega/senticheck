#!/usr/bin/env python3

import streamlit as st


from charts.chart_templates import create_chart_template


def render_volume_analysis_chart(selected_keyword: str, days: int = 30):
    """Render the volume analysis chart using template system."""
    st.markdown("## Volume Analysis")
    st.markdown("---")

    st.markdown(f"Daily posting volume for {selected_keyword}")

    try:

        chart_template = create_chart_template(selected_keyword)
        fig = chart_template.render_volume_analysis(days)

        st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.error(f"Error loading volume chart: {e}")
