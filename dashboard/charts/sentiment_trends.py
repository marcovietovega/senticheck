#!/usr/bin/env python3


import streamlit as st
import plotly.graph_objects as go
import sys
from pathlib import Path

# Add parent directory to path for imports
parent_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(parent_dir))

from dashboard.data_service import get_dashboard_data_service


def render_sentiment_over_time_chart():
    """Render the sentiment over time."""

    st.markdown("## Sentiment Analysis")
    st.markdown("---")
    st.markdown("AI discussion trends and patterns over time")

    col1, col2 = st.columns([3, 1])

    with col2:
        time_range = st.selectbox(
            "Time range:",
            options=["7 days", "15 days", "30 days"],
            index=0,
            key="time_range_selector",
        )

    try:
        data_service = get_dashboard_data_service()

        days_map = {"7 days": 7, "15 days": 15, "30 days": 30}
        days = days_map[time_range]

        chart_data = data_service.get_sentiment_over_time(days=days)

        fig = go.Figure()

        colors = {
            "Positive": "#10b981",
            "Negative": "#ef4444",
            "Neutral": "#6b7280",
        }

        for sentiment in ["Positive", "Negative", "Neutral"]:
            if sentiment.lower() in chart_data.columns:
                fig.add_trace(
                    go.Scatter(
                        x=chart_data["date"],
                        y=chart_data[sentiment.lower()],
                        mode="lines+markers",
                        name=sentiment,
                        line=dict(color=colors[sentiment], width=3),
                        marker=dict(size=6, color=colors[sentiment]),
                        hovertemplate=(
                            f"<b>{sentiment} Sentiment</b><br>"
                            + "Date: %{x|%b %d, %Y}<br>"
                            + "Posts: %{y:,}<br>"
                            + "<extra></extra>"
                        ),
                    )
                )

        fig.update_layout(
            title=f"AI Sentiment Trends - Last {time_range}",
            xaxis_title="Date",
            yaxis_title="Number of Posts",
            height=450,
            margin=dict(b=130, l=40, r=40, t=60),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=-0.4,
                xanchor="center",
                x=0.5,
                bgcolor="rgba(255,255,255,0.9)",
            ),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#1f2937"),
            hoverlabel=dict(
                bgcolor="rgba(255, 255, 255, 0.95)",
                font_color="#1f2937",
                font_size=12,
                bordercolor="#e5e7eb",
            ),
        )

        fig.update_xaxes(
            gridcolor="rgba(0,0,0,0.1)",
            linecolor="rgba(0,0,0,0.2)",
            tickfont=dict(color="#1f2937"),
        )
        fig.update_yaxes(
            gridcolor="rgba(0,0,0,0.1)",
            linecolor="rgba(0,0,0,0.2)",
            tickfont=dict(color="#1f2937"),
        )

        st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.error(f"Error loading chart data: {e}")
