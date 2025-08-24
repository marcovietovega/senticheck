#!/usr/bin/env python3
"""
Chart utilities for SentiCheck dashboard.

This module provides chart creation and styling functions following
the SentiCheck UI Style Guide for clean, minimal visualizations.
"""

import plotly.graph_objects as go
import plotly.express as px
from typing import Dict, List, Any, Optional
import pandas as pd

try:
    from dashboard.config import DASHBOARD_CONFIG, SENTIMENT_LABELS
except ImportError:
    # Fallback config if import fails
    DASHBOARD_CONFIG = {
        "colors": {
            "background": "#ffffff",
            "positive": "#10b981",
            "negative": "#ef4444",
            "neutral": "#6b7280",
            "accent": "#3b82f6",
        }
    }
    SENTIMENT_LABELS = {
        "positive": {"color": "#10b981"},
        "negative": {"color": "#ef4444"},
        "neutral": {"color": "#6b7280"},
    }


def apply_style_guide_layout(fig: go.Figure, title: str = None) -> go.Figure:
    """
    Apply SentiCheck style guide formatting to a Plotly figure.

    Args:
        fig: Plotly figure to style
        title: Optional chart title

    Returns:
        Styled Plotly figure
    """
    fig.update_layout(
        # Background styling - white as per style guide
        paper_bgcolor=DASHBOARD_CONFIG["colors"]["background"],
        plot_bgcolor=DASHBOARD_CONFIG["colors"]["background"],
        # Title styling
        title=(
            dict(
                text=title,
                font=dict(size=18, color="#1f2937"),
                x=0,  # Left align titles
                xanchor="left",
            )
            if title
            else None
        ),
        # Font styling
        font=dict(
            family="Inter, -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif",
            size=14,
            color="#374151",
        ),
        # Margins for proper spacing
        margin=dict(l=20, r=20, t=50 if title else 20, b=20),
        # Legend styling
        legend=dict(
            bgcolor="rgba(255,255,255,0.8)",
            bordercolor="#e5e7eb",
            borderwidth=1,
            font=dict(size=12),
        ),
        # Grid and axis styling
        xaxis=dict(
            gridcolor="#f3f4f6",
            gridwidth=1,
            zeroline=False,
            tickfont=dict(size=12, color="#6b7280"),
        ),
        yaxis=dict(
            gridcolor="#f3f4f6",
            gridwidth=1,
            zeroline=False,
            tickfont=dict(size=12, color="#6b7280"),
        ),
    )

    return fig


def create_sentiment_distribution_chart(sentiment_data: Dict[str, int]) -> go.Figure:
    """
    Create a pie chart for sentiment distribution.

    Args:
        sentiment_data: Dict with sentiment labels as keys and counts as values

    Returns:
        Plotly pie chart figure
    """
    labels = []
    values = []
    colors = []

    for sentiment, count in sentiment_data.items():
        if count > 0:  # Only include sentiments with data
            labels.append(SENTIMENT_LABELS[sentiment].get("label", sentiment.title()))
            values.append(count)
            colors.append(SENTIMENT_LABELS[sentiment]["color"])

    fig = go.Figure(
        data=[
            go.Pie(
                labels=labels,
                values=values,
                marker=dict(colors=colors),
                textinfo="label+percent",
                textfont=dict(size=12),
                hovertemplate="<b>%{label}</b><br>Count: %{value}<br>Percentage: %{percent}<extra></extra>",
            )
        ]
    )

    # Apply style guide
    fig = apply_style_guide_layout(fig, "Sentiment Distribution")

    # Remove legend since labels are on the chart
    fig.update_layout(showlegend=False)

    return fig


def create_sentiment_timeline_chart(timeline_data: pd.DataFrame) -> go.Figure:
    """
    Create a line chart showing sentiment trends over time.

    Args:
        timeline_data: DataFrame with date and sentiment columns

    Returns:
        Plotly line chart figure
    """
    fig = go.Figure()

    # Add lines for each sentiment
    for sentiment in ["positive", "negative", "neutral"]:
        if sentiment in timeline_data.columns:
            fig.add_trace(
                go.Scatter(
                    x=timeline_data["date"],
                    y=timeline_data[sentiment],
                    mode="lines+markers",
                    name=SENTIMENT_LABELS[sentiment].get("label", sentiment.title()),
                    line=dict(color=SENTIMENT_LABELS[sentiment]["color"], width=2),
                    marker=dict(size=6),
                    hovertemplate=f"<b>{sentiment.title()}</b><br>Date: %{{x}}<br>Count: %{{y}}<extra></extra>",
                )
            )

    # Apply style guide
    fig = apply_style_guide_layout(fig, "Sentiment Trends Over Time")

    # Update axes
    fig.update_xaxes(title="Date")
    fig.update_yaxes(title="Number of Posts")

    return fig


def create_confidence_distribution_chart(confidence_data: List[float]) -> go.Figure:
    """
    Create a histogram showing confidence score distribution.

    Args:
        confidence_data: List of confidence scores (0-1 range)

    Returns:
        Plotly histogram figure
    """
    # Convert to percentage for display
    confidence_pct = [score * 100 for score in confidence_data]

    fig = go.Figure(
        data=[
            go.Histogram(
                x=confidence_pct,
                nbinsx=20,
                marker_color=DASHBOARD_CONFIG["colors"]["accent"],
                opacity=0.7,
                hovertemplate="<b>Confidence Range</b><br>%{x}<br>Count: %{y}<extra></extra>",
            )
        ]
    )

    # Apply style guide
    fig = apply_style_guide_layout(fig, "Confidence Score Distribution")

    # Update axes
    fig.update_xaxes(title="Confidence Score (%)")
    fig.update_yaxes(title="Number of Posts")

    return fig


def create_simple_bar_chart(
    data: Dict[str, float], title: str, x_label: str = "", y_label: str = ""
) -> go.Figure:
    """
    Create a simple bar chart with style guide formatting.

    Args:
        data: Dict with categories as keys and values as values
        title: Chart title
        x_label: X-axis label
        y_label: Y-axis label

    Returns:
        Plotly bar chart figure
    """
    fig = go.Figure(
        data=[
            go.Bar(
                x=list(data.keys()),
                y=list(data.values()),
                marker_color=DASHBOARD_CONFIG["colors"]["accent"],
                opacity=0.8,
                hovertemplate="<b>%{x}</b><br>Value: %{y}<extra></extra>",
            )
        ]
    )

    # Apply style guide
    fig = apply_style_guide_layout(fig, title)

    # Update axes
    fig.update_xaxes(title=x_label)
    fig.update_yaxes(title=y_label)

    return fig


def create_metric_sparkline(values: List[float], color: str = None) -> go.Figure:
    """
    Create a small sparkline chart for metric trends.

    Args:
        values: List of values for the sparkline
        color: Optional color override

    Returns:
        Plotly sparkline figure
    """
    if not color:
        color = DASHBOARD_CONFIG["colors"]["accent"]

    fig = go.Figure(
        data=[
            go.Scatter(
                y=values,
                mode="lines",
                line=dict(color=color, width=2),
                showlegend=False,
                hovertemplate="Value: %{y}<extra></extra>",
            )
        ]
    )

    # Minimal sparkline styling
    fig.update_layout(
        paper_bgcolor=DASHBOARD_CONFIG["colors"]["background"],
        plot_bgcolor=DASHBOARD_CONFIG["colors"]["background"],
        margin=dict(l=0, r=0, t=0, b=0),
        height=60,
        xaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
        yaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
    )

    return fig


# Test function
def test_chart_utils():
    """Test the chart utility functions."""
    print("Testing Chart Utilities...")

    try:
        # Test sentiment distribution chart
        sentiment_data = {"positive": 150, "negative": 50, "neutral": 100}
        fig1 = create_sentiment_distribution_chart(sentiment_data)
        print("✅ Sentiment distribution chart created")

        # Test simple bar chart
        test_data = {"Category A": 25, "Category B": 40, "Category C": 15}
        fig2 = create_simple_bar_chart(test_data, "Test Chart", "Categories", "Values")
        print("✅ Simple bar chart created")

        # Test sparkline
        test_values = [10, 15, 12, 18, 22, 19, 25]
        fig3 = create_metric_sparkline(test_values)
        print("✅ Sparkline chart created")

        print("\n🎉 All chart utility tests passed!")

    except Exception as e:
        print(f"❌ Chart utility test failed: {e}")


if __name__ == "__main__":
    test_chart_utils()
