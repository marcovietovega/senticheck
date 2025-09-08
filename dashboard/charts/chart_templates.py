#!/usr/bin/env python3
"""
Chart Template Framework for SentiCheck Dashboard

This module provides reusable chart templates that work with individual keywords.
All charts maintain consistent styling while displaying keyword-specific data.
"""

import plotly.graph_objects as go
import sys
from pathlib import Path
from typing import List, Optional

parent_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(parent_dir))

from dashboard.data_service import get_dashboard_data_service


class ChartTemplate:
    """Template class for creating consistent charts across different keywords."""

    def __init__(self, selected_keywords: Optional[List[str]] = None):
        """
        Initialize chart template with selected keywords.

        Args:
            selected_keywords: List of keywords to filter data by, None for all keywords
        """
        self.selected_keywords = selected_keywords
        self.data_service = get_dashboard_data_service()

        self.colors = {
            "positive": "#10b981",
            "negative": "#ef4444",
            "neutral": "#6b7280",
            "primary": "#3b82f6",
            "secondary": "#6b7280",
        }

        self.layout_defaults = {
            "height": 450,
            "margin": dict(b=130, l=40, r=40, t=60),
            "plot_bgcolor": "rgba(0,0,0,0)",
            "paper_bgcolor": "rgba(0,0,0,0)",
            "font": dict(color="#1f2937"),
            "hoverlabel": dict(
                bgcolor="rgba(255, 255, 255, 0.95)",
                font_color="#1f2937",
                font_size=12,
                bordercolor="#e5e7eb",
            ),
        }

    def _get_keyword_text(self) -> str:
        """Get display text for selected keywords."""
        if not self.selected_keywords:
            return "All Keywords"
        elif len(self.selected_keywords) == 1:
            return self.selected_keywords[0]
        elif len(self.selected_keywords) <= 3:
            return ", ".join(self.selected_keywords)
        else:
            return f"{len(self.selected_keywords)} Keywords"

    def render_sentiment_trends(self, days: int = 7) -> go.Figure:
        """
        Template for sentiment trends over time.

        Args:
            days: Number of days of data to display

        Returns:
            Plotly Figure with sentiment trends
        """
        try:
            chart_data = self.data_service.get_sentiment_over_time(
                days=days, selected_keywords=self.selected_keywords
            )

            fig = go.Figure()

            for sentiment in ["positive", "negative", "neutral"]:
                if sentiment in chart_data.columns:
                    sentiment_name = sentiment.capitalize()

                    fig.add_trace(
                        go.Scatter(
                            x=chart_data["date"],
                            y=chart_data[sentiment],
                            mode="lines+markers",
                            name=sentiment_name,
                            line=dict(color=self.colors[sentiment], width=3),
                            marker=dict(size=6, color=self.colors[sentiment]),
                            hovertemplate=(
                                f"<b>{sentiment_name} Sentiment</b><br>"
                                + "Date: %{x|%b %d, %Y}<br>"
                                + "Posts: %{y:,}<br>"
                                + "<extra></extra>"
                            ),
                        )
                    )

            keyword_text = self._get_keyword_text()
            fig.update_layout(
                **self.layout_defaults,
                title=f"{keyword_text} Sentiment Trends - Last {days} days",
                xaxis_title="Date",
                yaxis_title="Number of Posts",
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=-0.4,
                    xanchor="center",
                    x=0.5,
                    bgcolor="rgba(255,255,255,0.9)",
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

            return fig

        except Exception as e:
            fig = go.Figure()
            fig.update_layout(
                title=f"Error loading sentiment trends: {str(e)}",
                **self.layout_defaults,
            )
            return fig

    def render_sentiment_distribution(self) -> go.Figure:
        """
        Template for sentiment distribution pie chart.

        Returns:
            Plotly Figure with sentiment distribution
        """
        try:
            distribution = self.data_service.get_sentiment_distribution(
                selected_keywords=self.selected_keywords
            )

            sentiments = list(distribution.keys())
            values = list(distribution.values())
            colors = [self.colors.get(s, self.colors["secondary"]) for s in sentiments]

            fig = go.Figure(
                data=[
                    go.Pie(
                        labels=[s.capitalize() for s in sentiments],
                        values=values,
                        hole=0.3,
                        marker=dict(colors=colors, line=dict(color="white", width=2)),
                        textinfo="label+percent",
                        textfont_size=12,
                        hovertemplate="<b>%{label}</b><br>"
                        + "Posts: %{value:,}<br>"
                        + "Percentage: %{percent}<br>"
                        + "<extra></extra>",
                    )
                ]
            )

            keyword_text = self._get_keyword_text()
            fig.update_layout(
                **self.layout_defaults,
                title=f"{keyword_text} Sentiment Distribution",
                showlegend=True,
                legend=dict(
                    orientation="v", yanchor="middle", y=0.5, xanchor="left", x=1.05
                ),
            )

            return fig

        except Exception as e:
            fig = go.Figure()
            fig.update_layout(
                title=f"Error loading sentiment distribution: {str(e)}",
                **self.layout_defaults,
            )
            return fig

    def render_volume_analysis(self, days: int = 7) -> go.Figure:
        """
        Template for volume analysis over time.

        Args:
            days: Number of days of data to display

        Returns:
            Plotly Figure with volume analysis
        """
        try:
            chart_data = self.data_service.get_sentiment_over_time(days=days, selected_keywords=self.selected_keywords)

            chart_data["total_volume"] = chart_data[
                ["positive", "negative", "neutral"]
            ].sum(axis=1)

            fig = go.Figure()

            fig.add_trace(
                go.Bar(
                    x=chart_data["date"],
                    y=chart_data["total_volume"],
                    name="Daily Volume",
                    marker_color=self.colors["primary"],
                    hovertemplate=(
                        "<b>Daily Volume</b><br>"
                        + "Date: %{x|%b %d, %Y}<br>"
                        + "Total Posts: %{y:,}<br>"
                        + "<extra></extra>"
                    ),
                )
            )

            keyword_text = self._get_keyword_text()
            fig.update_layout(
                **self.layout_defaults,
                title=f"{keyword_text} Daily Volume - Last {days} days",
                xaxis_title="Date",
                yaxis_title="Number of Posts",
                showlegend=False,
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

            return fig

        except Exception as e:
            fig = go.Figure()
            fig.update_layout(
                title=f"Error loading volume analysis: {str(e)}", **self.layout_defaults
            )
            return fig


def create_chart_template(
    selected_keywords: Optional[List[str]] = None,
) -> ChartTemplate:
    """
    Factory function to create a chart template instance.

    Args:
        selected_keywords: List of keywords to filter charts by

    Returns:
        ChartTemplate instance configured for the specified keywords
    """
    return ChartTemplate(selected_keywords)
