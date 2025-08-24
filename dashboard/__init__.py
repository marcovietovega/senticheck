#!/usr/bin/env python3
"""
SentiCheck Dashboard Module

A modern, minimal Streamlit dashboard for sentiment analysis insights.
Built following the SentiCheck UI Style Guide for clean, scannable visualizations.
"""

__version__ = "1.0.0"
__author__ = "SentiCheck Team"

from .config import DASHBOARD_CONFIG, SENTIMENT_LABELS, KPI_METRICS
from .data_service import get_dashboard_data_service
from .chart_utils import create_sentiment_distribution_chart

__all__ = [
    "DASHBOARD_CONFIG",
    "SENTIMENT_LABELS",
    "KPI_METRICS",
    "get_dashboard_data_service",
    "create_sentiment_distribution_chart",
]
