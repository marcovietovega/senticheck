#!/usr/bin/env python3
"""
SentiCheck Dashboard Module

A modern, minimal Streamlit dashboard for sentiment analysis insights.
Built following the SentiCheck UI Style Guide for clean, scannable visualizations.
"""

__version__ = "1.0.0"
__author__ = "SentiCheck Team"

from .config import DASHBOARD_CONFIG, SENTIMENT_LABELS
from .data_service_api import get_dashboard_data_service

__all__ = [
    "DASHBOARD_CONFIG",
    "SENTIMENT_LABELS", 
    "get_dashboard_data_service",
]
