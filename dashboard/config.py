#!/usr/bin/env python3
"""
Dashboard configuration following SentiCheck UI Style Guide.

This module contains all configuration constants and settings
for the Streamlit dashboard, ensuring consistency across components.
"""

from typing import Dict, Any


# Dashboard Configuration
DASHBOARD_CONFIG: Dict[str, Any] = {
    # Layout settings (following style guide)
    "layout": {
        "page_layout": "wide",  # Wide layout on desktop
        "max_columns": 3,  # Max 3 columns as per style guide
        "spacing_unit": 8,  # 8px base spacing unit
        "card_padding": 16,  # 16-20px padding inside cards
        "section_spacing": 32,  # 24-32px between sections
    },
    # Typography settings
    "typography": {
        "page_title_size": "24px",
        "section_title_size": "20px",
        "body_size": "16px",
        "caption_size": "14px",
        "line_height": "1.5",
    },
    # Color system (minimal, white-first)
    "colors": {
        "background": "#ffffff",  # Pure white background
        "secondary_bg": "#fafafa",  # Near-white for subtle contrast
        "text_primary": "#1f2937",  # Very dark gray for body text
        "text_secondary": "#6b7280",  # Lighter gray for metadata
        "accent": "#3b82f6",  # Single accent color (blue)
        "positive": "#10b981",  # Green for positive sentiment
        "negative": "#ef4444",  # Red for negative sentiment
        "neutral": "#6b7280",  # Gray for neutral sentiment
        "border": "#e5e7eb",  # Light gray for borders
    },
    # Chart settings
    "charts": {
        "background": "#ffffff",  # White chart backgrounds
        "gridlines": "#f3f4f6",  # Light gray gridlines
        "max_series": 4,  # Max 4 data series colors
        "default_height": 400,  # Default chart height
    },
    # KPI settings
    "kpi": {
        "decimal_places": 1,  # Number of decimal places for percentages
        "show_trends": True,  # Show trend comparisons
        "trend_period": "yesterday",  # Default comparison period
    },
    # Data refresh settings
    "refresh": {
        "auto_refresh_interval": 30,  # Auto-refresh interval in seconds
        "cache_ttl": 300,  # Cache time-to-live in seconds (5 minutes)
        "max_records": 10000,  # Maximum records to fetch for performance
    },
    # Page settings
    "page": {
        "title": "SentiCheck Dashboard",
        "icon": "📊",
        "description": "Modern sentiment analysis dashboard",
    },
    # Database settings
    "database": {
        "connection_timeout": 30,  # Connection timeout in seconds
        "retry_attempts": 3,  # Number of retry attempts
        "batch_size": 1000,  # Batch size for large queries
    },
}


# Sentiment label mappings for display
SENTIMENT_LABELS = {
    "positive": {
        "label": "Positive",
        "color": DASHBOARD_CONFIG["colors"]["positive"],
        "icon": "😊",
    },
    "negative": {
        "label": "Negative",
        "color": DASHBOARD_CONFIG["colors"]["negative"],
        "icon": "😞",
    },
    "neutral": {
        "label": "Neutral",
        "color": DASHBOARD_CONFIG["colors"]["neutral"],
        "icon": "😐",
    },
}


# KPI metric definitions
KPI_METRICS = {
    "total_posts": {
        "title": "Total Posts Analyzed",
        "description": "Total number of posts processed through sentiment analysis",
        "format": "count",
        "show_trend": True,
    },
    "positive_percentage": {
        "title": "Positive Sentiment",
        "description": "Percentage of posts with positive sentiment",
        "format": "percentage",
        "show_trend": True,
        "good_direction": "up",
    },
    "negative_percentage": {
        "title": "Negative Sentiment",
        "description": "Percentage of posts with negative sentiment",
        "format": "percentage",
        "show_trend": True,
        "good_direction": "down",  # Lower negative sentiment is better
    },
    "neutral_percentage": {
        "title": "Neutral Sentiment",
        "description": "Percentage of posts with neutral sentiment",
        "format": "percentage",
        "show_trend": True,
    },
    "avg_confidence": {
        "title": "Average Confidence",
        "description": "Average confidence score of sentiment predictions",
        "format": "percentage",
        "show_trend": True,
        "good_direction": "up",
    },
    "posts_today": {
        "title": "Posts Today",
        "description": "Number of posts processed today",
        "format": "count",
        "show_trend": True,
    },
}


# Chart configurations
CHART_CONFIGS = {
    "sentiment_distribution": {
        "type": "pie",
        "title": "Sentiment Distribution",
        "colors": [
            DASHBOARD_CONFIG["colors"]["positive"],
            DASHBOARD_CONFIG["colors"]["negative"],
            DASHBOARD_CONFIG["colors"]["neutral"],
        ],
    },
    "sentiment_over_time": {
        "type": "line",
        "title": "Sentiment Trends Over Time",
        "height": 400,
    },
    "confidence_distribution": {
        "type": "histogram",
        "title": "Confidence Score Distribution",
        "bins": 20,
    },
}


# Error messages
ERROR_MESSAGES = {
    "database_connection": "Unable to connect to the database. Please check your connection settings.",
    "no_data": "No data available. Please check your data source or try again later.",
    "loading_error": "Error loading data. Please refresh the page or contact support.",
    "invalid_date_range": "Invalid date range selected. Please choose a valid range.",
}


# Success messages
SUCCESS_MESSAGES = {
    "data_loaded": "Data loaded successfully",
    "database_connected": "Database connection established",
    "refresh_complete": "Dashboard refreshed successfully",
}


def get_color_palette(sentiment_type: str = None) -> str:
    """
    Get color for sentiment type or return the accent color.

    Args:
        sentiment_type: Type of sentiment ('positive', 'negative', 'neutral')

    Returns:
        str: Hex color code
    """
    if sentiment_type and sentiment_type in SENTIMENT_LABELS:
        return SENTIMENT_LABELS[sentiment_type]["color"]
    return DASHBOARD_CONFIG["colors"]["accent"]


def format_metric_value(value: float, format_type: str) -> str:
    """
    Format metric value according to its type.

    Args:
        value: The numeric value to format
        format_type: Type of formatting ('count', 'percentage', 'decimal')

    Returns:
        str: Formatted value string
    """
    if format_type == "count":
        if value >= 1000:
            return f"{value:,.0f}"
        return f"{value:.0f}"
    elif format_type == "percentage":
        return f"{value:.1f}%"
    elif format_type == "decimal":
        return f"{value:.2f}"
    else:
        return str(value)


def get_trend_color(trend_value: float, good_direction: str = "up") -> str:
    """
    Get color for trend indicator based on direction.

    Args:
        trend_value: The trend value (positive or negative)
        good_direction: Whether 'up' or 'down' is considered good

    Returns:
        str: Color name for Streamlit ('normal', 'inverse')
    """
    if good_direction == "up":
        return "normal" if trend_value >= 0 else "inverse"
    else:  # good_direction == "down"
        return "inverse" if trend_value >= 0 else "normal"
