#!/usr/bin/env python3
"""
Dashboard Configuration for SentiCheck

Minimal configuration containing only what's actually used.
"""

from typing import Dict, Any

# Dashboard Configuration - only used settings
DASHBOARD_CONFIG: Dict[str, Any] = {
    "colors": {
        "accent": "#3b82f6",
        "positive": "#10b981",
        "negative": "#ef4444", 
        "neutral": "#6b7280",
    },
    "refresh": {
        "cache_ttl": 300,  # Cache time-to-live in seconds (5 minutes)
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