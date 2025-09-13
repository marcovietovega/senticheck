from .base import COLORS


def get_dynamic_color(value, thresholds=None):
    """
    Get color based on value and thresholds.

    Args:
        value: Numeric value to evaluate
        thresholds: Dict with 'good', 'warning', 'danger' threshold values

    Returns:
        Color hex code
    """
    if thresholds is None:
        thresholds = {"good": 70, "warning": 50}

    if value >= thresholds["good"]:
        return COLORS["green_600"]
    elif value >= thresholds["warning"]:
        return COLORS["orange_600"]
    else:
        return COLORS["red_600"]


def get_delta_color_class(delta_value):
    """
    Get CSS class name for delta styling based on value.

    Args:
        delta_value: Numeric delta value

    Returns:
        CSS class name string
    """
    if delta_value > 0:
        return "metric-delta-positive"
    elif delta_value < 0:
        return "metric-delta-negative"
    else:
        return "metric-delta-neutral"


def get_rank_style(rank, total_items):
    """
    Get styling info for rank display.

    Args:
        rank: Current rank (1-based)
        total_items: Total number of items

    Returns:
        Dict with icon, context, and color
    """
    if rank <= 3 and total_items >= 5:
        return {"icon": "🏆", "context": "Top performer", "color": COLORS["green_600"]}
    elif rank <= total_items * 0.3:
        return {"icon": "🥇", "context": "High volume", "color": COLORS["blue_600"]}
    elif rank <= total_items * 0.7:
        return {"icon": "📊", "context": "Moderate volume", "color": COLORS["gray_500"]}
    else:
        return {"icon": "📉", "context": "Low volume", "color": COLORS["orange_600"]}


def get_confidence_quality(confidence_score):
    """
    Get quality rating info based on confidence score.

    Args:
        confidence_score: Confidence percentage (0-100)

    Returns:
        Dict with rating, icon, and color
    """
    if confidence_score >= 85:
        return {"rating": "excellent", "icon": "🎯", "color": COLORS["green_600"]}
    elif confidence_score >= 75:
        return {"rating": "good", "icon": "⭐", "color": COLORS["blue_600"]}
    elif confidence_score >= 65:
        return {"rating": "fair", "icon": "⚠️", "color": COLORS["orange_600"]}
    else:
        return {"rating": "poor", "icon": "⚡", "color": COLORS["red_600"]}


def format_time_12h(time_str):
    """
    Convert 24h time string to 12h format.

    Args:
        time_str: Time string in HH:MM format

    Returns:
        Formatted time string in 12h format
    """
    if not time_str or time_str == "N/A":
        return "N/A"
    try:
        hour = int(time_str.split(":")[0])
        if hour == 0:
            return "12 AM"
        elif hour < 12:
            return f"{hour} AM"
        elif hour == 12:
            return "12 PM"
        else:
            return f"{hour - 12} PM"
    except:
        return str(time_str)


def format_date_short(date_str):
    """
    Format date string to short format.

    Args:
        date_str: Date string in YYYY-MM-DD format

    Returns:
        Formatted date string
    """
    if not date_str or date_str == "N/A":
        return "N/A"
    try:
        from datetime import datetime

        date_obj = datetime.strptime(str(date_str), "%Y-%m-%d")
        return date_obj.strftime("%b %d")
    except:
        return str(date_str)
