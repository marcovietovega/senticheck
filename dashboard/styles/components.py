"""
Component-specific CSS styles for the SentiCheck dashboard.
Contains styles for metric cards, insight cards, tooltips, etc.
"""

from .base import COLORS, TYPOGRAPHY, SPACING, BORDER_RADIUS, SHADOWS, LAYOUT, TRANSITIONS


def get_metric_card_styles():
    """Get CSS styles for metric cards."""
    return f"""
        .metric-card {{
            background-color: {COLORS["white"]};
            border: 1px solid {COLORS["gray_200"]};
            border-radius: {BORDER_RADIUS["md"]};
            padding: {SPACING["xl"]};
            margin-bottom: {SPACING["lg"]};
            box-shadow: {SHADOWS["sm"]};
            transition: box-shadow {TRANSITIONS["fast"]};
            display: flex;
            flex-direction: column;
            justify-content: flex-start;
            align-items: center;
            text-align: center;
            height: {LAYOUT["metric_card_height"]};
            width: 100%;
            margin: 0 auto {SPACING["lg"]} auto;
            position: relative;
            overflow: visible;
        }}
        
        .metric-card:hover {{
            box-shadow: {SHADOWS["md"]};
        }}
        
        .metric-card-insights {{
            height: {LAYOUT["insight_card_height"]};
        }}
        
        .metric-header {{
            display: flex;
            justify-content: center;
            align-items: center;
            margin-bottom: {SPACING["md"]};
            width: 100%;
            position: relative;
        }}
        
        .metric-title {{
            font-size: {TYPOGRAPHY["metric_title_size"]};
            font-weight: 500;
            color: {COLORS["gray_500"]};
            text-align: center;
            flex-grow: 1;
            line-height: 1.4;
        }}
        
        .metric-value {{
            font-size: {TYPOGRAPHY["metric_value_size"]};
            font-weight: 600;
            color: {COLORS["gray_900"]};
            margin-bottom: {SPACING["md"]};
            line-height: 1.1;
            text-align: center;
        }}
        
        .metric-delta {{
            font-size: {TYPOGRAPHY["metric_delta_size"]};
            font-weight: 500;
            padding: {SPACING["xs"]} {SPACING["sm"]};
            border-radius: {BORDER_RADIUS["lg"]};
            display: inline-block;
            text-align: center;
            line-height: 1.2;
        }}
        
        .metric-delta-positive {{
            background-color: {COLORS["green_100"]};
            color: {COLORS["green_600"]};
        }}
        
        .metric-delta-negative {{
            background-color: {COLORS["red_100"]};
            color: {COLORS["red_600"]};
        }}
        
        .metric-delta-neutral {{
            background-color: {COLORS["gray_100"]};
            color: {COLORS["gray_500"]};
        }}
    """


def get_tooltip_styles():
    """Get CSS styles for help tooltips."""
    return f"""
        .metric-help-container {{
            position: absolute;
            right: 0;
            top: 50%;
            transform: translateY(-50%);
            z-index: 10001;
        }}
        
        .metric-help {{
            width: {LAYOUT["help_icon_size"]};
            height: {LAYOUT["help_icon_size"]};
            border-radius: {BORDER_RADIUS["full"]};
            background-color: {COLORS["gray_100"]};
            color: {COLORS["gray_500"]};
            font-size: {TYPOGRAPHY["help_size"]};
            font-weight: bold;
            text-align: center;
            line-height: {LAYOUT["help_icon_size"]};
            cursor: help;
            opacity: 0.7;
            display: block;
            transition: opacity {TRANSITIONS["fast"]};
            position: relative;
        }}
        
        .metric-help:hover {{
            opacity: 1;
            background-color: {COLORS["gray_200"]};
        }}
        
        .metric-tooltip {{
            position: absolute;
            bottom: 30px;
            right: -50px;
            background-color: {COLORS["gray_800"]};
            color: {COLORS["white"]};
            padding: 10px 14px;
            border-radius: {BORDER_RADIUS["md"]};
            font-size: {TYPOGRAPHY["tooltip_size"]};
            line-height: 1.4;
            width: {LAYOUT["tooltip_width"]};
            z-index: 10002;
            opacity: 0;
            visibility: hidden;
            transform: translateY(10px);
            transition: all {TRANSITIONS["normal"]};
            box-shadow: {SHADOWS["lg"]};
            text-align: left;
            pointer-events: none;
        }}
        
        .metric-tooltip::before {{
            content: '';
            position: absolute;
            bottom: -6px;
            right: 60px;
            width: {SPACING["md"]};
            height: {SPACING["md"]};
            background-color: {COLORS["gray_800"]};
            transform: rotate(45deg);
            z-index: -1;
        }}
        
        .metric-help-container:hover .metric-tooltip {{
            opacity: 1;
            visibility: visible;
            transform: translateY(0);
        }}
    """


def get_keyword_selector_styles():
    """Get CSS styles for keyword selector component."""
    return f"""
        .keyword-selector-container {{
            background: {COLORS["gray_50"]};
            padding: {SPACING["lg"]};
            border-radius: {BORDER_RADIUS["lg"]};
            margin-bottom: {SPACING["2xl"]};
        }}
        
        .keyword-performance-card {{
            background: {COLORS["white"]};
            padding: {SPACING["lg"]};
            border-radius: {BORDER_RADIUS["md"]};
            box-shadow: {SHADOWS["sm"]};
        }}
        
        .keyword-performance-card h4 {{
            margin: 0;
            color: {COLORS["gray_900"]};
        }}
        
        .keyword-performance-card p {{
            margin: {SPACING["sm"]} 0 {SPACING["xs"]} 0;
            font-size: {TYPOGRAPHY["metric_title_size"]};
            color: {COLORS["gray_500"]};
        }}
        
        .keyword-performance-stats {{
            display: flex;
            justify-content: space-between;
            margin-top: {SPACING["sm"]};
        }}
        
        .keyword-performance-positive {{
            color: {COLORS["success"]};
        }}
        
        .keyword-performance-confidence {{
            color: {COLORS["gray_500"]};
        }}
    """


def get_insight_details_styles():
    """Get CSS styles for detailed insight information within cards."""
    return f"""
        .insight-card-details {{
            margin-top: {SPACING["md"]};
            font-size: {TYPOGRAPHY["metric_delta_size"]};
            color: {COLORS["gray_500"]};
            line-height: 1.4;
            padding: 0 2px;
        }}
        
        .insight-best-worst {{
            margin-bottom: 2px;
        }}
        
        .insight-rank-info {{
            font-weight: 500;
        }}
        
        .insight-period-info {{
            margin-top: 10px;
        }}
    """