"""
Page layout and structure styles for the SentiCheck dashboard.
Contains styles for titles, sections, spacing, and overall page layout.
"""

from .base import COLORS, TYPOGRAPHY, SPACING, BORDER_RADIUS, SHADOWS


def get_page_title_styles():
    """Get CSS styles for the main page title."""
    return f"""
        .page-title-container {{
            text-align: center;
            margin-bottom: {SPACING["3xl"]};
        }}
        
        .page-title {{
            font-size: {TYPOGRAPHY["title_size"]};
            font-weight: 600;
            margin-bottom: {SPACING["sm"]};
            color: {COLORS["gray_900"]};
        }}
        
        .page-subtitle {{
            color: {COLORS["gray_500"]};
            font-size: {TYPOGRAPHY["subtitle_size"]};
            line-height: 1.5;
        }}
    """


def get_section_styles():
    """Get CSS styles for dashboard sections.""" 
    return f"""
        .dashboard-section {{
            margin-bottom: {SPACING["2xl"]};
        }}
        
        .section-header {{
            margin: 2rem 0 0 0 !important;
            padding: 0 !important;
        }}
        
        .section-divider {{
            margin: 0.5rem 0 2rem 0px !important;
        }}
    """


def get_spacing_utilities():
    """Get CSS utility classes for spacing."""
    return f"""
        .spacing-top-sm {{
            margin-top: {SPACING["sm"]} !important;
        }}
        
        .spacing-top-md {{
            margin-top: {SPACING["md"]} !important;
        }}
        
        .spacing-top-lg {{
            margin-top: {SPACING["lg"]} !important;
        }}
        
        .spacing-top-xl {{
            margin-top: {SPACING["xl"]} !important;
        }}
        
        .spacing-top-2xl {{
            margin-top: {SPACING["2xl"]} !important;
        }}
        
        .spacing-bottom-sm {{
            margin-bottom: {SPACING["sm"]} !important;
        }}
        
        .spacing-bottom-md {{
            margin-bottom: {SPACING["md"]} !important;
        }}
        
        .spacing-bottom-lg {{
            margin-bottom: {SPACING["lg"]} !important;
        }}
        
        .spacing-bottom-xl {{
            margin-bottom: {SPACING["xl"]} !important;
        }}
        
        .spacing-bottom-2xl {{
            margin-bottom: {SPACING["2xl"]} !important;
        }}
    """


def get_layout_styles():
    """Get general layout and positioning styles."""
    return f"""
        .flex-center {{
            display: flex;
            justify-content: center;
            align-items: center;
        }}
        
        .flex-between {{
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        
        .text-center {{
            text-align: center;
        }}
        
        .text-left {{
            text-align: left;
        }}
        
        .full-width {{
            width: 100%;
        }}
        
        .border-left-success {{
            border-left: 4px solid {COLORS["success"]};
        }}
        
        .border-left-warning {{
            border-left: 4px solid {COLORS["orange_600"]};
        }}
        
        .border-left-danger {{
            border-left: 4px solid {COLORS["red_600"]};
        }}
    """


def get_sidebar_styles():
    """Get CSS styles for the unified sidebar controls."""
    return f"""
        .sidebar .sidebar-content {{
            background-color: {COLORS["white"]};
            border-right: 1px solid {COLORS["gray_200"]};
            padding: {SPACING["lg"]};
        }}
        
        .sidebar h2 {{
            font-size: 1.2rem !important;
            font-weight: 600 !important;
            color: {COLORS["gray_800"]} !important;
            margin-bottom: {SPACING["md"]} !important;
            padding-bottom: {SPACING["sm"]} !important;
            border-bottom: 2px solid {COLORS["blue_600"]} !important;
        }}
        
        .sidebar .stSelectbox > div > div {{
            background-color: {COLORS["gray_50"]};
            border: 1px solid {COLORS["gray_200"]};
            border-radius: {BORDER_RADIUS["md"]};
            transition: all 0.2s ease;
        }}
        
        .sidebar .stSelectbox > div > div:hover {{
            border-color: {COLORS["blue_600"]};
            box-shadow: 0 0 0 1px {COLORS["blue_600"]}40;
        }}
        
        .sidebar .stRadio > div {{
            gap: {SPACING["sm"]};
        }}
        
        .sidebar .stRadio > div > label {{
            display: flex !important;
            align-items: center !important;
            padding: {SPACING["sm"]} {SPACING["md"]} !important;
            margin: 0 0 {SPACING["xs"]} 0 !important;
            background-color: {COLORS["white"]} !important;
            border: 1px solid {COLORS["gray_200"]} !important;
            border-radius: {BORDER_RADIUS["md"]} !important;
            cursor: pointer !important;
            transition: all 0.2s ease !important;
            width: 100% !important;
        }}
        
        .sidebar .stRadio > div > label:hover {{
            background-color: {COLORS["gray_50"]} !important;
            border-color: {COLORS["blue_600"]} !important;
            transform: translateX(2px) !important;
        }}
        
        .sidebar .stRadio > div > label[data-checked="true"] {{
            background-color: {COLORS["blue_600"]} !important;
            color: {COLORS["white"]} !important;
            border-color: {COLORS["blue_600"]} !important;
            font-weight: 600 !important;
        }}
        
        .sidebar .stRadio > div > label > div:first-child {{
            margin-right: {SPACING["sm"]} !important;
        }}
        
        .sidebar hr {{
            margin: {SPACING["lg"]} 0 !important;
            border: none !important;
            height: 1px !important;
            background-color: {COLORS["gray_200"]} !important;
        }}
        
        .sidebar .stSpinner {{
            color: {COLORS["blue_600"]} !important;
        }}
        
        .sidebar .stTooltipIcon {{
            color: {COLORS["gray_500"]} !important;
            opacity: 0.7 !important;
            transition: opacity 0.2s ease !important;
        }}
        
        .sidebar .stTooltipIcon:hover {{
            opacity: 1 !important;
            color: {COLORS["gray_800"]} !important;
        }}
        
        .sidebar [data-testid="stTooltipHoverTarget"] {{
            position: relative !important;
        }}
        
        .sidebar [data-testid="stTooltipContent"] {{
            background-color: {COLORS["gray_800"]} !important;
            color: {COLORS["white"]} !important;
            border-radius: {BORDER_RADIUS["md"]} !important;
            padding: 10px 14px !important;
            font-size: 0.85rem !important;
            line-height: 1.4 !important;
            box-shadow: {SHADOWS["lg"]} !important;
            border: none !important;
            max-width: 200px !important;
        }}
        
        @media (max-width: 768px) {{
            .sidebar .sidebar-content {{
                padding: {SPACING["md"]};
            }}
            
            .sidebar h2 {{
                font-size: 1.1rem !important;
            }}
            
            .sidebar .stRadio > div > label {{
                padding: {SPACING["xs"]} {SPACING["sm"]} !important;
                font-size: 0.9rem !important;
            }}
        }}
    """