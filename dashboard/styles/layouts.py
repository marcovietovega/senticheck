"""
Page layout and structure styles for the SentiCheck dashboard.
Contains styles for titles, sections, spacing, and overall page layout.
"""

from .base import COLORS, TYPOGRAPHY, SPACING, BORDER_RADIUS


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