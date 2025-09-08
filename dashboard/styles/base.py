"""
Base design tokens and constants for the SentiCheck dashboard.
Provides consistent colors, spacing, typography, and other design foundations.
"""

# Color palette
COLORS = {
    # Primary grays
    "gray_900": "#111827",
    "gray_800": "#1f2937",
    "gray_700": "#374151",
    "gray_600": "#4b5563",
    "gray_500": "#6b7280",
    "gray_400": "#9ca3af",
    "gray_300": "#d1d5db",
    "gray_200": "#e5e7eb",
    "gray_100": "#f3f4f6",
    "gray_50": "#f9fafb",
    # Status colors
    "green_600": "#059669",
    "green_100": "#d1fae5",
    "blue_600": "#2563eb",
    "orange_600": "#d97706",
    "red_600": "#dc2626",
    "red_100": "#fee2e2",
    # Special colors
    "white": "#ffffff",
    "black": "#000000",
    "success": "#10b981",
}

# Typography
TYPOGRAPHY = {
    "font_family": "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif",
    "title_size": "48px",
    "subtitle_size": "18px",
    "metric_value_size": "28px",
    "metric_title_size": "14px",
    "metric_delta_size": "12px",
    "tooltip_size": "12px",
    "help_size": "10px",
}

# Spacing
SPACING = {
    "xs": "4px",
    "sm": "8px",
    "md": "12px",
    "lg": "16px",
    "xl": "20px",
    "2xl": "24px",
    "3xl": "32px",
}

# Border radius
BORDER_RADIUS = {
    "sm": "4px",
    "md": "8px",
    "lg": "12px",
    "xl": "16px",
    "full": "50%",
}

# Shadows
SHADOWS = {
    "sm": "0 1px 3px rgba(0,0,0,0.05)",
    "md": "0 4px 12px rgba(0,0,0,0.08)",
    "lg": "0 10px 20px rgba(0,0,0,0.25)",
}

# Layout dimensions
LAYOUT = {
    "metric_card_height": "140px",
    "insight_card_height": "180px",
    "tooltip_width": "240px",
    "help_icon_size": "16px",
}

# Animation timing
TRANSITIONS = {
    "fast": "0.2s ease",
    "normal": "0.3s ease",
}
