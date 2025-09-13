import streamlit as st
from .components import (
    get_metric_card_styles,
    get_tooltip_styles,
    get_keyword_selector_styles,
    get_insight_details_styles,
)
from .layouts import (
    get_page_title_styles,
    get_section_styles,
    get_spacing_utilities,
    get_layout_styles,
    get_sidebar_styles,
)
from .utils import (
    get_dynamic_color,
    get_delta_color_class,
    get_rank_style,
    get_confidence_quality,
    format_time_12h,
    format_date_short,
)


def apply_all_styles():
    """
    Apply all dashboard styles in one function call.
    This should be called once at the start of the app.
    """
    all_styles = f"""
    <style>
    {get_metric_card_styles()}
    {get_tooltip_styles()}
    {get_keyword_selector_styles()}
    {get_insight_details_styles()}
    {get_page_title_styles()}
    {get_section_styles()}
    {get_spacing_utilities()}
    {get_layout_styles()}
    {get_sidebar_styles()}
    </style>
    """

    st.markdown(all_styles, unsafe_allow_html=True)


def apply_component_styles():
    """Apply only component-specific styles."""
    styles = f"""
    <style>
    {get_metric_card_styles()}
    {get_tooltip_styles()}
    {get_keyword_selector_styles()}
    {get_insight_details_styles()}
    </style>
    """
    st.markdown(styles, unsafe_allow_html=True)


def apply_layout_styles():
    """Apply only layout and page-level styles."""
    styles = f"""
    <style>
    {get_page_title_styles()}
    {get_section_styles()}
    {get_spacing_utilities()}
    {get_layout_styles()}
    </style>
    """
    st.markdown(styles, unsafe_allow_html=True)


# Export commonly used functions
__all__ = [
    "apply_all_styles",
    "apply_component_styles",
    "apply_layout_styles",
    "get_dynamic_color",
    "get_delta_color_class",
    "get_rank_style",
    "get_confidence_quality",
    "format_time_12h",
    "format_date_short",
]
