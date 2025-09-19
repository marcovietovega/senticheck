"""
Shared utilities for the SentiCheck dashboard.
Contains common functions used across dashboard components.
"""

import streamlit as st
from typing import Optional


def render_metric_card(
    title: str,
    value: str,
    delta: Optional[str] = None,
    delta_color: str = "normal",
    help_text: Optional[str] = None,
):
    """
    Render a KPI metric card.

    Args:
        title: The metric label
        value: The primary value to display
        delta: Optional delta value with sign and unit
        delta_color: Color for delta ('normal', 'inverse', 'off')
        help_text: Optional help text for the metric
    """
    card_html = f"""
    <div class="metric-card">
        <div class="metric-header">
            <span class="metric-title">{title}</span>
            {f'<div class="metric-help-container"><span class="metric-help">?</span><div class="metric-tooltip">{help_text}</div></div>' if help_text else ''}
        </div>
        <div class="metric-value">{value}</div>
        {f'<div class="metric-delta metric-delta-{delta_color}">{delta}</div>' if delta else ''}
    </div>
    """

    st.markdown(card_html, unsafe_allow_html=True)