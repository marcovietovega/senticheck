#!/usr/bin/env python3
"""
Keyword Selector Component for SentiCheck Dashboard

This component provides a dynamic keyword selection interface that automatically
detects available keywords from the database and allows users to filter analytics.
"""

import streamlit as st
from typing import List, Optional
import sys
from pathlib import Path

# Add parent directory to path for imports
parent_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(parent_dir))

from dashboard.data_service import get_dashboard_data_service


def render_keyword_selector() -> Optional[List[str]]:
    """
    Render the keyword selector component.

    Returns:
        List of selected keywords or None if "All" is selected
    """
    try:
        data_service = get_dashboard_data_service()

        keywords_data = data_service.get_available_keywords()

        if not keywords_data:
            st.warning("⚠️ No keywords found in database. Please check data collection.")
            return ["AI"]

        available_keywords = [k for k in keywords_data.keys() if k and k.strip()]

        if not available_keywords:
            st.warning("⚠️ No valid keywords found in database.")
            return ["AI"]

        st.markdown('<div class="keyword-selector-container">', unsafe_allow_html=True)

        col1, col2 = st.columns([3, 1])

        with col1:
            st.markdown("### 🏷️ **Keyword Selection**")

            selected_keywords = st.multiselect(
                "Select keywords to analyze:",
                options=available_keywords,
                default=[available_keywords[0]] if available_keywords else [],
                key="keyword_selector",
                help=f"Choose from {len(available_keywords)} available keywords for analysis",
            )

            if st.checkbox("Select all keywords", key="select_all_keywords"):
                selected_keywords = available_keywords

            if selected_keywords:
                total_posts = sum(keywords_data.get(kw, 0) for kw in selected_keywords)
                st.info(
                    f"📊 Selected: **{len(selected_keywords)} keywords** with **{total_posts:,} total posts**"
                )

        with col2:
            st.markdown("### 📈 **Available Keywords**")

            for keyword, count in list(keywords_data.items())[:5]:  # Show top 5
                is_selected = keyword in (selected_keywords or [])
                icon = "✅" if is_selected else "⚪"
                st.markdown(f"{icon} **{keyword}**: {count:,} posts")

            if len(keywords_data) > 5:
                st.markdown(f"... and {len(keywords_data) - 5} more keywords")

        st.markdown("</div>", unsafe_allow_html=True)

        if not selected_keywords:
            return None

        return selected_keywords

    except Exception as e:
        st.error(f"Error loading keywords: {e}")
        return ["AI"]


def render_keyword_performance_summary(selected_keywords: Optional[List[str]] = None):
    """
    Render a quick performance summary for selected keywords.

    Args:
        selected_keywords: List of selected keywords, None for all
    """
    if not selected_keywords:
        return

    data_service = get_dashboard_data_service()

    st.markdown("### 🎯 **Keyword Performance Summary**")

    if len(selected_keywords) <= 3:
        cols = st.columns(len(selected_keywords))
    else:
        cols = st.columns(3)

    for i, keyword in enumerate(selected_keywords[: len(cols)]):
        try:
            metrics = data_service.get_keyword_metrics(keyword)

            with cols[i]:
                pos_pct = metrics.get("positive_percentage", 0)
                color = "green" if pos_pct > 20 else "orange" if pos_pct > 10 else "red"

                border_class = (
                    "border-left-success"
                    if color == "green"
                    else (
                        "border-left-warning"
                        if color == "orange"
                        else "border-left-danger"
                    )
                )

                st.markdown(
                    f"""
                <div class="keyword-performance-card {border_class}">
                    <h4>🏷️ {keyword}</h4>
                    <p>{metrics.get("total_posts", 0):,} posts analyzed</p>
                    <div class="keyword-performance-stats">
                        <span class="keyword-performance-positive">↗️ {pos_pct:.1f}%</span>
                        <span class="keyword-performance-confidence">📊 {metrics.get("avg_confidence", 0):.1f}%</span>
                    </div>
                </div>
                """,
                    unsafe_allow_html=True,
                )

        except Exception as e:
            with cols[i]:
                st.error(f"Error loading {keyword}: {str(e)[:50]}...")

    if len(selected_keywords) > 3:
        st.info(f"📝 Showing first 3 of {len(selected_keywords)} selected keywords")
