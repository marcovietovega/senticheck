import streamlit as st
from typing import Dict, Any
import logging

from dashboard.data_service_api import get_dashboard_data_service

logger = logging.getLogger(__name__)


def render_sidebar_controls() -> Dict[str, Any]:
    """
    Render unified sidebar controls

    Returns:
        Dict with selected_keyword (str) and time_range_days (int)
    """
    try:
        data_service = get_dashboard_data_service()

        with st.sidebar:
            st.markdown("## Time Range")
            time_range = st.selectbox(
                "Analysis period:",
                options=["7 days", "15 days", "30 days"],
                index=0,
                key="unified_time_range",
                help="Select time period for all charts and analysis",
            )

            days_map = {"7 days": 7, "15 days": 15, "30 days": 30}
            selected_days = days_map[time_range]

            st.markdown("---")

            st.markdown("## Keywords")

            keywords_data = data_service.get_available_keywords()

            if not keywords_data:
                st.warning("No keywords found in database.")
                return {"selected_keyword": "AI", "time_range_days": selected_days}

            available_keywords = [item["keyword"] for item in keywords_data]
            keyword_counts = {item["keyword"]: item["count"] for item in keywords_data}

            def format_keyword_with_count(keyword: str) -> str:
                total_count = keyword_counts.get(keyword, 0)
                return f"{keyword} ({total_count:,} posts)"

            selected_keyword = st.radio(
                "Select keyword to analyse:",
                options=available_keywords,
                format_func=format_keyword_with_count,
                key="sidebar_keyword_selector",
                help="Choose keyword for individual analysis within selected time range",
            )

            st.markdown("---")

            st.markdown("## Quick Stats")

            if selected_keyword:
                posts_in_range = keyword_counts.get(selected_keyword, 0)

                st.markdown(f"**Range**: {time_range}")
                st.markdown(f"**Keyword**: {selected_keyword}")
                st.markdown(f"**Posts**: {posts_in_range:,}")

                total_keywords = len(available_keywords)
                keyword_rank = (
                    sorted(
                        keyword_counts.items(), key=lambda x: x[1], reverse=True
                    ).index((selected_keyword, posts_in_range))
                    + 1
                )

                st.markdown(f"**Rank**: #{keyword_rank} of {total_keywords}")

            return {
                "selected_keyword": selected_keyword or "AI",
                "time_range_days": selected_days,
            }

    except Exception as e:
        logger.error(f"Error rendering sidebar controls: {e}")
        st.sidebar.error(f"Error loading sidebar controls: {e}")

        return {"selected_keyword": "AI", "time_range_days": 7}


def get_sidebar_selection() -> Dict[str, Any]:
    """
    Get current sidebar selection from session state.

    Returns:
        Dict with current sidebar selections
    """
    return {
        "selected_keyword": st.session_state.get("selected_keyword", "AI"),
        "time_range_days": st.session_state.get("time_range_days", 7),
    }


def update_session_state_from_sidebar(sidebar_data: Dict[str, Any]) -> None:
    """
    Update session state with sidebar selections.

    Args:
        sidebar_data: Dict from render_sidebar_controls()
    """
    st.session_state["selected_keyword"] = sidebar_data["selected_keyword"]
    st.session_state["time_range_days"] = sidebar_data["time_range_days"]
