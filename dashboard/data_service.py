#!/usr/bin/env python3

import logging
import sys
import pandas as pd
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from pathlib import Path

parent_dir = Path(__file__).parent.parent
sys.path.insert(0, str(parent_dir))


from models.db_manager import get_db_manager
from dashboard.config import DASHBOARD_CONFIG

logger = logging.getLogger(__name__)


class DashboardDataService:
    """Data service for the SentiCheck dashboard.

    Provides methods to fetch and process data for dashboard components,
    including KPIs, charts, and trends.
    """

    def __init__(self):
        self.db_manager = get_db_manager()
        self.cache = {}
        self.cache_ttl = DASHBOARD_CONFIG["refresh"]["cache_ttl"]

    def _is_cache_valid(self, cache_key: str) -> bool:
        """Check if cached data is still valid.

        Returns:
            True if cache is valid, False otherwise
        """
        if cache_key not in self.cache:
            return False

        cached_time = self.cache[cache_key].get("timestamp")
        if not cached_time:
            return False

        return (datetime.now() - cached_time).seconds < self.cache_ttl

    def _get_cached_data(self, cache_key: str) -> Optional[Any]:
        """
        Get data from cache if valid.

        Args:
            cache_key: The key for the cached data

        Returns:
            Cached data if valid, None otherwise
        """
        if self._is_cache_valid(cache_key):
            return self.cache[cache_key]["data"]
        return None

    def _set_cache_data(self, cache_key: str, data: Any):
        """
        Store data in cache with timestamp.

        Args:
            cache_key: The key for the cached data
            data: The data to cache
        """
        self.cache[cache_key] = {"data": data, "timestamp": datetime.now()}

    def get_database_stats(self) -> Dict[str, int]:
        """
        Get basic database statistics.

        Returns:
            Dict with counts of raw posts, cleaned posts, and analyzed posts
        """
        cache_key = "database_stats"
        cached_data = self._get_cached_data(cache_key)
        if cached_data:
            return cached_data

        try:
            stats = self.db_manager.get_database_stats()
            self._set_cache_data(cache_key, stats)
            return stats
        except Exception as e:
            logger.error(f"Error getting database stats: {e}")
            return {
                "raw_posts": 0,
                "cleaned_posts": 0,
                "analyzed_posts": 0,
                "unprocessed_posts": 0,
                "unanalyzed_posts": 0,
            }

    def get_sentiment_distribution(self) -> Dict[str, int]:
        """
        Get sentiment distribution counts.

        Returns:
            Dict with counts for each sentiment label
        """
        cache_key = "sentiment_distribution"
        cached_data = self._get_cached_data(cache_key)
        if cached_data:
            return cached_data

        try:
            distribution = self.db_manager.get_sentiment_distribution()
            self._set_cache_data(cache_key, distribution)
            return distribution

        except Exception as e:
            logger.error(f"Error getting sentiment distribution: {e}")
            return {"positive": 0, "negative": 0, "neutral": 0}

    def get_average_confidence(self) -> float:
        """
        Get average confidence score across all analyzed posts.

        Returns:
            Average confidence score as percentage (0-100)
        """
        return self.db_manager.get_average_confidence()

    def get_posts_by_date(self, days: int = 7) -> Dict[str, int]:
        """
        Get post counts by date for the last N days.

        Args:
            days: Number of days to look back

        Returns:
            Dict with dates as keys and post counts as values
        """
        return self.db_manager.get_posts_by_date(days)

    def get_today_posts_count(self) -> int:
        """
        Get count of posts created today.

        Returns:
            Count of posts created today
        """
        return self.db_manager.get_today_posts_count()

    def calculate_sentiment_trends(
        self, selected_keywords: Optional[List[str]] = None
    ) -> Dict[str, float]:
        """
        Calculate sentiment trends compared to previous day.

        Returns:
            Dict with trend percentages for each sentiment
        """
        try:
            return self.db_manager.calculate_sentiment_trends(selected_keywords)
        except Exception as e:
            logger.error(f"Error calculating sentiment trends: {e}")
            return {"positive_trend": 0.0, "negative_trend": 0.0, "neutral_trend": 0.0}

    def get_sentiment_over_time(self, days: int = 7) -> pd.DataFrame:
        """
        Get sentiment data over time for charting.

        Args:
            days: Number of days of historical data to return

        Returns:
            DataFrame with columns: date, positive, negative, neutral
        """
        cache_key = f"sentiment_over_time_{days}"
        cached_data = self._get_cached_data(cache_key)
        if cached_data is not None:
            return cached_data

        try:
            # Get raw data from db_manager
            raw_data = self.db_manager.get_sentiment_over_time(days)

            # Calculate date range for filling missing dates
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=days - 1)
            date_range = pd.date_range(start=start_date, end=end_date, freq="D")

            if raw_data:
                # Convert raw data to DataFrame
                df = pd.DataFrame(raw_data)
                df["date"] = pd.to_datetime(df["date"])

                # Fill missing dates with zero counts
                df = (
                    df.set_index("date").reindex(date_range, fill_value=0).reset_index()
                )
                df.rename(columns={"index": "date"}, inplace=True)
            else:
                # Create empty DataFrame with full date range
                df = pd.DataFrame(
                    {"date": date_range, "positive": 0, "negative": 0, "neutral": 0}
                )

            self._set_cache_data(cache_key, df)
            return df

        except Exception as e:
            logger.error(f"Error getting sentiment over time: {e}")
            # Return empty DataFrame on error
            return pd.DataFrame(columns=["date", "positive", "negative", "neutral"])

    def get_kpi_metrics(
        self, selected_keywords: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Get all KPI metrics for the dashboard, optionally filtered by keywords.

        Args:
            selected_keywords: List of keywords to filter by, None for all keywords

        Returns:
            Dict with all KPI values and trends
        """
        cache_key = f"kpi_metrics_{selected_keywords or 'all'}"
        cached_data = self._get_cached_data(cache_key)
        if cached_data:
            return cached_data

        try:
            kpi_base = self.db_manager.get_unified_kpi_metrics(selected_keywords)
            trends = self.db_manager.calculate_sentiment_trends(selected_keywords)

            yesterday_posts = self.get_posts_by_date(2)
            posts_data = list(yesterday_posts.values())
            yesterday_count = posts_data[0] if len(posts_data) >= 2 else 0
            posts_today = kpi_base["posts_today"]

            daily_trend = (
                ((posts_today - yesterday_count) / yesterday_count * 100)
                if yesterday_count > 0
                else (100.0 if posts_today > 0 else 0.0)
            )

            kpi_data = {
                **kpi_base,
                "positive_trend": trends.get("positive_trend", 0.0),
                "negative_trend": trends.get("negative_trend", 0.0),
                "neutral_trend": trends.get("neutral_trend", 0.0),
                "confidence_trend": 0.0,
                "daily_trend": round(daily_trend, 1),
            }

            self._set_cache_data(cache_key, kpi_data)
            return kpi_data

        except Exception as e:
            logger.error(f"Error getting KPI metrics: {e}")
            return {
                "total_posts": 0,
                "positive_percentage": 0.0,
                "negative_percentage": 0.0,
                "neutral_percentage": 0.0,
                "avg_confidence": 0.0,
                "posts_today": 0,
                "positive_trend": 0.0,
                "negative_trend": 0.0,
                "neutral_trend": 0.0,
                "confidence_trend": 0.0,
                "daily_trend": 0.0,
            }

    def get_recent_posts(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get recent posts with sentiment analysis.

        Args:
            limit: Maximum number of posts to return

        Returns:
            List of post dictionaries with sentiment data
        """
        try:
            # Get raw data from db_manager
            raw_posts = self.db_manager.get_recent_posts_with_analysis(limit)

            # Format the data for display (truncate text fields)
            posts = []
            for post in raw_posts:
                posts.append(
                    {
                        "id": post["id"],
                        "text": (
                            post["text"][:200] + "..."
                            if len(post["text"]) > 200
                            else post["text"]
                        ),
                        "author": post["author"],
                        "created_at": post["created_at"],
                        "sentiment": post["sentiment"],
                        "confidence": round(post["confidence"] * 100, 1),
                        "cleaned_text": (
                            post["cleaned_text"][:100] + "..."
                            if len(post["cleaned_text"]) > 100
                            else post["cleaned_text"]
                        ),
                    }
                )

            return posts

        except Exception as e:
            logger.error(f"Error getting recent posts: {e}")
            return []

    def get_available_keywords(self) -> Dict[str, int]:
        """
        Get all available keywords from the database with their post counts.

        Returns:
            Dict mapping keyword to post count
        """
        cache_key = "available_keywords"
        cached_data = self._get_cached_data(cache_key)
        if cached_data:
            return cached_data

        try:
            # Get keywords with counts from database
            keywords_data = self.db_manager.get_keywords_with_counts()

            # Convert to dict format
            keywords_dict = (
                {keyword: count for keyword, count in keywords_data}
                if keywords_data
                else {"AI": 0}
            )  # Fallback to AI if no data

            self._set_cache_data(cache_key, keywords_dict)
            return keywords_dict

        except Exception as e:
            logger.error(f"Error getting available keywords: {e}")
            return {"AI": 0}  # Fallback default

    def get_keyword_metrics(self, keyword: str) -> Dict[str, Any]:
        """
        Get metrics for a specific keyword.

        Args:
            keyword: The keyword to analyze

        Returns:
            Dict with keyword-specific metrics
        """
        cache_key = f"keyword_metrics_{keyword}"
        cached_data = self._get_cached_data(cache_key)
        if cached_data:
            return cached_data

        try:
            # Get keyword-specific data
            keyword_data = self.db_manager.get_keyword_specific_metrics(keyword)

            if not keyword_data:
                return {
                    "total_posts": 0,
                    "positive_percentage": 0,
                    "negative_percentage": 0,
                    "neutral_percentage": 0,
                    "avg_confidence": 0,
                    "posts_today": 0,
                }

            self._set_cache_data(cache_key, keyword_data)
            return keyword_data

        except Exception as e:
            logger.error(f"Error getting keyword metrics for {keyword}: {e}")
            return {
                "total_posts": 0,
                "positive_percentage": 0,
                "negative_percentage": 0,
                "neutral_percentage": 0,
                "avg_confidence": 0,
                "posts_today": 0,
            }


# Global data service instance
_data_service = None


def get_dashboard_data_service() -> DashboardDataService:
    """Get the global dashboard data service instance."""
    global _data_service
    if _data_service is None:
        _data_service = DashboardDataService()
    return _data_service
