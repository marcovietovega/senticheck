#!/usr/bin/env python3
"""
Dashboard Data Service

This module provides data access and processing services for the SentiCheck dashboard.
It handles all database operations and data transformations needed for visualization.
"""

import logging
import sys
import os
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Add parent directory to path for imports
parent_dir = Path(__file__).parent.parent
sys.path.insert(0, str(parent_dir))

try:
    from models.db_manager import get_db_manager
    from models.database import RawPost, CleanedPost, SentimentAnalysis
    from dashboard.config import DASHBOARD_CONFIG, KPI_METRICS
except ImportError as e:
    logging.error(f"Import error in data service: {e}")

logger = logging.getLogger(__name__)


class DashboardDataService:
    """
    Data service for the SentiCheck dashboard.

    Provides high-level methods to fetch and process data for dashboard components,
    including KPIs, charts, and trends.
    """

    def __init__(self):
        """Initialize the data service."""
        self.db_manager = get_db_manager()
        self.cache = {}
        self.cache_ttl = DASHBOARD_CONFIG["refresh"]["cache_ttl"]

    def _is_cache_valid(self, cache_key: str) -> bool:
        """Check if cached data is still valid."""
        if cache_key not in self.cache:
            return False

        cached_time = self.cache[cache_key].get("timestamp")
        if not cached_time:
            return False

        return (datetime.now() - cached_time).seconds < self.cache_ttl

    def _get_cached_data(self, cache_key: str) -> Optional[Any]:
        """Get data from cache if valid."""
        if self._is_cache_valid(cache_key):
            return self.cache[cache_key]["data"]
        return None

    def _set_cache_data(self, cache_key: str, data: Any):
        """Store data in cache with timestamp."""
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
            db_ops = self.db_manager.db_ops
            with db_ops.db_connection.get_session() as session:
                # Query sentiment distribution
                from sqlalchemy import func

                result = (
                    session.query(
                        SentimentAnalysis.sentiment_label,
                        func.count(SentimentAnalysis.id).label("count"),
                    )
                    .group_by(SentimentAnalysis.sentiment_label)
                    .all()
                )

                distribution = {"positive": 0, "negative": 0, "neutral": 0}

                for sentiment, count in result:
                    if sentiment in distribution:
                        distribution[sentiment] = count

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
        try:
            db_ops = self.db_manager.db_ops
            with db_ops.db_connection.get_session() as session:
                from sqlalchemy import func

                result = session.query(
                    func.avg(SentimentAnalysis.confidence_score)
                ).scalar()

                return round(result * 100, 1) if result else 0.0

        except Exception as e:
            logger.error(f"Error getting average confidence: {e}")
            return 0.0

    def get_posts_by_date(self, days: int = 7) -> Dict[str, int]:
        """
        Get post counts by date for the last N days.

        Args:
            days: Number of days to look back

        Returns:
            Dict with dates as keys and post counts as values
        """
        try:
            db_ops = self.db_manager.db_ops
            with db_ops.db_connection.get_session() as session:
                from sqlalchemy import func, Date

                end_date = datetime.now(timezone.utc).date()
                start_date = end_date - timedelta(days=days)

                result = (
                    session.query(
                        func.date(RawPost.created_at).label("date"),
                        func.count(RawPost.id).label("count"),
                    )
                    .filter(func.date(RawPost.created_at) >= start_date)
                    .group_by(func.date(RawPost.created_at))
                    .order_by(func.date(RawPost.created_at))
                    .all()
                )

                # Initialize all dates with 0 counts
                date_counts = {}
                current_date = start_date
                while current_date <= end_date:
                    date_counts[current_date.strftime("%Y-%m-%d")] = 0
                    current_date += timedelta(days=1)

                # Fill in actual counts
                for date, count in result:
                    date_key = date.strftime("%Y-%m-%d")
                    date_counts[date_key] = count

                return date_counts

        except Exception as e:
            logger.error(f"Error getting posts by date: {e}")
            return {}

    def get_today_posts_count(self) -> int:
        """Get count of posts created today."""
        try:
            db_ops = self.db_manager.db_ops
            with db_ops.db_connection.get_session() as session:
                from sqlalchemy import func

                today = datetime.now(timezone.utc).date()

                count = (
                    session.query(func.count(RawPost.id))
                    .filter(func.date(RawPost.created_at) == today)
                    .scalar()
                )

                return count or 0

        except Exception as e:
            logger.error(f"Error getting today's post count: {e}")
            return 0

    def calculate_sentiment_trends(self) -> Dict[str, float]:
        """
        Calculate sentiment trends compared to previous day.

        Returns:
            Dict with trend percentages for each sentiment
        """
        try:
            # Get today's and yesterday's sentiment distributions
            today = datetime.now(timezone.utc).date()
            yesterday = today - timedelta(days=1)

            db_ops = self.db_manager.db_ops
            with db_ops.db_connection.get_session() as session:
                from sqlalchemy import func

                # Today's sentiment counts
                today_result = (
                    session.query(
                        SentimentAnalysis.sentiment_label,
                        func.count(SentimentAnalysis.id).label("count"),
                    )
                    .join(CleanedPost)
                    .join(RawPost)
                    .filter(func.date(RawPost.created_at) == today)
                    .group_by(SentimentAnalysis.sentiment_label)
                    .all()
                )

                # Yesterday's sentiment counts
                yesterday_result = (
                    session.query(
                        SentimentAnalysis.sentiment_label,
                        func.count(SentimentAnalysis.id).label("count"),
                    )
                    .join(CleanedPost)
                    .join(RawPost)
                    .filter(func.date(RawPost.created_at) == yesterday)
                    .group_by(SentimentAnalysis.sentiment_label)
                    .all()
                )

                # Convert to dicts
                today_counts = {sentiment: count for sentiment, count in today_result}
                yesterday_counts = {
                    sentiment: count for sentiment, count in yesterday_result
                }

                # Calculate trends
                trends = {}
                for sentiment in ["positive", "negative", "neutral"]:
                    today_count = today_counts.get(sentiment, 0)
                    yesterday_count = yesterday_counts.get(sentiment, 0)

                    if yesterday_count > 0:
                        trend = (
                            (today_count - yesterday_count) / yesterday_count
                        ) * 100
                        trends[f"{sentiment}_trend"] = round(trend, 1)
                    else:
                        trends[f"{sentiment}_trend"] = 0.0

                return trends

        except Exception as e:
            logger.error(f"Error calculating sentiment trends: {e}")
            return {"positive_trend": 0.0, "negative_trend": 0.0, "neutral_trend": 0.0}

    def get_kpi_metrics(self) -> Dict[str, Any]:
        """
        Get all KPI metrics for the dashboard.

        Returns:
            Dict with all KPI values and trends
        """
        cache_key = "kpi_metrics"
        cached_data = self._get_cached_data(cache_key)
        if cached_data:
            return cached_data

        try:
            # Get basic stats
            stats = self.get_database_stats()

            # Get sentiment distribution
            sentiment_dist = self.get_sentiment_distribution()
            total_analyzed = sum(sentiment_dist.values())

            # Calculate percentages
            positive_pct = (
                (sentiment_dist["positive"] / total_analyzed * 100)
                if total_analyzed > 0
                else 0
            )
            negative_pct = (
                (sentiment_dist["negative"] / total_analyzed * 100)
                if total_analyzed > 0
                else 0
            )
            neutral_pct = (
                (sentiment_dist["neutral"] / total_analyzed * 100)
                if total_analyzed > 0
                else 0
            )

            # Get other metrics
            avg_confidence = self.get_average_confidence()
            posts_today = self.get_today_posts_count()

            # Calculate trends
            trends = self.calculate_sentiment_trends()

            # Calculate daily trend for posts
            yesterday_posts = self.get_posts_by_date(2)
            yesterday_count = (
                list(yesterday_posts.values())[-2]
                if len(yesterday_posts.values()) >= 2
                else 0
            )
            daily_trend = (
                ((posts_today - yesterday_count) / yesterday_count * 100)
                if yesterday_count > 0
                else 0
            )

            kpi_data = {
                "total_posts": stats.get("analyzed_posts", 0),
                "positive_percentage": round(positive_pct, 1),
                "negative_percentage": round(negative_pct, 1),
                "neutral_percentage": round(neutral_pct, 1),
                "avg_confidence": avg_confidence,
                "posts_today": posts_today,
                "positive_trend": trends.get("positive_trend", 0.0),
                "negative_trend": trends.get("negative_trend", 0.0),
                "neutral_trend": trends.get("neutral_trend", 0.0),
                "confidence_trend": 0.0,  # TODO: Implement confidence trend calculation
                "daily_trend": round(daily_trend, 1),
            }

            self._set_cache_data(cache_key, kpi_data)
            return kpi_data

        except Exception as e:
            logger.error(f"Error getting KPI metrics: {e}")
            # Return default values
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
            db_ops = self.db_manager.db_ops
            with db_ops.db_connection.get_session() as session:

                result = (
                    session.query(RawPost, CleanedPost, SentimentAnalysis)
                    .join(CleanedPost, RawPost.id == CleanedPost.raw_post_id)
                    .join(
                        SentimentAnalysis,
                        CleanedPost.id == SentimentAnalysis.cleaned_post_id,
                    )
                    .order_by(RawPost.created_at.desc())
                    .limit(limit)
                    .all()
                )

                posts = []
                for raw_post, cleaned_post, sentiment in result:
                    posts.append(
                        {
                            "id": raw_post.id,
                            "text": (
                                raw_post.text[:200] + "..."
                                if len(raw_post.text) > 200
                                else raw_post.text
                            ),
                            "author": raw_post.author,
                            "created_at": raw_post.created_at,
                            "sentiment": sentiment.sentiment_label,
                            "confidence": round(sentiment.confidence_score * 100, 1),
                            "cleaned_text": (
                                cleaned_post.cleaned_text[:100] + "..."
                                if len(cleaned_post.cleaned_text) > 100
                                else cleaned_post.cleaned_text
                            ),
                        }
                    )

                return posts

        except Exception as e:
            logger.error(f"Error getting recent posts: {e}")
            return []


# Global data service instance
_data_service = None


def get_dashboard_data_service() -> DashboardDataService:
    """Get the global dashboard data service instance."""
    global _data_service
    if _data_service is None:
        _data_service = DashboardDataService()
    return _data_service


# Test functions for debugging
def test_data_service():
    """Test the data service functionality."""
    print("Testing Dashboard Data Service...")

    try:
        service = get_dashboard_data_service()

        print("\n1. Database Stats:")
        stats = service.get_database_stats()
        for key, value in stats.items():
            print(f"   {key}: {value}")

        print("\n2. Sentiment Distribution:")
        dist = service.get_sentiment_distribution()
        for sentiment, count in dist.items():
            print(f"   {sentiment}: {count}")

        print(f"\n3. Average Confidence: {service.get_average_confidence():.1f}%")

        print(f"\n4. Posts Today: {service.get_today_posts_count()}")

        print("\n5. KPI Metrics:")
        kpis = service.get_kpi_metrics()
        for key, value in kpis.items():
            print(f"   {key}: {value}")

        print("\n6. Recent Posts:")
        recent = service.get_recent_posts(5)
        for i, post in enumerate(recent, 1):
            print(
                f"   [{i}] {post['author']}: {post['text'][:50]}... ({post['sentiment']})"
            )

        print("\n✅ Data service test completed successfully!")

    except Exception as e:
        print(f"❌ Data service test failed: {e}")


if __name__ == "__main__":
    test_data_service()
