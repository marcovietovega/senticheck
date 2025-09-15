#!/usr/bin/env python3

import logging
from collections import defaultdict
from typing import List, Dict, Optional, Any
from datetime import datetime, timedelta, timezone
from sqlalchemy import func


from .db_operations import get_db_operations
from .database import RawPost, CleanedPost, SentimentAnalysis


logger = logging.getLogger(__name__)


class SentiCheckDBManager:
    """Database manager for SentiCheck sentiment analysis pipeline."""

    def __init__(self):
        self.db_ops = get_db_operations()

    def test_connection(self) -> bool:
        """Test database connection."""
        return self.db_ops.db_connection.test_connection()

    def create_tables(self):
        """Create all database tables."""
        self.db_ops.db_connection.create_tables()

    def get_database_stats(self) -> Dict[str, Any]:
        """Get database statistics.

        Returns:
            Dictionary containing database statistics
        """
        return self.db_ops.get_database_stats()

    def store_raw_posts(self, posts_data: List[Dict]) -> int:
        """Store raw posts from social media platforms.

        Args:
            posts_data: List of raw post data (each post should have search_keyword)

        Returns:
            Number of stored raw posts
        """
        return self.db_ops.store_raw_posts(posts_data)

    def get_unprocessed_posts(self) -> List[RawPost]:
        """
        Get raw posts that haven't been cleaned yet.

        Returns:
            List of unprocessed raw posts
        """
        return self.db_ops.get_unprocessed_posts()

    def store_cleaned_post(
        self,
        raw_post_id: int,
        cleaned_text: str,
        original_text: str,
        search_keyword: str,
        cleaning_metadata: Dict = None,
        preserve_hashtags: bool = False,
        preserve_mentions: bool = False,
    ) -> Optional[int]:
        """
        Store cleaned post data.

        Args:
            raw_post_id: ID of the raw post
            cleaned_text: Cleaned text of the post
            original_text: Original text of the post
            cleaning_metadata: Metadata related to the cleaning process
            preserve_hashtags: Whether to preserve hashtags
            preserve_mentions: Whether to preserve mentions

        Returns:
            ID of the stored cleaned post, or None if failed
        """
        return self.db_ops.store_cleaned_post(
            raw_post_id=raw_post_id,
            cleaned_text=cleaned_text,
            original_text=original_text,
            search_keyword=search_keyword,
            cleaning_metadata=cleaning_metadata or {},
            preserve_hashtags=preserve_hashtags,
            preserve_mentions=preserve_mentions,
        )

    def get_unanalyzed_posts(self, limit: int = 1000) -> List[CleanedPost]:
        """
        Get cleaned posts that haven't been analyzed for sentiment yet.

        Args:
            limit: Maximum number of posts to retrieve

        Returns:
            List of unanalyzed cleaned posts
        """
        return self.db_ops.get_unanalyzed_posts(limit)

    def store_sentiment_analysis(
        self,
        cleaned_post_id: int,
        sentiment_label: str,
        confidence_score: float,
        positive_score: float = None,
        negative_score: float = None,
        neutral_score: float = None,
        model_name: str = "unknown",
        model_version: str = None,
    ) -> Optional[int]:
        """
        Store sentiment analysis results for a single post.

        Args:
            cleaned_post_id: ID of the cleaned post
            sentiment_label: Predicted sentiment label
            confidence_score: Confidence score of the prediction
            positive_score: Positive sentiment score (optional)
            negative_score: Negative sentiment score (optional)
            neutral_score: Neutral sentiment score (optional)
            model_name: Name of the model used for prediction
            model_version: Version of the model used for prediction

        Returns:
            ID of the stored sentiment analysis result, or None if failed
        """
        return self.db_ops.store_sentiment_analysis(
            cleaned_post_id=cleaned_post_id,
            sentiment_label=sentiment_label,
            confidence_score=confidence_score,
            positive_score=positive_score,
            negative_score=negative_score,
            neutral_score=neutral_score,
            model_name=model_name,
            model_version=model_version,
        )

    def store_sentiment_analysis_batch(self, sentiment_results: List[Dict]) -> int:
        """
        Store multiple sentiment analysis results.

        Args:
            sentiment_results: List of sentiment analysis results to store

        Returns:
            Number of successfully stored results
        """
        return self.db_ops.store_sentiment_analysis_batch(sentiment_results)

    def get_sentiment_distribution(
        self, search_keyword: str = None, days: int = 30
    ) -> Dict[str, int]:
        """
        Get sentiment distribution counts from database.

        Returns:
            Dict[str, int]: A dictionary with sentiment labels as keys and their counts as values.
        """
        try:
            raw_results = self.db_ops.get_sentiment_distribution(search_keyword, days)

            distribution = {"positive": 0, "negative": 0, "neutral": 0}
            for sentiment_label, count in raw_results:
                if sentiment_label in distribution:
                    distribution[sentiment_label] = count

            return distribution
        except Exception as e:
            logger.error(f"Error getting sentiment distribution: {e}")
            return {"positive": 0, "negative": 0, "neutral": 0}

    def get_sentiment_over_time(
        self, search_keyword: str, days: int = 7
    ) -> List[Dict[str, Any]]:  # TODO: move to db_operations
        """
        Get sentiment counts by date for time series analysis.

        Args:
            days: Number of days of historical data to return

        Returns:
            List of dicts with date and sentiment counts
        """
        try:
            with self.db_ops.db_connection.get_session() as session:
                end_date = datetime.now().date()
                start_date = end_date - timedelta(days=days - 1)

                all_records = (
                    session.query(
                        func.date(SentimentAnalysis.analyzed_at).label("date"),
                        SentimentAnalysis.sentiment_label,
                    )
                    .filter(SentimentAnalysis.search_keyword == search_keyword)
                    .filter(func.date(SentimentAnalysis.analyzed_at) >= start_date)
                    .all()
                )

                date_sentiment_counts = defaultdict(
                    lambda: {"positive": 0, "negative": 0, "neutral": 0}
                )

                for record in all_records:
                    date_sentiment_counts[record.date][record.sentiment_label] += 1

                results = []
                for date, counts in date_sentiment_counts.items():
                    results.append(
                        type(
                            "Result",
                            (),
                            {
                                "date": date,
                                "positive": counts["positive"],
                                "negative": counts["negative"],
                                "neutral": counts["neutral"],
                            },
                        )
                    )

                    results.sort(key=lambda x: x.date)

                data = []
                for result in results:
                    data.append(
                        {
                            "date": result.date,
                            "positive": result.positive,
                            "negative": result.negative,
                            "neutral": result.neutral,
                        }
                    )

                return data

        except Exception as e:
            logger.error(f"Error getting sentiment over time: {e}")
            return []

    def get_sentiment_over_time_filtered(
        self, days: int = 7, selected_keywords: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:  # TODO: move to db_operations
        """
        Get sentiment counts by date for time series analysis, optionally filtered by keywords.

        Args:
            days: Number of days of historical data to return
            selected_keywords: Optional list of keywords to filter by. None for all keywords.

        Returns:
            List of dicts with date and sentiment counts
        """
        try:
            with self.db_ops.db_connection.get_session() as session:
                end_date = datetime.now().date()
                start_date = end_date - timedelta(days=days - 1)

                base_query = (
                    session.query(
                        func.date(SentimentAnalysis.analyzed_at).label("date"),
                        SentimentAnalysis.sentiment_label,
                        func.count(SentimentAnalysis.id).label("count"),
                    )
                    .filter(func.date(SentimentAnalysis.analyzed_at) >= start_date)
                    .filter(func.date(SentimentAnalysis.analyzed_at) <= end_date)
                )

                if selected_keywords is not None and selected_keywords:
                    base_query = base_query.filter(
                        SentimentAnalysis.search_keyword.in_(selected_keywords)
                    )

                results = base_query.group_by(
                    func.date(SentimentAnalysis.analyzed_at),
                    SentimentAnalysis.sentiment_label,
                ).all()

                data_dict = {}
                for result in results:
                    date_str = result.date.strftime("%Y-%m-%d")
                    if date_str not in data_dict:
                        data_dict[date_str] = {
                            "date": date_str,
                            "positive": 0,
                            "negative": 0,
                            "neutral": 0,
                        }

                    sentiment = result.sentiment_label.lower()
                    if sentiment in ["positive", "negative", "neutral"]:
                        data_dict[date_str][sentiment] = result.count

                    data = list(data_dict.values())
                data.sort(key=lambda x: x["date"])

                return data

        except Exception as e:
            logger.error(f"Error getting filtered sentiment over time: {e}")
            return []

    def calculate_sentiment_trends(self) -> Dict[str, float]:
        """
        Calculate sentiment trends compared to previous day .

        Returns:
            Dict with trend percentages for each sentiment
        """
        return self.db_ops.calculate_sentiment_trends()

    def get_average_confidence(self) -> float:
        """
        Get average confidence score across all analyzed posts.

        Returns:
            float: Average confidence score
        """
        try:
            return self.db_ops.get_average_confidence()
        except Exception as e:
            logger.error(f"Error getting average confidence: {e}")
            return 0.0

    def get_today_posts_count(self) -> int:
        """
        Get count of posts created today.

        Returns:
            int: Count of today's posts
        """
        try:
            return self.db_ops.get_today_posts_count()
        except Exception as e:
            logger.error(f"Error getting today's post count: {e}")
            return 0

    def get_posts_by_date(self, search_keyword: str, days: int = 7) -> Dict[str, int]:
        """
        Get post counts by date for the last N days.

        Args:
            search_keyword: Keyword to filter posts
            days: Number of days to look back

        Returns:
            Dictionary with dates as keys and post counts as values
        """
        try:
            raw_results = self.db_ops.get_posts_by_date_range(search_keyword, days)

            end_date = datetime.now(timezone.utc).date()
            start_date = end_date - timedelta(days=days)
            date_counts = {}
            current_date = start_date
            while current_date <= end_date:
                date_counts[current_date.strftime("%Y-%m-%d")] = 0
                current_date += timedelta(days=1)

            for date_str, count in raw_results:
                if date_str in date_counts:
                    date_counts[date_str] = count

            return date_counts
        except Exception as e:
            logger.error(f"Error getting posts by date: {e}")
            return {}

    def get_keywords_with_counts(self) -> List[tuple]:
        """
        Get all available keywords with their post counts.

        Returns:
            List of tuples (keyword, count)
        """
        try:
            return self.db_ops.get_keywords_with_counts()
        except Exception as e:
            logger.error(f"Error getting keywords with counts: {e}")
            return []

    def get_keyword_specific_metrics(self, keyword: str, days: int) -> Dict[str, Any]:
        """
        Get sentiment metrics for a specific keyword.

        Args:
            keyword: The keyword to analyze
            days: Number of days of historical data

        Returns:
            Dictionary with keyword-specific metrics
        """
        try:
            return self.db_ops.get_keyword_specific_metrics(keyword, days)
        except Exception as e:
            logger.error(f"Error getting keyword metrics for {keyword}: {e}")
            return {}

    def get_keyword_specific_kpis(
        self, selected_keyword: str, days: int
    ) -> Dict[str, Any]:
        """
        Get enhanced KPI metrics for a specific keyword.

        Args:
            selected_keyword: Single keyword to analyze
            days: Number of days of historical data

        Returns:
            Dictionary with keyword-specific KPI metrics
        """
        return self.db_ops.get_keyword_specific_kpis(selected_keyword, days)

    def get_text_analysis_for_keyword(
        self, selected_keyword: str, days: int
    ) -> List[Dict]:
        """
        Get text content and sentiment data for word cloud analysis.

        Args:
            selected_keyword: Keyword to analyze
            days: Number of days of historical data

        Returns:
            List of dictionaries with cleaned_text and sentiment_score
        """
        return self.db_ops.get_text_analysis_for_keyword(selected_keyword, days)


db_manager = None


def get_db_manager() -> SentiCheckDBManager:
    """Get the global database manager instance."""
    global db_manager
    if db_manager is None:
        db_manager = SentiCheckDBManager()
    return db_manager
