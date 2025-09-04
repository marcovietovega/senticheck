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

    def get_unprocessed_posts(self, limit: Optional[int] = 100) -> List[RawPost]:
        """
        Get raw posts that haven't been cleaned yet.

        Args:
            limit: Maximum number of posts to retrieve

        Returns:
            List of unprocessed raw posts
        """
        return self.db_ops.get_unprocessed_posts(limit)

    def store_cleaned_post(
        self,
        raw_post_id: int,
        cleaned_text: str,
        original_text: str,
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
            cleaning_metadata=cleaning_metadata or {},
            preserve_hashtags=preserve_hashtags,
            preserve_mentions=preserve_mentions,
        )

    def get_unanalyzed_posts(self, limit: Optional[int] = 100) -> List[CleanedPost]:
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

    def process_raw_posts_to_cleaned(
        self,
        preserve_hashtags: bool = False,
        preserve_mentions: bool = False,
        filter_hashtag_only: bool = True,
        min_content_words: int = 3,
        limit: Optional[int] = 100,
    ) -> int:
        """
        Process raw posts through text cleaning pipeline.

        Args:
            preserve_hashtags: Whether to keep hashtags during cleaning
            preserve_mentions: Whether to keep mentions during cleaning
            filter_hashtag_only: Whether to filter posts that are mostly hashtags
            min_content_words: Minimum number of content words required
            limit: Maximum number of posts to process. If None, processes all unprocessed posts.

        Returns:
            int: Number of posts processed
        """
        try:
            from scripts.text_cleaner import TextCleaner

            raw_posts = self.get_unprocessed_posts(limit)
            if not raw_posts:
                logger.info("No unprocessed posts found")
                return 0

            cleaner = TextCleaner()
            processed_count = 0
            filtered_count = 0

            for raw_post in raw_posts:
                try:
                    post_data = {
                        "id": raw_post.id,
                        "text": raw_post.text,
                        "author": raw_post.author,
                        "created_at": (
                            raw_post.created_at.isoformat()
                            if raw_post.created_at
                            else None
                        ),
                    }

                    cleaned_post = cleaner.clean_post(
                        post_data,
                        preserve_hashtags=preserve_hashtags,
                        preserve_mentions=preserve_mentions,
                        filter_hashtag_only=filter_hashtag_only,
                        min_content_words=min_content_words,
                    )

                    # Check if post was filtered out
                    if cleaned_post is None:
                        filtered_count += 1
                        # Mark the raw post as processed even if filtered
                        with self.db_ops.db_connection.get_session() as session:
                            raw_post_obj = (
                                session.query(RawPost).filter_by(id=raw_post.id).first()
                            )
                            if raw_post_obj:
                                raw_post_obj.is_processed = True
                        continue

                    if cleaned_post.get("text", "").strip():
                        # Add metadata
                        cleaning_metadata = {
                            "cleaned_at": datetime.now().isoformat(),
                            "filter_hashtag_only": filter_hashtag_only,
                            "min_content_words": min_content_words,
                        }

                        # Include content analysis
                        if "content_analysis" in cleaned_post:
                            cleaning_metadata["content_analysis"] = cleaned_post[
                                "content_analysis"
                            ]

                        self.store_cleaned_post(
                            raw_post_id=raw_post.id,
                            cleaned_text=cleaned_post["text"],
                            original_text=cleaned_post["original_text"],
                            cleaning_metadata=cleaning_metadata,
                            preserve_hashtags=preserve_hashtags,
                            preserve_mentions=preserve_mentions,
                        )
                        processed_count += 1
                    else:
                        logger.warning(
                            f"Post {raw_post.id} has no content after cleaning"
                        )

                except Exception as e:
                    logger.error(f"Failed to process raw post {raw_post.id}: {e}")
                    continue

            logger.info(f"Processed {processed_count} raw posts to cleaned posts")
            if filtered_count > 0:
                logger.info(
                    f"Filtered out {filtered_count} hashtag-only or low-content posts"
                )
            return processed_count

        except ImportError:
            logger.error("Text cleaner module not available")
            return 0
        except Exception as e:
            logger.error(f"Failed to process raw posts: {e}")
            return 0

    def analyze_cleaned_posts_sentiment(
        self,
        model_name: str = "cardiffnlp/twitter-roberta-base-sentiment-latest",
        limit: int = 100,
    ) -> int:
        """
        Analyze sentiment for cleaned posts that haven't been analyzed yet.

        Args:
            model_name: Hugging Face model name for sentiment analysis
            limit: Maximum number of posts to analyze

        Returns:
            int: Number of posts analyzed
        """
        try:
            from scripts.sentiment_analyzer import SentimentAnalyzer

            cleaned_posts = self.get_unanalyzed_posts(limit)
            if not cleaned_posts:
                logger.info("No unanalyzed posts found")
                return 0

            analyzer = SentimentAnalyzer(model_name)
            if not analyzer.initialize():
                logger.error("Failed to initialize sentiment analyzer")
                return 0

            analyzed_count = 0
            sentiment_results = []

            for cleaned_post in cleaned_posts:
                try:

                    sentiment_result = analyzer.analyze_text(cleaned_post.cleaned_text)

                    if sentiment_result:

                        sentiment_results.append(
                            {
                                "cleaned_post_id": cleaned_post.id,
                                "sentiment_label": sentiment_result["sentiment_label"],
                                "confidence_score": sentiment_result[
                                    "confidence_score"
                                ],
                                "positive_score": sentiment_result.get(
                                    "positive_score"
                                ),
                                "negative_score": sentiment_result.get(
                                    "negative_score"
                                ),
                                "neutral_score": sentiment_result.get("neutral_score"),
                                "model_name": sentiment_result["model_name"],
                                "model_version": sentiment_result.get("model_version"),
                            }
                        )
                        analyzed_count += 1
                    else:
                        logger.warning(
                            f"Failed to analyze sentiment for cleaned post {cleaned_post.id}"
                        )

                except Exception as e:
                    logger.error(
                        f"Failed to analyze cleaned post {cleaned_post.id}: {e}"
                    )
                    continue

            if sentiment_results:
                stored_count = self.store_sentiment_analysis_batch(sentiment_results)
                logger.info(f"Analyzed and stored sentiment for {stored_count} posts")
                return stored_count
            else:
                logger.warning("No sentiment results to store")
                return 0

        except ImportError:
            logger.error(
                "Sentiment analyzer module not available - install transformers library"
            )
            return 0
        except Exception as e:
            logger.error(f"Failed to analyze sentiment: {e}")
            return 0

    def get_sentiment_distribution(self) -> Dict[str, int]:
        """
        Get sentiment distribution counts from database.

        Returns:
            Dict[str, int]: A dictionary with sentiment labels as keys and their counts as values.
        """
        try:
            raw_results = self.db_ops.get_sentiment_distribution()

            distribution = {"positive": 0, "negative": 0, "neutral": 0}
            for sentiment_label, count in raw_results:
                if sentiment_label in distribution:
                    distribution[sentiment_label] = count

            return distribution
        except Exception as e:
            logger.error(f"Error getting sentiment distribution: {e}")
            return {"positive": 0, "negative": 0, "neutral": 0}

    def get_sentiment_over_time(self, days: int = 7) -> List[Dict[str, Any]]:
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

                # Get all records and group in Python
                all_records = (
                    session.query(
                        func.date(SentimentAnalysis.analyzed_at).label("date"),
                        SentimentAnalysis.sentiment_label,
                    )
                    .filter(func.date(SentimentAnalysis.analyzed_at) >= start_date)
                    .filter(func.date(SentimentAnalysis.analyzed_at) <= end_date)
                    .all()
                )

                # Group data by date and sentiment
                date_sentiment_counts = defaultdict(
                    lambda: {"positive": 0, "negative": 0, "neutral": 0}
                )

                for record in all_records:
                    date_sentiment_counts[record.date][record.sentiment_label] += 1

                # Convert to expected format
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

                # Sort by date
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

    def calculate_sentiment_trends(self) -> Dict[str, float]:
        """
        Calculate sentiment trends compared to previous day.

        Returns:
            Dict with trend percentages for each sentiment
        """
        try:

            today = datetime.now(timezone.utc).date()
            yesterday = today - timedelta(days=1)

            with self.db_ops.db_connection.get_session() as session:
                # Get today's sentiment counts
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

                # Get yesterday's sentiment counts
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

                today_counts = {sentiment: count for sentiment, count in today_result}
                yesterday_counts = {
                    sentiment: count for sentiment, count in yesterday_result
                }

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

    def get_recent_posts_with_analysis(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get recent posts with their sentiment analysis results.

        Args:
            limit: Maximum number of posts to return

        Returns:
            List of post dictionaries with sentiment data
        """
        try:

            with self.db_ops.db_connection.get_session() as session:
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
                            "text": raw_post.text,
                            "author": raw_post.author,
                            "created_at": raw_post.created_at,
                            "sentiment": sentiment.sentiment_label,
                            "confidence": sentiment.confidence_score,
                            "cleaned_text": cleaned_post.cleaned_text,
                        }
                    )

                return posts

        except Exception as e:
            logger.error(f"Error getting recent posts: {e}")
            return []

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

    def get_posts_by_date(self, days: int = 7) -> Dict[str, int]:
        """
        Get post counts by date for the last N days.

        Args:
            days: Number of days to look back

        Returns:
            Dictionary with dates as keys and post counts as values
        """
        try:
            raw_results = self.db_ops.get_posts_by_date_range(days)

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


db_manager = None


def get_db_manager() -> SentiCheckDBManager:
    """Get the global database manager instance."""
    global db_manager
    if db_manager is None:
        db_manager = SentiCheckDBManager()
    return db_manager
