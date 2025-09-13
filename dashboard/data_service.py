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
from dashboard.wordcloud_filters import get_stop_words

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

    def clear_cache(self):
        """Clear all cached data."""
        self.cache.clear()
        logger.info("Dashboard cache cleared")

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

    def get_sentiment_distribution(
        self, selected_keywords: Optional[List[str]] = None
    ) -> Dict[str, int]:
        """
        Get sentiment distribution counts.

        Args:
            selected_keywords: List of keywords to filter by, None for all keywords

        Returns:
            Dict with counts for each sentiment label
        """
        cache_key = f"sentiment_distribution_{selected_keywords or 'all'}"
        cached_data = self._get_cached_data(cache_key)
        if cached_data:
            return cached_data

        try:
            distribution = self.db_manager.get_sentiment_distribution_filtered(
                selected_keywords
            )
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

    def get_sentiment_over_time(
        self, days: int = 7, selected_keywords: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Get sentiment data over time for charting.

        Args:
            days: Number of days of historical data to return
            selected_keywords: List of keywords to filter by, None for all keywords

        Returns:
            DataFrame with columns: date, positive, negative, neutral
        """
        cache_key = f"sentiment_over_time_{days}_{selected_keywords or 'all'}"
        cached_data = self._get_cached_data(cache_key)
        if cached_data is not None:
            return cached_data

        try:
            # Get raw data from db_manager with keyword filtering
            raw_data = self.db_manager.get_sentiment_over_time_filtered(
                days, selected_keywords
            )

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

    def get_keyword_specific_kpis(self, selected_keyword: str) -> Dict[str, Any]:
        """
        Get enhanced KPI metrics focused on a specific keyword.

        Args:
            selected_keyword: Single keyword to analyze

        Returns:
            Dict with keyword-specific KPI values
        """
        cache_key = f"keyword_kpis_{selected_keyword}"
        cached_data = self._get_cached_data(cache_key)
        if cached_data:
            return cached_data

        try:
            kpi_data = self.db_manager.get_keyword_specific_kpis(selected_keyword)
            self._set_cache_data(cache_key, kpi_data)
            return kpi_data

        except Exception as e:
            logger.error(
                f"Error getting keyword-specific KPIs for {selected_keyword}: {e}"
            )
            return {
                "posts_this_week": 0,
                "week_trend": 0.0,
                "confidence_score": 0.0,
                "sentiment_momentum": "stable",
                "momentum_change": 0.0,
                "keyword_rank": 0,
                "total_keywords": 0,
                "daily_average": 0.0,
                "peak_sentiment": 0.0,
                "peak_date": None,
            }

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

    def get_keyword_insights(
        self, selected_keywords: Optional[List[str]], days: int = 7
    ) -> Dict[str, Any]:
        """
        Get keyword insights data with caching.

        Args:
            selected_keywords: List of keywords to analyze, None for all
            days: Number of days to analyze (7, 15, or 30)

        Returns:
            Dictionary with insights data
        """
        try:
            cache_key = f"keyword_insights_{selected_keywords or 'all'}_{days}"
            cached_data = self._get_cached_data(cache_key)
            if cached_data is not None:
                return cached_data

            insights_data = self.db_manager.get_keyword_insights(
                selected_keywords, days
            )

            self._set_cache_data(cache_key, insights_data)

            return insights_data

        except Exception as e:
            logger.error(f"Error getting keyword insights: {e}")
            return {
                "trend_analysis": {},
                "volume_stats": {},
                "performance_metrics": {},
                "activity_patterns": {},
            }

    def get_wordcloud_data(
        self, selected_keywords: List[str], days: int = 30
    ) -> Dict[str, Any]:
        """
        Get word frequency and sentiment data for word cloud generation.

        Args:
            selected_keywords: List of keywords (should be exactly one for word cloud)
            days: Number of days of historical data to analyze

        Returns:
            Dictionary with word frequencies, sentiment associations, and metadata
        """
        if not selected_keywords or len(selected_keywords) != 1:
            return {
                "word_frequencies": {},
                "word_sentiments": {},
                "total_posts": 0,
                "date_range": "",
            }

        keyword = selected_keywords[0]
        cache_key = f"wordcloud_data_{keyword}_{days}"
        cached_data = self._get_cached_data(cache_key)
        if cached_data is not None:
            return cached_data

        try:
            text_data = self.db_manager.get_text_analysis_for_keywords([keyword], days)

            if not text_data:
                empty_result = {
                    "word_frequencies": {},
                    "word_sentiments": {},
                    "total_posts": 0,
                    "date_range": "",
                }
                self._set_cache_data(cache_key, empty_result)
                return empty_result

            word_frequencies, word_sentiments = self._process_text_for_wordcloud(
                text_data
            )

            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=days - 1)
            date_range = (
                f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
            )

            result = {
                "word_frequencies": word_frequencies,
                "word_sentiments": word_sentiments,
                "total_posts": len(text_data),
                "date_range": date_range,
            }

            self._set_cache_data(cache_key, result)
            return result

        except Exception as e:
            logger.error(f"Error getting wordcloud data: {e}")
            return {
                "word_frequencies": {},
                "word_sentiments": {},
                "total_posts": 0,
                "date_range": "",
            }

    def get_wordcloud_stats(self, keyword: str, days: int = 30) -> Dict[str, Any]:
        """
        Get statistical insights for wordcloud analysis with sentiment-focused metrics.

        Args:
            keyword: The selected keyword
            days: Number of days of historical data to analyze

        Returns:
            Dictionary with sentiment-focused statistical insights about the wordcloud
        """
        cache_key = f"wordcloud_stats_{keyword}_{days}"
        cached_data = self._get_cached_data(cache_key)
        if cached_data is not None:
            return cached_data

        try:
            wc_data = self.get_wordcloud_data([keyword], days)

            if not wc_data["word_frequencies"]:
                return None

            word_frequencies = wc_data["word_frequencies"]
            word_sentiments = wc_data["word_sentiments"]

            sentiment_analysis = self._analyze_sentiment_words(
                word_frequencies, word_sentiments
            )

            unique_words = len(word_frequencies)
            total_posts = wc_data["total_posts"]

            most_positive = sentiment_analysis["most_positive_word"]
            most_negative = sentiment_analysis["most_negative_word"]
            most_neutral = sentiment_analysis["most_neutral_word"]

            result = {
                "unique_words": unique_words,
                "total_posts": total_posts,
                "date_range": wc_data["date_range"],
                "most_positive_word": most_positive[0] if most_positive else "None",
                "positive_sentiment_score": (
                    f"{most_positive[1]:.2f}" if most_positive else "0.00"
                ),
                "positive_frequency": most_positive[2] if most_positive else 0,
                "most_negative_word": most_negative[0] if most_negative else "None",
                "negative_sentiment_score": (
                    f"{most_negative[1]:.2f}" if most_negative else "0.00"
                ),
                "negative_frequency": most_negative[2] if most_negative else 0,
                "most_neutral_word": most_neutral[0] if most_neutral else "None",
                "neutral_sentiment_score": (
                    f"{most_neutral[1]:.2f}" if most_neutral else "0.50"
                ),
                "neutral_frequency": most_neutral[2] if most_neutral else 0,
                "emotional_intensity": sentiment_analysis["emotional_intensity"],
                "sentiment_ratio": sentiment_analysis["sentiment_ratio"],
                "trending_positive": [
                    word[0] for word in sentiment_analysis["trending_positive"]
                ],
                "trending_negative": [
                    word[0] for word in sentiment_analysis["trending_negative"]
                ],
            }

            self._set_cache_data(cache_key, result)
            return result

        except Exception as e:
            logger.error(f"Error getting wordcloud stats for {keyword}: {e}")
            return None

    def _process_text_for_wordcloud(self, text_data: List[Dict]) -> tuple:
        """
        Process text data to extract word frequencies and sentiment associations.

        Args:
            text_data: List of dictionaries with text and sentiment data

        Returns:
            Tuple of (word_frequencies, word_sentiments)
        """
        import re
        from collections import Counter

        stop_words = get_stop_words()

        all_words = []
        word_sentiment_data = {}

        for item in text_data:
            text = item.get("cleaned_text", "").lower()
            sentiment_score = float(item.get("sentiment_score", 0.5))

            words = re.findall(r"\b[a-zA-Z]{3,}\b", text)

            for word in words:
                if word not in stop_words and len(word) >= 3:
                    all_words.append(word)

                    if word not in word_sentiment_data:
                        word_sentiment_data[word] = []
                    word_sentiment_data[word].append(sentiment_score)

        word_frequencies = dict(Counter(all_words).most_common(100))

        word_sentiments = {}
        for word, sentiments in word_sentiment_data.items():
            if word in word_frequencies:
                word_sentiments[word] = sum(sentiments) / len(sentiments)

        return word_frequencies, word_sentiments

    def _analyze_sentiment_words(
        self, word_frequencies: Dict[str, int], word_sentiments: Dict[str, float]
    ) -> Dict[str, Any]:
        """
        Analyze words for sentiment insights and patterns.

        Args:
            word_frequencies: Dictionary of word frequencies
            word_sentiments: Dictionary of word sentiment scores (0-1, where 0.5 is neutral)

        Returns:
            Dictionary with sentiment analysis insights
        """
        if not word_frequencies or not word_sentiments:
            return {
                "most_positive_word": None,
                "most_negative_word": None,
                "most_neutral_word": None,
                "emotional_intensity": 0,
                "sentiment_ratio": 0,
                "trending_positive": [],
                "trending_negative": [],
            }

        positive_words = []
        negative_words = []
        neutral_words = []

        for word, sentiment in word_sentiments.items():
            if word in word_frequencies:
                freq = word_frequencies[word]
                if sentiment > 0.6:
                    positive_words.append((word, sentiment, freq))
                elif sentiment < 0.4:
                    negative_words.append((word, sentiment, freq))
                else:
                    distance_from_neutral = abs(sentiment - 0.5)
                    neutral_words.append((word, sentiment, freq, distance_from_neutral))

        positive_words.sort(key=lambda x: (x[1], x[2]), reverse=True)
        negative_words.sort(key=lambda x: (1 - x[1], x[2]), reverse=True)
        neutral_words.sort(key=lambda x: (x[3], -x[2]))

        sentiment_distances = [abs(s - 0.5) for s in word_sentiments.values()]
        emotional_intensity = (
            sum(sentiment_distances) / len(sentiment_distances)
            if sentiment_distances
            else 0
        )

        positive_count = len(positive_words)
        negative_count = len(negative_words)
        total_sentiment_words = positive_count + negative_count
        sentiment_ratio = (
            positive_count / total_sentiment_words if total_sentiment_words > 0 else 0.5
        )

        return {
            "most_positive_word": positive_words[0] if positive_words else None,
            "most_negative_word": negative_words[0] if negative_words else None,
            "most_neutral_word": neutral_words[0] if neutral_words else None,
            "emotional_intensity": emotional_intensity,
            "sentiment_ratio": sentiment_ratio,
            "trending_positive": positive_words[:3],
            "trending_negative": negative_words[:3],
        }


# Global data service instance
_data_service = None


def get_dashboard_data_service() -> DashboardDataService:
    """Get the global dashboard data service instance."""
    global _data_service
    if _data_service is None:
        _data_service = DashboardDataService()
    return _data_service
