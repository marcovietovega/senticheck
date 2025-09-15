#!/usr/bin/env python3

import logging
import requests
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from collections import Counter
import re

from dashboard.config import DASHBOARD_CONFIG
from dashboard.wordcloud_filters import get_stop_words

logger = logging.getLogger(__name__)


class DashboardDataServiceAPI:

    def __init__(self, api_base_url: str = "http://localhost:8000"):
        self.api_base_url = api_base_url.rstrip("/")
        self.cache = {}
        self.cache_ttl = DASHBOARD_CONFIG["refresh"]["cache_ttl"]

    def _is_cache_valid(self, cache_key: str) -> bool:
        if cache_key not in self.cache:
            return False

        cached_time = self.cache[cache_key].get("timestamp")
        if not cached_time:
            return False

        return (datetime.now() - cached_time).seconds < self.cache_ttl

    def _get_cached_data(self, cache_key: str) -> Optional[Any]:
        if self._is_cache_valid(cache_key):
            return self.cache[cache_key]["data"]
        return None

    def _set_cache_data(self, cache_key: str, data: Any):
        self.cache[cache_key] = {"data": data, "timestamp": datetime.now()}

    def clear_cache(self):
        self.cache = {}

    def _api_call(
        self,
        endpoint: str,
        method: str = "GET",
        params: Dict = None,
        json_data: Dict = None,
    ) -> Any:
        try:
            url = f"{self.api_base_url}{endpoint}"

            if method == "GET":
                response = requests.get(url, params=params, timeout=30)
            elif method == "POST":
                response = requests.post(url, params=params, json=json_data, timeout=30)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            response.raise_for_status()
            return response.json()

        except requests.RequestException as e:
            logger.error(f"API call failed for {endpoint}: {e}")
            raise

    def get_database_stats(self) -> Dict[str, Any]:
        cache_key = "database_stats"
        cached_data = self._get_cached_data(cache_key)
        if cached_data:
            return cached_data

        try:
            data = self._api_call("/data/stats")
            self._set_cache_data(cache_key, data)
            return data
        except Exception as e:
            logger.error(f"Error getting database stats: {e}")
            return {}

    def get_sentiment_distribution(
        self, selected_keyword: str, days: int = 30
    ) -> Dict[str, Any]:
        cache_key = f"sentiment_distribution_{selected_keyword}_{days}"
        cached_data = self._get_cached_data(cache_key)
        if cached_data:
            return cached_data

        try:
            params = {"days": days}
            params["search_keyword"] = selected_keyword

            data = self._api_call("/data/sentiment/distribution", params=params)
            self._set_cache_data(cache_key, data)
            return data
        except Exception as e:
            logger.error(f"Error getting sentiment distribution: {e}")
            return {"positive": 0, "negative": 0, "neutral": 0}

    def get_average_confidence(
        self, search_keyword: str = None, days: int = 30
    ) -> float:
        try:
            params = {"days": days}
            if search_keyword:
                params["search_keyword"] = search_keyword

            data = self._api_call("/data/stats", params=params)
            return data.get("average_confidence", 0.0)
        except Exception as e:
            logger.error(f"Error getting average confidence: {e}")
            return 0.0

    def get_posts_by_date(
        self, date: datetime, search_keyword: str = None
    ) -> List[Dict]:
        try:
            params = {"date": date.isoformat(), "search_keyword": search_keyword}
            return self._api_call("/data/posts/by_date", params=params)
        except Exception as e:
            logger.error(f"Error getting posts by date: {e}")
            return []

    def get_today_posts_count(self, search_keyword: str = None) -> int:
        try:
            params = {}
            if search_keyword:
                params["search_keyword"] = search_keyword

            data = self._api_call("/data/stats", params=params)
            return data.get("today_posts", 0)
        except Exception as e:
            logger.error(f"Error getting today posts count: {e}")
            return 0

    def calculate_sentiment_trends(
        self, search_keyword: str = None, days: int = 7
    ) -> Dict[str, Any]:
        cache_key = f"sentiment_trends_{search_keyword}_{days}"
        cached_data = self._get_cached_data(cache_key)
        if cached_data:
            return cached_data

        try:
            params = {"days": days}
            if search_keyword:
                params["search_keyword"] = search_keyword

            data = self._api_call("/data/sentiment/trends", params=params)
            self._set_cache_data(cache_key, data)
            return data
        except Exception as e:
            logger.error(f"Error calculating sentiment trends: {e}")
            return {}

    def get_sentiment_over_time(self, days: int, selected_keyword: str) -> List[Dict]:
        cache_key = f"sentiment_over_time_{selected_keyword}_{days}"
        cached_data = self._get_cached_data(cache_key)
        if cached_data:
            return cached_data

        try:
            params = {"days": days}
            params["search_keyword"] = selected_keyword

            data = self._api_call("/data/sentiment/over_time", params=params)

            if data and isinstance(data, list):
                self._set_cache_data(cache_key, data)
                return data
            else:
                empty_list = []
                self._set_cache_data(cache_key, empty_list)
                return empty_list

        except Exception as e:
            logger.error(f"Error getting sentiment over time: {e}")
            return []

    def get_kpi_metrics(self, keyword: str, days: int = 30) -> Dict[str, Any]:
        cache_key = f"kpi_metrics_{keyword}_{days}"
        cached_data = self._get_cached_data(cache_key)
        if cached_data:
            return cached_data

        try:
            keyword_data = self._api_call(
                f"/data/metrics/keyword/{keyword}", params={"days": days}
            )

            if (
                "avg_confidence" in keyword_data
                and "confidence_score" not in keyword_data
            ):
                keyword_data["confidence_score"] = keyword_data["avg_confidence"]

            try:
                trends = self._api_call("/data/sentiment/trends")
                for trend_field in [
                    "positive_trend",
                    "negative_trend",
                    "neutral_trend",
                ]:
                    if trend_field not in keyword_data:
                        keyword_data[trend_field] = trends.get(trend_field, 0.0)
            except Exception as e:
                logger.warning(f"Could not get trends data: {e}")
                trend_defaults = {
                    "positive_trend": 0.0,
                    "negative_trend": 0.0,
                    "neutral_trend": 0.0,
                }
                for field, default in trend_defaults.items():
                    if field not in keyword_data:
                        keyword_data[field] = default

            if "daily_trend" not in keyword_data:
                try:
                    yesterday_posts = self._api_call(
                        "/data/posts/by_date", params={"days": 2}
                    )
                    posts_data = list(yesterday_posts.values())
                    yesterday_count = posts_data[0] if len(posts_data) >= 2 else 0
                    posts_today = keyword_data.get("posts_today", 0)

                    daily_trend = (
                        ((posts_today - yesterday_count) / yesterday_count * 100)
                        if yesterday_count > 0
                        else (100.0 if posts_today > 0 else 0.0)
                    )
                    keyword_data["daily_trend"] = round(daily_trend, 1)
                except Exception as e:
                    logger.warning(f"Could not calculate daily trend: {e}")
                    keyword_data["daily_trend"] = 0.0

            if "confidence_trend" not in keyword_data:
                keyword_data["confidence_trend"] = 0.0

            self._set_cache_data(cache_key, keyword_data)
            return keyword_data

        except Exception as e:
            logger.error(f"Error getting KPI metrics for keyword '{keyword}': {e}")
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
                "week_trend": 0.0,
                "sentiment_momentum": "stable",
                "momentum_change": 0.0,
                "keyword_rank": 1,
                "total_keywords": 3,
                "daily_average": 0.0,
                "peak_date": None,
                "peak_sentiment": 0.0,
                "posts_this_week": 0,
                "confidence_score": 0.0,
            }

    def get_available_keywords(self, days: int = 30) -> List[Dict]:
        cache_key = f"available_keywords_{days}"
        cached_data = self._get_cached_data(cache_key)
        if cached_data:
            return cached_data

        try:
            data = self._api_call("/data/keywords", params={"days": days})

            if isinstance(data, list) and data and isinstance(data[0], list):
                transformed_data = [
                    {"keyword": item[0], "count": item[1]} for item in data
                ]
                self._set_cache_data(cache_key, transformed_data)
                return transformed_data

            self._set_cache_data(cache_key, data)
            return data
        except Exception as e:
            logger.error(f"Error getting available keywords: {e}")
            return []

    def get_wordcloud_data(self, keyword: str, days: int = 30) -> Dict[str, Any]:
        cache_key = f"wordcloud_data_{keyword}_{days}"
        cached_data = self._get_cached_data(cache_key)
        if cached_data is not None:
            return cached_data

        try:
            text_data = self._api_call(
                "/data/text_analysis",
                method="POST",
                params={"keyword": keyword, "days": days},
            )

            word_frequencies = {}
            word_sentiments = {}

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
            return {"word_frequencies": {}, "word_sentiments": {}}

    def get_wordcloud_stats(self, keyword: str, days: int) -> Dict[str, Any]:
        cache_key = f"wordcloud_stats_{keyword}_{days}"
        cached_data = self._get_cached_data(cache_key)
        if cached_data is not None:
            return cached_data
        try:
            wordcloud_data = self.get_wordcloud_data(keyword, days)

            if not wordcloud_data["word_frequencies"]:
                return None

            word_frequencies = wordcloud_data["word_frequencies"]
            word_sentiments = wordcloud_data["word_sentiments"]

            sentiment_analysis = self._analyze_sentiment_words(
                word_frequencies, word_sentiments
            )

            most_positive = sentiment_analysis["most_positive_word"]
            most_negative = sentiment_analysis["most_negative_word"]
            most_neutral = sentiment_analysis["most_neutral_word"]

            result = {
                "most_positive_word": most_positive[0] if most_positive else "None",
                "positive_sentiment_score": (
                    f"{most_positive[1]:.2f}" if most_positive else "0.00"
                ),
                "positive_frequency": most_positive[2] if most_positive else 0,
                "most_neutral_word": most_neutral[0] if most_neutral else "None",
                "neutral_sentiment_score": (
                    f"{most_neutral[1]:.2f}" if most_neutral else "0.50"
                ),
                "neutral_frequency": most_neutral[2] if most_neutral else 0,
                "most_negative_word": most_negative[0] if most_negative else "None",
                "negative_sentiment_score": (
                    f"{most_negative[1]:.2f}" if most_negative else "0.00"
                ),
                "negative_frequency": most_negative[2] if most_negative else 0,
            }

            self._set_cache_data(cache_key, result)
            return result
        except Exception as e:
            logger.error(f"Error getting wordcloud stats: {e}")
            return {}

    def _process_text_for_wordcloud(self, text_data: List[Dict]) -> tuple:
        """
        Process text data to extract word frequencies and sentiment associations.

        Args:
            text_data: List of dictionaries with text and sentiment data

        Returns:
            Tuple of (word_frequencies, word_sentiments)
        """

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

        return {
            "most_positive_word": positive_words[0] if positive_words else None,
            "most_negative_word": negative_words[0] if negative_words else None,
            "most_neutral_word": neutral_words[0] if neutral_words else None,
        }


_data_service = None


def get_dashboard_data_service() -> DashboardDataServiceAPI:
    global _data_service
    if _data_service is None:
        _data_service = DashboardDataServiceAPI()
    return _data_service
