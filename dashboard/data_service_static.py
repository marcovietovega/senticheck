#!/usr/bin/env python3

import json
import logging
import re
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from collections import Counter

from wordcloud_filters import get_stop_words

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent / "data"


class DashboardDataServiceStatic:

    def __init__(self):
        self.data_dir = DATA_DIR
        self._cache = {}
        self.metadata = self._load_json("metadata.json")
        self.reference_date = self.metadata.get("reference_date", "2025-12-25")

    def _load_json(self, filename: str) -> Any:
        if filename in self._cache:
            return self._cache[filename]

        filepath = self.data_dir / filename
        try:
            with open(filepath, "r") as f:
                data = json.load(f)
            self._cache[filename] = data
            return data
        except FileNotFoundError:
            logger.warning(f"Data file not found: {filepath}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing {filepath}: {e}")
            return None

    def _get_key(self, keyword: str, days: int) -> str:
        return f"{keyword.lower()}_{days}d"

    def get_sentiment_distribution(
        self, selected_keyword: str, days: int = 30
    ) -> Dict[str, Any]:
        key = self._get_key(selected_keyword, days)
        data = self._load_json(f"distribution_{key}.json")
        if data:
            return data
        return {"positive": 0, "negative": 0, "neutral": 0}

    def get_sentiment_over_time(self, days: int, selected_keyword: str) -> List[Dict]:
        key = self._get_key(selected_keyword, days)
        data = self._load_json(f"over_time_{key}.json")
        if data and isinstance(data, list):
            return data
        return []

    def get_kpi_metrics(self, keyword: str, days: int = 30) -> Dict[str, Any]:
        key = self._get_key(keyword, days)
        data = self._load_json(f"metrics_{key}.json")
        if data:
            return data
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
        data = self._load_json("keywords.json")
        if data:
            return [{"keyword": item[0], "count": item[1]} for item in data]
        return []

    def get_wordcloud_data(self, keyword: str, days: int = 30) -> Dict[str, Any]:
        key = self._get_key(keyword, days)
        text_data = self._load_json(f"text_analysis_{key}.json")

        if not text_data:
            return {"word_frequencies": {}, "word_sentiments": {}}

        word_frequencies, word_sentiments = self._process_text_for_wordcloud(text_data)

        ref = datetime.strptime(self.reference_date, "%Y-%m-%d").date()
        start_date = ref - timedelta(days=days - 1)
        date_range = f"{start_date.strftime('%Y-%m-%d')} to {ref.strftime('%Y-%m-%d')}"

        return {
            "word_frequencies": word_frequencies,
            "word_sentiments": word_sentiments,
            "total_posts": len(text_data),
            "date_range": date_range,
        }

    def get_wordcloud_stats(self, keyword: str, days: int) -> Optional[Dict[str, Any]]:
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

        return {
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

    def _process_text_for_wordcloud(self, text_data: List[Dict]) -> tuple:
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


def get_dashboard_data_service() -> DashboardDataServiceStatic:
    global _data_service
    if _data_service is None:
        _data_service = DashboardDataServiceStatic()
    return _data_service
