# ML Service package for SentiCheck sentiment analysis API

from .utils.sentiment_analyzer import SentimentAnalyzer, analyze_sentiment_batch

__all__ = [
    "SentimentAnalyzer",
    "analyze_sentiment_batch",
]
