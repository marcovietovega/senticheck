import logging
import time
from typing import List, Dict, Optional
from datetime import datetime
import threading

from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification

logger = logging.getLogger(__name__)

# Global model cache to prevent reloading
_model_cache = {}
_cache_lock = threading.Lock()


class SentimentAnalyzer:
    """Sentiment analysis utility for social media posts."""

    def __init__(
        self, model_name: str = "cardiffnlp/twitter-roberta-base-sentiment-latest"
    ):
        """Initialize the sentiment analyzer.

        Args:
            model_name: Hugging Face model name for sentiment analysis
        """

        self.model_name = model_name
        self.pipeline = None
        self.tokenizer = None
        self.model = None
        self.is_initialized = False

    @classmethod
    def get_cached_analyzer(
        cls, model_name: str = "cardiffnlp/twitter-roberta-base-sentiment-latest"
    ):
        """Get a cached analyzer instance to avoid reloading models.

        Args:
            model_name: Hugging Face model name for sentiment analysis

        Returns:
            Cached SentimentAnalyzer instance or None if initialization failed
        """
        with _cache_lock:
            if model_name not in _model_cache:
                logger.info(f"Creating new cached analyzer for model: {model_name}")
                analyzer = cls(model_name)
                if analyzer.initialize():
                    _model_cache[model_name] = analyzer
                    logger.info(f"Successfully cached analyzer for model: {model_name}")
                else:
                    logger.error(
                        f"Failed to initialize analyzer for model: {model_name}"
                    )
                    return None
            else:
                logger.debug(f"Using cached analyzer for model: {model_name}")

            return _model_cache.get(model_name)

    def initialize(self) -> bool:
        """Initialize the sentiment analysis model.

        Returns:
            True if initialization successful, False otherwise
        """
        try:
            logger.info(f"Initializing sentiment analysis model: {self.model_name}")

            self.pipeline = pipeline(
                "sentiment-analysis",
                model=self.model_name,
                top_k=None,
                device=-1,
            )

            self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
            self.model = AutoModelForSequenceClassification.from_pretrained(
                self.model_name
            )

            self.is_initialized = True
            logger.info("Sentiment analysis model initialized successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to initialize sentiment analysis model: {e}")
            self.is_initialized = False
            return False

    def analyze_text(self, text: str) -> Optional[Dict]:
        """Analyze sentiment of a single text.

        Args:
            text: Text to analyze

        Returns:
            Sentiment analysis result or None if failed
        """
        if not text or not text.strip():
            logger.warning("Empty text provided for sentiment analysis")
            return None

        try:
            tokens = self.tokenizer.encode(text, add_special_tokens=True)
            max_length = getattr(self.tokenizer, "model_max_length", 500)

            if len(tokens) > max_length:
                truncated_tokens = tokens[: max_length - 1]
                text = self.tokenizer.decode(truncated_tokens, skip_special_tokens=True)
                logger.debug(
                    f"Text truncated from {len(tokens)} to {len(truncated_tokens)} tokens"
                )

            results = self.pipeline(text)

            if not results or not isinstance(results, list) or not results[0]:
                logger.warning("No results from sentiment analysis")
                return None

            scores = results[0]

            best_prediction = max(scores, key=lambda x: x["score"])

            sentiment_result = {
                "sentiment_label": self._standardize_label(best_prediction["label"]),
                "confidence_score": round(best_prediction["score"], 4),
                "model_name": self.model_name,
                "model_version": getattr(self.model.config, "model_version", "unknown"),
                "analyzed_at": datetime.now(),
            }

            for score_item in scores:
                label = self._standardize_label(score_item["label"])
                sentiment_result[f"{label}_score"] = round(score_item["score"], 4)

            return sentiment_result

        except Exception as e:
            logger.error(f"Error analyzing sentiment for text: {e}")
            return None

    def _standardize_label(self, label: str) -> str:
        """Standardize sentiment labels across different models.

        Args:
            label: Original label from model

        Returns:
            Standardized label ('positive', 'negative', 'neutral')
        """
        label_lower = label.lower()

        if label_lower in ["positive", "pos", "label_2"]:
            return "positive"
        elif label_lower in ["negative", "neg", "label_0"]:
            return "negative"
        elif label_lower in ["neutral", "neu", "label_1"]:
            return "neutral"
        else:
            logger.warning(f"Unknown sentiment label: {label}")
            return label_lower

    def analyze_posts_batch(self, posts: List[Dict]) -> List[Dict]:
        """Analyze sentiment for a batch of posts.

        Args:
            posts: List of post dictionaries with 'text' field

        Returns:
            List of posts with sentiment analysis results
        """

        if not posts:
            logger.warning("No posts provided for sentiment analysis")
            return []

        analyzed_posts = []
        start_time = time.time()

        for i, post in enumerate(posts):
            try:
                text = post.get("text", "")
                if not text:
                    logger.warning(f"Post {i+1} has no text to analyze")
                    continue

                sentiment_result = self.analyze_text(text)

                if sentiment_result:
                    analyzed_post = post.copy()
                    analyzed_post["sentiment_analysis"] = sentiment_result
                    analyzed_posts.append(analyzed_post)

                    logger.debug(
                        f"Post {i+1}: {sentiment_result['sentiment_label']} "
                        f"(confidence: {sentiment_result['confidence_score']:.3f})"
                    )
                else:
                    logger.warning(f"Failed to analyze sentiment for post {i+1}")

            except Exception as e:
                logger.error(f"Error processing post {i+1}: {e}")
                continue

        processing_time = time.time() - start_time
        logger.info(
            f"Analyzed sentiment for {len(analyzed_posts)} out of {len(posts)} posts "
            f"in {processing_time:.2f} seconds"
        )

        return analyzed_posts


def analyze_sentiment_batch(
    posts: List[Dict],
    model_name: str = "cardiffnlp/twitter-roberta-base-sentiment-latest",
) -> List[Dict]:
    """Analyze sentiment for a batch of posts.

    Args:
        posts: List of post dictionaries
        model_name: Hugging Face model name

    Returns:
        List of posts with sentiment analysis results
    """
    analyzer = SentimentAnalyzer.get_cached_analyzer(model_name)

    if not analyzer:
        logger.error("Failed to get cached sentiment analyzer")
        return []

    try:
        return analyzer.analyze_posts_batch(posts)
    except Exception as e:
        logger.error(f"Batch sentiment analysis failed: {e}")
        return []
