import logging
import time
from typing import List, Dict, Optional, Tuple
from datetime import datetime

from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification

logger = logging.getLogger(__name__)


class SentimentAnalyzer:
    """Sentiment analysis utility for social media posts."""

    def __init__(
        self, model_name: str = "cardiffnlp/twitter-roberta-base-sentiment-latest"
    ):
        """
        Initialize the sentiment analyzer.

        Args:
            model_name (str): Hugging Face model name for sentiment analysis
        """

        self.model_name = model_name
        self.pipeline = None
        self.tokenizer = None
        self.model = None
        self.is_initialized = False

    def initialize(self) -> bool:
        """
        Initialize the sentiment analysis model.

        Returns:
            bool: True if initialization successful, False otherwise
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
        """
        Analyze sentiment of a single text.

        Args:
            text (str): Text to analyze

        Returns:
            Dict: Sentiment analysis result or None if failed
        """
        if not text or not text.strip():
            logger.warning("Empty text provided for sentiment analysis")
            return None

        try:
            tokens = self.tokenizer.encode(text, add_special_tokens=True)
            max_length = getattr(self.tokenizer, "model_max_length", 500)

            if len(tokens) > max_length:
                truncated_tokens = tokens[: max_length - 1]  # -1 for end token
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
        """
        Standardize sentiment labels across different models.

        Args:
            label (str): Original label from model

        Returns:
            str: Standardized label ('positive', 'negative', 'neutral')
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
        """
        Analyze sentiment for a batch of posts.

        Args:
            posts (List[Dict]): List of post dictionaries with 'text' field

        Returns:
            List[Dict]: List of posts with sentiment analysis results
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

    def get_model_info(self) -> Dict:
        """
        Get information about the loaded model.

        Returns:
            Dict: Model information
        """
        if not self.is_initialized:
            return {"error": "Model not initialized"}

        try:
            return {
                "model_name": self.model_name,
                "model_type": type(self.model).__name__,
                "tokenizer_type": type(self.tokenizer).__name__,
                "max_length": getattr(self.tokenizer, "model_max_length", "unknown"),
                "vocab_size": getattr(self.tokenizer, "vocab_size", "unknown"),
            }
        except Exception as e:
            logger.error(f"Error getting model info: {e}")
            return {"error": str(e)}


def analyze_sentiment_batch(
    posts: List[Dict],
    model_name: str = "cardiffnlp/twitter-roberta-base-sentiment-latest",
) -> List[Dict]:
    """
    Function to analyze sentiment for a batch of posts.

    Args:
        posts (List[Dict]): List of post dictionaries
        model_name (str): Hugging Face model name

    Returns:
        List[Dict]: List of posts with sentiment analysis results
    """
    analyzer = SentimentAnalyzer(model_name)

    if not analyzer.initialize():
        logger.error("Failed to initialize sentiment analyzer")
        return []

    try:
        return analyzer.analyze_posts_batch(posts)
    except Exception as e:
        logger.error(f"Batch sentiment analysis failed: {e}")
        return []


if __name__ == "__main__":
    import sys
    import os

    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    from connectors.bluesky.fetch_posts import fetch_bluesky_posts
    from scripts.text_cleaner import clean_bluesky_posts

    print("Fetching sample posts...")
    posts = fetch_bluesky_posts("AI", 3)

    if posts:
        print("\nCleaning posts...")
        cleaned_posts = clean_bluesky_posts(posts)

        print("\nAnalyzing sentiment...")
        analyzed_posts = analyze_sentiment_batch(cleaned_posts)

        print(f"\nSENTIMENT ANALYSIS RESULTS:")
        print("=" * 60)

        for i, post in enumerate(analyzed_posts, 1):
            sentiment = post.get("sentiment_analysis", {})
            print(f"\n[{i}] Text: {post.get('text', '')[:100]}...")
            print(f"    Sentiment: {sentiment.get('sentiment_label', 'N/A')}")
            print(f"    Confidence: {sentiment.get('confidence_score', 'N/A'):.3f}")

            for label in ["positive", "negative", "neutral"]:
                score_key = f"{label}_score"
                if score_key in sentiment:
                    print(f"    {label.title()}: {sentiment[score_key]:.3f}")

    else:
        print("No posts found to analyze.")
