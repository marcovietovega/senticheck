"""
HTTP Client for SentiCheck Sentiment Analysis Service

Client for communicating with the FastAPI sentiment analysis service.
Provides a clean interface for HTTP requests to analyze sentiment.
"""

import httpx
import logging
import time
from typing import List, Dict, Any, Optional

try:
    from .config import config
except ImportError:
    import sys
    import os

    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from config import config

logger = logging.getLogger(__name__)


class SentimentAnalysisClient:
    """Client for communicating with the SentiCheck Sentiment Analysis API.

    Provides methods to check service health, analyze texts, and get model information.
    Handles HTTP errors, retries, and provides a clean interface for Airflow tasks.
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        timeout: Optional[float] = None,
        max_retries: Optional[int] = None,
        retry_delay: Optional[float] = None,
    ):
        """
        Initialize the sentiment analysis client.

        Args:
            base_url: Base URL of the sentiment analysis service (uses config default if None)
            timeout: Request timeout in seconds (uses config default if None)
            max_retries: Maximum number of retry attempts (uses config default if None)
            retry_delay: Delay between retries in seconds (uses config default if None)
        """
        self.base_url = (base_url or config.get_service_url()).rstrip("/")
        self.timeout = timeout or config.timeout
        self.max_retries = max_retries or config.max_retries
        self.retry_delay = retry_delay or config.retry_delay

        # HTTP client configuration
        self.client = httpx.Client(
            timeout=httpx.Timeout(timeout), follow_redirects=True
        )

        logger.info(
            f"SentimentAnalysisClient initialized with base_URL: {self.base_url}"
        )

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup resources."""
        self.close()

    def close(self):
        """Close the HTTP client connection."""
        if hasattr(self, "client"):
            self.client.close()

    def _make_request_with_retry(
        self, method: str, endpoint: str, **kwargs
    ) -> httpx.Response:
        """
        Make HTTP request with retry logic.

        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint path
            **kwargs: Additional arguments for the HTTP request

        Returns:
            httpx.Response: HTTP response object

        Raises:
            httpx.HTTPError: If all retries fail
        """
        url = f"{self.base_url}{endpoint}"

        for attempt in range(self.max_retries + 1):
            try:
                logger.debug(
                    f"Making {method} request to {url} (attempt {attempt + 1})"
                )

                response = self.client.request(method, url, **kwargs)
                response.raise_for_status()
                return response

            except httpx.RequestError as e:
                logger.warning(f"Request failed (attempt {attempt + 1}): {e}")
                if attempt == self.max_retries:
                    raise
                time.sleep(self.retry_delay * (attempt + 1))  # Exponential backoff

            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP error {e.response.status_code}: {e.response.text}")
                if attempt == self.max_retries or e.response.status_code < 500:
                    # Don't retry for client errors (4xx), only server errors (5xx)
                    raise
                time.sleep(self.retry_delay * (attempt + 1))

    def check_health(self) -> Dict[str, Any]:
        """
        Check the health status of the sentiment analysis service.

        Returns:
            Dict: Service health information

        Raises:
            Exception: If service is not healthy or unreachable
        """
        try:
            logger.info("Checking sentiment analysis service health...")

            response = self._make_request_with_retry("GET", "/health")
            health_data = response.json()

            if health_data.get("status") != "healthy":
                raise Exception(f"Service is not healthy: {health_data}")

            logger.info(
                f"✅ Service is healthy - Model: {health_data.get('model_name', 'Unknown')}"
            )
            return health_data

        except Exception as e:
            logger.error(f"❌ Health check failed: {e}")
            raise Exception(f"Sentiment analysis service is not available: {e}")

    def analyze_cleaned_posts(
        self, cleaned_posts: List[Any], model_name: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Analyze sentiment for a list of cleaned post objects.

        This method is specifically designed to work with the CleanedPost database objects
        from the SentiCheck system.

        Args:
            cleaned_posts: List of CleanedPost objects with cleaned_text attribute
            model_name: Optional model name to use for analysis

        Returns:
            List[Dict]: List of sentiment analysis results ready for database storage

        Raises:
            Exception: If analysis fails
        """
        try:
            logger.info(
                f"Processing {len(cleaned_posts)} cleaned posts for sentiment analysis..."
            )

            if not cleaned_posts:
                logger.warning("No cleaned posts provided for analysis")
                return []

            # Prepare texts for batch analysis
            batch_texts = []
            post_id_mapping = {}

            for post in cleaned_posts:
                if hasattr(post, "cleaned_text") and hasattr(post, "id"):
                    text_id = f"post_{post.id}"
                    batch_texts.append({"text": post.cleaned_text, "id": text_id})
                    post_id_mapping[text_id] = post.id
                else:
                    logger.warning(f"Skipping invalid post object: {post}")

            if not batch_texts:
                logger.warning("No valid texts found in cleaned posts")
                return []

            # Perform batch analysis
            batch_result = self.analyze_batch_texts(batch_texts, model_name)

            # Convert results to database-ready format
            sentiment_results = []
            for result in batch_result.get("results", []):
                text_id = result.get("text_id")
                if text_id and text_id in post_id_mapping:
                    cleaned_post_id = post_id_mapping[text_id]

                    sentiment_results.append(
                        {
                            "cleaned_post_id": cleaned_post_id,
                            "sentiment_label": result["sentiment_label"],
                            "confidence_score": result["confidence_score"],
                            "positive_score": result.get("positive_score"),
                            "negative_score": result.get("negative_score"),
                            "neutral_score": result.get("neutral_score"),
                            "model_name": result["model_name"],
                            "model_version": result.get("model_version"),
                        }
                    )

            logger.info(f"✅ Processed {len(sentiment_results)} posts successfully")
            return sentiment_results

        except Exception as e:
            logger.error(f"❌ Failed to analyze cleaned posts: {e}")
            raise Exception(f"Cleaned posts analysis failed: {e}")


# Convenience function for quick usage
def create_sentiment_client(
    service_url: Optional[str] = None, timeout: Optional[float] = None
) -> SentimentAnalysisClient:
    """
    Create a sentiment analysis client with configuration-based defaults.

    Args:
        service_url: URL of the sentiment analysis service (uses .env config if None)
        timeout: Request timeout in seconds (uses .env config if None)

    Returns:
        SentimentAnalysisClient: Configured client instance
    """
    return SentimentAnalysisClient(base_url=service_url, timeout=timeout)
