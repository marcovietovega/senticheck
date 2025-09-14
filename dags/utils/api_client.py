"""API client for SentiCheck service interactions."""

import logging
import requests
from typing import Dict, Any, Optional
from airflow.sdk import Variable

logger = logging.getLogger(__name__)


class APIError(Exception):
    """Custom exception for API-related errors."""

    pass


class SentiCheckAPIClient:
    """Client for interacting with API service."""

    def __init__(self, base_url: Optional[str] = None):
        """Initialize API client.

        Args:
            base_url: Optional base URL. If not provided, uses default localhost.
        """
        if base_url:
            self.base_url = base_url
        else:
            try:
                self.base_url = Variable.get("api_service_url")
            except (KeyError, ImportError):
                self.base_url = "http://localhost:8000"

    def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict] = None,
        timeout: int = 30,
    ) -> Dict[str, Any]:
        """Make HTTP request with error handling.

        Args:
            method: HTTP method (get, post)
            endpoint: API endpoint path
            params: Request parameters
            timeout: Request timeout in seconds

        Returns:
            JSON response data

        Raises:
            APIError: If request fails
        """
        url = f"{self.base_url}{endpoint}"

        try:
            if method.lower() == "get":
                response = requests.get(url, params=params, timeout=timeout)
            elif method.lower() == "post":
                response = requests.post(url, params=params, timeout=timeout)
            else:
                raise APIError(f"Unsupported HTTP method: {method}")

            response.raise_for_status()
            return response.json()

        except requests.exceptions.Timeout:
            raise APIError(f"Request timeout after {timeout}s: {endpoint}")
        except requests.exceptions.ConnectionError:
            raise APIError(f"Connection error to API service: {endpoint}")
        except requests.exceptions.HTTPError as e:
            raise APIError(f"HTTP error {e.response.status_code}: {endpoint}")
        except requests.exceptions.RequestException as e:
            raise APIError(f"Request failed for {endpoint}: {str(e)}")
        except ValueError as e:
            raise APIError(f"Invalid JSON response from {endpoint}: {str(e)}")

    def health_check(self) -> Dict[str, Any]:
        """Check API service health status.

        Returns:
            Health status information
        """
        return self._make_request("get", "/health")

    def get_stats(self) -> Dict[str, Any]:
        """Get database statistics.

        Returns:
            Database statistics
        """
        return self._make_request("get", "/data/stats")

    def fetch_and_store_bluesky(self, keyword: str, lang: str = "en") -> Dict[str, Any]:
        """Fetch and store Bluesky posts for a keyword.

        Args:
            keyword: Search keyword
            lang: Language code

        Returns:
            Fetch and store results
        """
        return self._make_request(
            "post",
            "/connector/bluesky/fetch_and_store",
            params={"keyword": keyword, "lang": lang},
            timeout=300,
        )

    def process_raw_posts(self, limit: int = 1000) -> Dict[str, Any]:
        """Process raw posts through text cleaning pipeline.

        Args:
            limit: Batch size limit

        Returns:
            Processing results
        """
        return self._make_request(
            "post", "/pipeline/process_raw_posts", params={"limit": limit}, timeout=300
        )

    def analyze_sentiment(
        self,
        limit: int = 1000,
        model_name: str = "cardiffnlp/twitter-roberta-base-sentiment-latest",
    ) -> Dict[str, Any]:
        """Analyze sentiment of cleaned posts.

        Args:
            limit: Batch size limit
            model_name: Sentiment analysis model name

        Returns:
            Analysis results
        """
        return self._make_request(
            "post",
            "/pipeline/analyze_sentiment",
            params={"limit": limit, "model_name": model_name},
            timeout=600,
        )
