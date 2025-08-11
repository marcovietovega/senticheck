import os
import logging
from typing import List, Dict, Optional
from datetime import datetime
from dotenv import load_dotenv
from atproto import Client

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

load_dotenv()


class BlueskyConnector:
    """Connector for fetching posts from Bluesky using AT Protocol."""

    def __init__(self):
        """Initialize the Bluesky connector with environment variables."""
        self.handle = os.getenv("BLUESKY_HANDLE")
        self.app_password = os.getenv("BLUESKY_APP_PASSWORD")
        self.client = None

        if not self.handle or not self.app_password:
            raise ValueError(
                "BLUESKY_HANDLE and BLUESKY_APP_PASSWORD must be set in environment variables"
            )

    def connect(self) -> bool:
        """
        Establish connection to Bluesky.

        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            logger.info("Connecting to Bluesky...")
            self.client = Client()
            profile = self.client.login(self.handle, self.app_password)
            logger.info(f"Successfully connected as {profile.display_name}")
            return True
        except Exception as e:
            logger.error(
                "Failed to connect to Bluesky API - check credentials and network"
            )
            # For debugging, log the error type without sensitive details
            logger.debug(f"Connection error type: {type(e).__name__}")
            return False

    def fetch_posts(
        self, keyword: str = "AI", limit: int = 50, lang: str = "en"
    ) -> List[Dict]:
        """
        Fetch posts from Bluesky based on keyword search.

        Args:
            keyword (str): Search keyword
            limit (int): Maximum number of posts to fetch (1-100)
            lang (str): Language filter (e.g., 'en' for English)

        Returns:
            List[Dict]: List of post dictionaries with structured data
        """
        if not self.client:
            logger.error("Not connected to Bluesky. Call connect() first.")
            return []

        # Validate inputs
        if not keyword or not keyword.strip():
            logger.error("Keyword cannot be empty")
            return []

        if not isinstance(limit, int) or limit < 1 or limit > 100:
            logger.error("Limit must be an integer between 1 and 100")
            return []

        try:
            logger.info(
                f"Fetching posts with keyword: '{keyword}', limit: {limit}, language: {lang}"
            )

            params = {
                "limit": limit,
                "lang": lang,
                "q": keyword.strip(),
            }

            results = self.client.app.bsky.feed.search_posts(params)

            if not hasattr(results, "posts") or not results.posts:
                logger.warning("No posts found for the given search criteria")
                return []

            posts_data = []
            for post in results.posts:
                try:
                    post_data = self._extract_post_data(post)
                    if post_data:
                        posts_data.append(post_data)
                except Exception as e:
                    logger.warning(f"Failed to extract data from post: {e}")
                    continue

            logger.info(f"Successfully fetched {len(posts_data)} posts")
            return posts_data

        except Exception as e:
            logger.error(f"Failed to fetch posts: {e}")
            return []

    def _extract_post_data(self, post) -> Optional[Dict]:
        """
        Extract relevant data from a post object.

        Args:
            post: Raw post object from Bluesky API (PostView object)

        Returns:
            Dict: Structured post data or None if extraction fails
        """
        try:
            # Extract basic post information
            text = (
                post.record.text
                if hasattr(post, "record") and hasattr(post.record, "text")
                else ""
            )
            author = (
                post.author.display_name
                if hasattr(post, "author") and hasattr(post.author, "display_name") and post.author.display_name.strip()
                else "Unknown"
            )
            created_at = (
                post.record.created_at
                if hasattr(post, "record") and hasattr(post.record, "created_at")
                else ""
            )

            post_uri = post.uri if hasattr(post, "uri") else ""
            author_handle = (
                post.author.handle
                if hasattr(post, "author") and hasattr(post.author, "handle")
                else ""
            )
            cid = post.cid if hasattr(post, "cid") else ""

            # Parse timestamp
            timestamp = None
            if created_at:
                try:
                    timestamp = datetime.fromisoformat(
                        created_at.replace("Z", "+00:00")
                    )
                except ValueError:
                    logger.warning(f"Could not parse timestamp: {created_at}")

            langs = (
                post.record.langs
                if hasattr(post, "record") and hasattr(post.record, "langs")
                else ""
            )
            return {
                "text": text,
                "author": author,
                "author_handle": author_handle,
                "created_at": created_at,
                "timestamp": timestamp,
                "post_uri": post_uri,
                "cid": cid,
                "fetched_at": datetime.now(),
                "langs": langs,
            }

        except Exception as e:
            logger.error(f"Error extracting post data: {e}")
            return None

    def disconnect(self):
        """Clean up connection resources."""
        if self.client:
            self.client = None
            logger.info("Disconnected from Bluesky")
