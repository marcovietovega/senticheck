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
        self,
        keyword: str = "AI",
        lang: str = "en",
    ) -> List[Dict]:
        """
        Fetch recent posts from Bluesky (no date filtering).

        Args:
            keyword (str): Search keyword
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

        try:
            all_posts_data = []
            cursor = None
            page_count = 0

            logger.info(
                f"Fetching recent posts - Keyword: '{keyword}', Language: {lang}, Per page: 100"
            )

            while True:  # Continue until no more data
                page_count += 1
                page_limit = 100  # Always use 100 (API max)

                params = {
                    "limit": page_limit,
                    "lang": lang,
                    "q": keyword.strip(),
                }

                # Add cursor for pagination (except first request)
                if cursor:
                    params["cursor"] = cursor

                logger.info(f"Fetching page {page_count} (limit: {page_limit})")
                logger.debug(f"API params: {params}")

                results = self.client.app.bsky.feed.search_posts(params)

                if not hasattr(results, "posts") or not results.posts:
                    logger.info(f"No more posts found (page {page_count})")
                    break

                # Process posts from this page - API filtering is working, no client-side needed
                page_posts = []
                for post in results.posts:
                    try:
                        post_data = self._extract_post_data(post)
                        if post_data:
                            page_posts.append(post_data)
                    except Exception as e:
                        logger.warning(f"Failed to extract data from post: {e}")
                        continue

                all_posts_data.extend(page_posts)
                logger.info(
                    f"Page {page_count}: fetched {len(page_posts)} posts (total: {len(all_posts_data)})"
                )

                # Check if we have a cursor for the next page
                if hasattr(results, "cursor") and results.cursor:
                    cursor = results.cursor
                    logger.info(f"Got cursor for next page: {cursor[:20]}.")
                else:
                    logger.info("No more pages available (no cursor returned)")
                    break

                # If we got fewer posts than requested, we've likely reached the end
                if len(results.posts) < page_limit:
                    logger.info("Reached end of available posts")
                    break

                # Limit to reasonable number of pages for 30-minute runs
                if page_count >= 5:  # Max ~500 posts per run
                    logger.info(
                        f"Reached page limit for regular collection ({page_count} pages)"
                    )
                    break

            logger.info(
                f"Successfully fetched {len(all_posts_data)} recent posts across {page_count} pages"
            )
            return all_posts_data

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
                post.author.display_name.strip()
                if hasattr(post, "author")
                and hasattr(post.author, "display_name")
                and post.author.display_name
                and post.author.display_name.strip()
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
