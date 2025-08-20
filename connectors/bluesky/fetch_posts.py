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

    def __init__(self, posts_per_page: int = 100):
        """Initialize the Bluesky connector with environment variables."""
        self.handle = os.getenv("BLUESKY_HANDLE")
        self.app_password = os.getenv("BLUESKY_APP_PASSWORD")
        self.client = None
        self.posts_per_page = posts_per_page  # Default to 100 for efficiency

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
        self, keyword: str = "AI", lang: str = "en", max_posts: Optional[int] = None
    ) -> List[Dict]:
        """
        Fetch posts from Bluesky based on keyword search with pagination support.

        Args:
            keyword (str): Search keyword
            lang (str): Language filter (e.g., 'en' for English)
            max_posts (Optional[int]): Maximum total posts to fetch across all pages.
                                     If None, defaults to posts_per_page (single page).

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

        # If max_posts not specified, default to one page
        if max_posts is None:
            max_posts = self.posts_per_page

        try:
            all_posts_data = []
            cursor = None
            page_count = 0

            logger.info(
                f"Fetching posts - Keyword: '{keyword}', Language: {lang}, Target: {max_posts}, Per page: {self.posts_per_page}"
            )

            while len(all_posts_data) < max_posts:
                page_count += 1
                # Calculate remaining posts needed for this page
                remaining_posts = max_posts - len(all_posts_data)
                page_limit = min(
                    self.posts_per_page, remaining_posts, 100
                )  # API max is 100

                params = {
                    "limit": page_limit,
                    "lang": lang,
                    "q": keyword.strip(),
                }

                # Add cursor for pagination (except first request)
                if cursor:
                    params["cursor"] = cursor

                logger.info(f"Fetching page {page_count} (limit: {page_limit})")
                results = self.client.app.bsky.feed.search_posts(params)

                if not hasattr(results, "posts") or not results.posts:
                    logger.info(f"No more posts found (page {page_count})")
                    break

                # Process posts from this page
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
                    logger.debug(f"Got cursor for next page: {cursor[:20]}...")
                else:
                    logger.info("No more pages available (no cursor returned)")
                    break

                # If we got fewer posts than requested, we've likely reached the end
                if len(results.posts) < page_limit:
                    logger.info("Reached end of available posts")
                    break

            logger.info(
                f"Successfully fetched {len(all_posts_data)} posts across {page_count} pages"
            )
            return all_posts_data

        except Exception as e:
            logger.error(f"Failed to fetch posts: {e}")
            return []

    def fetch_many_posts(
        self,
        keyword: str = "AI",
        total_posts: int = 200,
        lang: str = "en",
        page_size: int = 100,
    ) -> List[Dict]:
        """
        Convenience method to fetch a large number of posts using pagination.

        Args:
            keyword (str): Search keyword
            total_posts (int): Total number of posts to fetch across all pages
            lang (str): Language filter (e.g., 'en' for English)
            page_size (int): Number of posts per API call (1-100, default 100 for efficiency)

        Returns:
            List[Dict]: List of post dictionaries with structured data

        Example:
            # Fetch 500 posts about AI
            posts = connector.fetch_many_posts("AI", total_posts=500)
        """
        return self.fetch_posts(
            keyword=keyword, limit=page_size, lang=lang, max_posts=total_posts
        )

    def fetch_posts_with_airflow_config(
        self, keyword: str = "AI", lang: str = "en", **context
    ) -> List[Dict]:
        """
        Fetch posts using Airflow context and variables for configuration.

        This method is designed to be used in Airflow DAGs where you can
        set variables to control the total number of posts to fetch.

        Args:
            keyword (str): Search keyword
            lang (str): Language filter
            **context: Airflow context (automatically passed in DAG tasks)

        Returns:
            List[Dict]: List of post dictionaries

        Expected Airflow Variables:
            - bluesky_posts_total: Total posts to fetch (default: 200)
            - bluesky_posts_per_page: Posts per API call (default: 100)

        Example in DAG:
            fetch_task = PythonOperator(
                task_id='fetch_bluesky_posts',
                python_callable=connector.fetch_posts_with_airflow_config,
                op_kwargs={'keyword': 'AI', 'lang': 'en'}
            )
        """
        try:
            from airflow.models import Variable

            # Get configuration from Airflow variables with defaults
            total_posts = int(Variable.get("bluesky_posts_total", default_var=200))
            posts_per_page = int(
                Variable.get("bluesky_posts_per_page", default_var=100)
            )

            logger.info(
                f"Airflow config - Total: {total_posts}, Per page: {posts_per_page}"
            )

            return self.fetch_posts(
                keyword=keyword, limit=posts_per_page, lang=lang, max_posts=total_posts
            )

        except ImportError:
            # Fallback if not running in Airflow context
            logger.warning("Not running in Airflow context, using default values")
            return self.fetch_posts(keyword=keyword, lang=lang, max_posts=200)

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
                if hasattr(post, "author")
                and hasattr(post.author, "display_name")
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
