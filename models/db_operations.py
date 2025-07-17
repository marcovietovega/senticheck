import logging
from typing import Optional, List, Dict, Any

from .database import (
    RawPost,
    CleanedPost,
    SentimentAnalysis,
    ProcessingLog,
)
from .db_connection import get_db_connection

# Configure logging
logger = logging.getLogger(__name__)


class DatabaseOperations:
    """Handles data operations for SentiCheck."""

    def __init__(self):
        """Initialize database operations with connection."""
        self.db_connection = get_db_connection()

    def store_raw_posts(
        self, posts_data: List[Dict], search_keyword: str = None
    ) -> int:
        """
        Store raw posts in the database.

        Args:
            posts_data: List of post dictionaries from Bluesky connector
            search_keyword: The keyword used to search for these posts

        Returns:
            int: Number of posts stored (excluding duplicates)
        """
        stored_count = 0

        with self.db_connection.get_session() as session:
            for post_data in posts_data:
                try:
                    # Check if post already exists
                    existing_post = (
                        session.query(RawPost)
                        .filter_by(post_uri=post_data.get("post_uri", ""))
                        .first()
                    )

                    if existing_post:
                        logger.debug(
                            f"Post already exists: {post_data.get('post_uri', '')}"
                        )
                        continue

                    # Create new raw post
                    raw_post = RawPost(
                        post_uri=post_data.get("post_uri", ""),
                        cid=post_data.get("cid", ""),
                        text=post_data.get("text", ""),
                        author=post_data.get("author", ""),
                        author_handle=post_data.get("author_handle", ""),
                        created_at=post_data.get("timestamp")
                        or post_data.get("fetched_at"),
                        fetched_at=post_data.get("fetched_at"),
                        search_keyword=search_keyword,
                    )

                    session.add(raw_post)
                    stored_count += 1

                except Exception as e:
                    logger.error(f"Failed to store post: {e}")
                    continue

        logger.info(f"Stored {stored_count} new posts out of {len(posts_data)} total")
        return stored_count

    def get_unprocessed_posts(self, limit: int = 100) -> List[RawPost]:
        """
        Get raw posts that haven't been cleaned yet.

        Args:
            limit: Maximum number of posts to return

        Returns:
            List[RawPost]: List of unprocessed raw posts
        """
        with self.db_connection.get_session() as session:
            posts = (
                session.query(RawPost).filter_by(is_processed=False).limit(limit).all()
            )
            # Detach from session to avoid lazy loading issues
            session.expunge_all()
            return posts

    def store_cleaned_post(
        self,
        raw_post_id: int,
        cleaned_text: str,
        original_text: str,
        cleaning_metadata: Dict,
        preserve_hashtags: bool = False,
        preserve_mentions: bool = False,
    ) -> Optional[int]:
        """
        Store cleaned post data.

        Args:
            raw_post_id: ID of the raw post
            cleaned_text: The cleaned text
            original_text: The original text
            cleaning_metadata: Metadata about the cleaning process
            preserve_hashtags: Whether hashtags were preserved
            preserve_mentions: Whether mentions were preserved

        Returns:
            int: ID of the created cleaned post, or None if failed
        """
        try:
            with self.db_connection.get_session() as session:
                cleaned_post = CleanedPost(
                    raw_post_id=raw_post_id,
                    cleaned_text=cleaned_text,
                    original_text=original_text,
                    cleaning_metadata=cleaning_metadata,
                    preserve_hashtags=preserve_hashtags,
                    preserve_mentions=preserve_mentions,
                )

                session.add(cleaned_post)
                session.flush()  # Get the ID without committing

                # Mark raw post as processed
                raw_post = session.query(RawPost).filter_by(id=raw_post_id).first()
                if raw_post:
                    raw_post.is_processed = True

                cleaned_post_id = cleaned_post.id

            logger.debug(f"Stored cleaned post with ID: {cleaned_post_id}")
            return cleaned_post_id

        except Exception as e:
            logger.error(f"Failed to store cleaned post: {e}")
            return None

    def log_processing_activity(
        self,
        process_type: str,
        status: str,
        message: str = None,
        records_processed: int = 0,
        processing_time: float = None,
        error_details: Dict = None,
        processing_metadata: Dict = None,
    ):
        """
        Log processing activity.

        Args:
            process_type: Type of process ('fetch', 'clean', 'analyze')
            status: Status ('success', 'error', 'warning')
            message: Optional message
            records_processed: Number of records processed
            processing_time: Processing time in seconds
            error_details: Error details if any
            processing_metadata: Additional metadata
        """
        try:
            with self.db_connection.get_session() as session:
                log_entry = ProcessingLog(
                    process_type=process_type,
                    status=status,
                    message=message,
                    records_processed=records_processed,
                    processing_time_seconds=processing_time,
                    error_details=error_details,
                    processing_metadata=processing_metadata,
                )
                session.add(log_entry)

        except Exception as e:
            logger.error(f"Failed to log processing activity: {e}")

    def get_database_stats(self) -> Dict[str, Any]:
        """
        Get database statistics.

        Returns:
            Dict: Database statistics
        """
        try:
            with self.db_connection.get_session() as session:
                raw_posts_count = session.query(RawPost).count()
                cleaned_posts_count = session.query(CleanedPost).count()
                analyzed_posts_count = session.query(SentimentAnalysis).count()

                unprocessed_posts = (
                    session.query(RawPost).filter_by(is_processed=False).count()
                )
                unanalyzed_posts = (
                    session.query(CleanedPost).filter_by(is_analyzed=False).count()
                )

                return {
                    "raw_posts": raw_posts_count,
                    "cleaned_posts": cleaned_posts_count,
                    "analyzed_posts": analyzed_posts_count,
                    "unprocessed_posts": unprocessed_posts,
                    "unanalyzed_posts": unanalyzed_posts,
                }

        except Exception as e:
            logger.error(f"Failed to get database stats: {e}")
            return {}


# Global database operations instance
db_operations = None


def get_db_operations() -> DatabaseOperations:
    """Get the global database operations instance."""
    global db_operations
    if db_operations is None:
        db_operations = DatabaseOperations()
    return db_operations
