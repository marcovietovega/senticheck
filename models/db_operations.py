import logging
import sys
import os
from typing import Optional, List, Dict, Any


if __name__ == "__main__":
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from .database import (
        RawPost,
        CleanedPost,
        SentimentAnalysis,
    )
    from .db_connection import get_db_connection
except ImportError:
    from database import (
        RawPost,
        CleanedPost,
        SentimentAnalysis,
    )
    from db_connection import get_db_connection


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
        Store raw posts in the database

        Args:
            posts_data: List of post dictionaries from Bluesky connector
            search_keyword: The keyword used to search for these posts

        Returns:
            int: Number of posts stored (excluding duplicates)
        """
        if not posts_data:
            logger.info("No posts to store")
            return 0

        # Try batch insert with PostgreSQL ON CONFLICT first
        try:
            return self._store_raw_posts_batch(posts_data, search_keyword)
        except Exception as e:
            logger.warning(
                f"Batch insert failed, falling back to individual inserts: {e}"
            )
            return self._store_raw_posts_individual(posts_data, search_keyword)

    def _store_raw_posts_batch(
        self, posts_data: List[Dict], search_keyword: str = None
    ) -> int:
        """
        Batch insert posts using PostgreSQL ON CONFLICT DO NOTHING.
        """
        from sqlalchemy.dialects.postgresql import insert

        stored_count = 0

        with self.db_connection.get_session() as session:
            # Prepare data for batch insert
            insert_data = []
            for post_data in posts_data:
                insert_data.append(
                    {
                        "post_uri": post_data.get("post_uri", ""),
                        "cid": post_data.get("cid", ""),
                        "text": post_data.get("text", ""),
                        "author": post_data.get("author") or "Unknown",
                        "author_handle": post_data.get("author_handle", ""),
                        "created_at": post_data.get("timestamp")
                        or post_data.get("fetched_at"),
                        "fetched_at": post_data.get("fetched_at"),
                        "search_keyword": search_keyword,
                        "is_processed": False,
                    }
                )

            # Use PostgreSQL's ON CONFLICT DO NOTHING to handle duplicates
            stmt = insert(RawPost).values(insert_data)
            stmt = stmt.on_conflict_do_nothing(index_elements=["post_uri"])

            result = session.execute(stmt)
            stored_count = result.rowcount

        logger.info(
            f"Batch stored {stored_count} new posts out of {len(posts_data)} total"
        )
        return stored_count

    def _store_raw_posts_individual(
        self, posts_data: List[Dict], search_keyword: str = None
    ) -> int:
        """
        Fallback method: Store posts individually with duplicate checking.
        """
        stored_count = 0
        skipped_count = 0

        for post_data in posts_data:
            try:
                with self.db_connection.get_session() as session:
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
                        skipped_count += 1
                        continue

                    # Create new post
                    raw_post = RawPost(
                        post_uri=post_data.get("post_uri", ""),
                        cid=post_data.get("cid", ""),
                        text=post_data.get("text", ""),
                        author=post_data.get("author") or "Unknown",
                        author_handle=post_data.get("author_handle", ""),
                        created_at=post_data.get("timestamp")
                        or post_data.get("fetched_at"),
                        fetched_at=post_data.get("fetched_at"),
                        search_keyword=search_keyword,
                    )

                    session.add(raw_post)
                    # Session commits automatically when exiting the context
                    stored_count += 1

            except Exception as e:
                logger.warning(
                    f"Failed to store post {post_data.get('post_uri', 'unknown')}: {e}"
                )
                continue

        logger.info(
            f"Individual stored {stored_count} new posts, skipped {skipped_count} duplicates out of {len(posts_data)} total"
        )
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
                session.flush()

                raw_post = session.query(RawPost).filter_by(id=raw_post_id).first()
                if raw_post:
                    raw_post.is_processed = True

                cleaned_post_id = cleaned_post.id

            logger.debug(f"Stored cleaned post with ID: {cleaned_post_id}")
            return cleaned_post_id

        except Exception as e:
            logger.error(f"Failed to store cleaned post: {e}")
            return None

    def get_unanalyzed_posts(self, limit: int = 100) -> List[CleanedPost]:
        """
        Get cleaned posts that haven't been analyzed for sentiment yet.

        Args:
            limit: Maximum number of posts to return

        Returns:
            List[CleanedPost]: List of unanalyzed cleaned posts
        """
        with self.db_connection.get_session() as session:
            posts = (
                session.query(CleanedPost)
                .filter_by(is_analyzed=False)
                .limit(limit)
                .all()
            )

            session.expunge_all()
            return posts

    def store_sentiment_analysis(
        self,
        cleaned_post_id: int,
        sentiment_label: str,
        confidence_score: float,
        positive_score: float = None,
        negative_score: float = None,
        neutral_score: float = None,
        model_name: str = "unknown",
        model_version: str = None,
    ) -> Optional[int]:
        """
        Store sentiment analysis results.

        Args:
            cleaned_post_id: ID of the cleaned post
            sentiment_label: The predicted sentiment ('positive', 'negative', 'neutral')
            confidence_score: Confidence score (0.0 to 1.0)
            positive_score: Positive sentiment score
            negative_score: Negative sentiment score
            neutral_score: Neutral sentiment score
            model_name: Name of the model used
            model_version: Version of the model

        Returns:
            int: ID of the created sentiment analysis, or None if failed
        """
        try:
            with self.db_connection.get_session() as session:
                sentiment_analysis = SentimentAnalysis(
                    cleaned_post_id=cleaned_post_id,
                    sentiment_label=sentiment_label,
                    confidence_score=confidence_score,
                    positive_score=positive_score,
                    negative_score=negative_score,
                    neutral_score=neutral_score,
                    model_name=model_name,
                    model_version=model_version,
                )

                session.add(sentiment_analysis)
                session.flush()

                cleaned_post = (
                    session.query(CleanedPost).filter_by(id=cleaned_post_id).first()
                )
                if cleaned_post:
                    cleaned_post.is_analyzed = True

                sentiment_analysis_id = sentiment_analysis.id

            logger.debug(f"Stored sentiment analysis with ID: {sentiment_analysis_id}")
            return sentiment_analysis_id

        except Exception as e:
            logger.error(f"Failed to store sentiment analysis: {e}")
            return None

    def store_sentiment_analysis_batch(self, sentiment_results: List[Dict]) -> int:
        """
        Store multiple sentiment analysis results in batch.

        Args:
            sentiment_results: List of sentiment analysis dictionaries with:
                - cleaned_post_id: int
                - sentiment_label: str
                - confidence_score: float
                - positive_score: float (optional)
                - negative_score: float (optional)
                - neutral_score: float (optional)
                - model_name: str
                - model_version: str (optional)

        Returns:
            int: Number of sentiment analyses stored
        """
        stored_count = 0

        with self.db_connection.get_session() as session:
            for result in sentiment_results:
                try:
                    sentiment_analysis = SentimentAnalysis(
                        cleaned_post_id=result["cleaned_post_id"],
                        sentiment_label=result["sentiment_label"],
                        confidence_score=result["confidence_score"],
                        positive_score=result.get("positive_score"),
                        negative_score=result.get("negative_score"),
                        neutral_score=result.get("neutral_score"),
                        model_name=result.get("model_name", "unknown"),
                        model_version=result.get("model_version"),
                    )

                    session.add(sentiment_analysis)

                    cleaned_post = (
                        session.query(CleanedPost)
                        .filter_by(id=result["cleaned_post_id"])
                        .first()
                    )
                    if cleaned_post:
                        cleaned_post.is_analyzed = True

                    stored_count += 1

                except Exception as e:
                    logger.error(
                        f"Failed to store sentiment analysis for post {result.get('cleaned_post_id')}: {e}"
                    )
                    continue

        logger.info(
            f"Stored {stored_count} sentiment analyses out of {len(sentiment_results)} total"
        )
        return stored_count

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


db_operations = None


def get_db_operations() -> DatabaseOperations:
    """Get the global database operations instance."""
    global db_operations
    if db_operations is None:
        db_operations = DatabaseOperations()
    return db_operations
