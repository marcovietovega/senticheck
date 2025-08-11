#!/usr/bin/env python3
"""
Database manager for SentiCheck with sentiment analysis support.

This module provides a high-level interface for managing the complete
sentiment analysis pipeline including data storage and retrieval.
"""

import logging
import sys
import os
from typing import List, Dict, Optional, Any
from datetime import datetime


if __name__ == "__main__":
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from .db_operations import get_db_operations
    from .database import RawPost, CleanedPost, SentimentAnalysis
except ImportError:
    from db_operations import get_db_operations
    from database import RawPost, CleanedPost, SentimentAnalysis

logger = logging.getLogger(__name__)


class SentiCheckDBManager:
    """
    High-level database manager for SentiCheck sentiment analysis pipeline.

    This class provides convenient methods for managing the complete workflow
    from raw posts to sentiment analysis results.
    """

    def __init__(self):
        """Initialize the database manager."""
        self.db_ops = get_db_operations()

    def test_connection(self) -> bool:
        """Test database connection."""
        return self.db_ops.db_connection.test_connection()

    def create_tables(self):
        """Create all database tables."""
        self.db_ops.db_connection.create_tables()

    def get_database_stats(self) -> Dict[str, Any]:
        """Get comprehensive database statistics."""
        return self.db_ops.get_database_stats()

    def store_raw_posts(
        self, posts_data: List[Dict], search_keyword: str = None
    ) -> int:
        """Store raw posts from social media platforms."""
        return self.db_ops.store_raw_posts(posts_data, search_keyword)

    def get_unprocessed_posts(self, limit: int = 100) -> List[RawPost]:
        """Get raw posts that haven't been cleaned yet."""
        return self.db_ops.get_unprocessed_posts(limit)

    def store_cleaned_post(
        self,
        raw_post_id: int,
        cleaned_text: str,
        original_text: str,
        cleaning_metadata: Dict = None,
        preserve_hashtags: bool = False,
        preserve_mentions: bool = False,
    ) -> Optional[int]:
        """Store cleaned post data."""
        return self.db_ops.store_cleaned_post(
            raw_post_id=raw_post_id,
            cleaned_text=cleaned_text,
            original_text=original_text,
            cleaning_metadata=cleaning_metadata or {},
            preserve_hashtags=preserve_hashtags,
            preserve_mentions=preserve_mentions,
        )

    def get_unanalyzed_posts(self, limit: int = 100) -> List[CleanedPost]:
        """Get cleaned posts that haven't been analyzed for sentiment yet."""
        return self.db_ops.get_unanalyzed_posts(limit)

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
        """Store sentiment analysis results for a single post."""
        return self.db_ops.store_sentiment_analysis(
            cleaned_post_id=cleaned_post_id,
            sentiment_label=sentiment_label,
            confidence_score=confidence_score,
            positive_score=positive_score,
            negative_score=negative_score,
            neutral_score=neutral_score,
            model_name=model_name,
            model_version=model_version,
        )

    def store_sentiment_analysis_batch(self, sentiment_results: List[Dict]) -> int:
        """Store multiple sentiment analysis results."""
        return self.db_ops.store_sentiment_analysis_batch(sentiment_results)

    def process_raw_posts_to_cleaned(
        self,
        preserve_hashtags: bool = False,
        preserve_mentions: bool = False,
        limit: int = 100,
    ) -> int:
        """
        Process raw posts through text cleaning pipeline.

        Args:
            preserve_hashtags: Whether to keep hashtags during cleaning
            preserve_mentions: Whether to keep mentions during cleaning
            limit: Maximum number of posts to process

        Returns:
            int: Number of posts processed
        """
        try:
            from scripts.text_cleaner import TextCleaner

            raw_posts = self.get_unprocessed_posts(limit)
            if not raw_posts:
                logger.info("No unprocessed posts found")
                return 0

            cleaner = TextCleaner()
            processed_count = 0

            for raw_post in raw_posts:
                try:
                    post_data = {
                        "text": raw_post.text,
                        "author": raw_post.author,
                        "created_at": (
                            raw_post.created_at.isoformat()
                            if raw_post.created_at
                            else None
                        ),
                    }

                    cleaned_post = cleaner.clean_post(
                        post_data,
                        preserve_hashtags=preserve_hashtags,
                        preserve_mentions=preserve_mentions,
                    )

                    if cleaned_post.get("text", "").strip():
                        self.store_cleaned_post(
                            raw_post_id=raw_post.id,
                            cleaned_text=cleaned_post["text"],
                            original_text=cleaned_post["original_text"],
                            cleaning_metadata={
                                "cleaned_at": datetime.now().isoformat()
                            },
                            preserve_hashtags=preserve_hashtags,
                            preserve_mentions=preserve_mentions,
                        )
                        processed_count += 1
                    else:
                        logger.warning(
                            f"Post {raw_post.id} has no content after cleaning"
                        )

                except Exception as e:
                    logger.error(f"Failed to process raw post {raw_post.id}: {e}")
                    continue

            logger.info(f"Processed {processed_count} raw posts to cleaned posts")
            return processed_count

        except ImportError:
            logger.error("Text cleaner module not available")
            return 0
        except Exception as e:
            logger.error(f"Failed to process raw posts: {e}")
            return 0

    def analyze_cleaned_posts_sentiment(
        self,
        model_name: str = "cardiffnlp/twitter-roberta-base-sentiment-latest",
        limit: int = 100,
    ) -> int:
        """
        Analyze sentiment for cleaned posts that haven't been analyzed yet.

        Args:
            model_name: Hugging Face model name for sentiment analysis
            limit: Maximum number of posts to analyze

        Returns:
            int: Number of posts analyzed
        """
        try:
            from scripts.sentiment_analyzer import SentimentAnalyzer

            cleaned_posts = self.get_unanalyzed_posts(limit)
            if not cleaned_posts:
                logger.info("No unanalyzed posts found")
                return 0

            analyzer = SentimentAnalyzer(model_name)
            if not analyzer.initialize():
                logger.error("Failed to initialize sentiment analyzer")
                return 0

            analyzed_count = 0
            sentiment_results = []

            for cleaned_post in cleaned_posts:
                try:

                    sentiment_result = analyzer.analyze_text(cleaned_post.cleaned_text)

                    if sentiment_result:

                        sentiment_results.append(
                            {
                                "cleaned_post_id": cleaned_post.id,
                                "sentiment_label": sentiment_result["sentiment_label"],
                                "confidence_score": sentiment_result[
                                    "confidence_score"
                                ],
                                "positive_score": sentiment_result.get(
                                    "positive_score"
                                ),
                                "negative_score": sentiment_result.get(
                                    "negative_score"
                                ),
                                "neutral_score": sentiment_result.get("neutral_score"),
                                "model_name": sentiment_result["model_name"],
                                "model_version": sentiment_result.get("model_version"),
                            }
                        )
                        analyzed_count += 1
                    else:
                        logger.warning(
                            f"Failed to analyze sentiment for cleaned post {cleaned_post.id}"
                        )

                except Exception as e:
                    logger.error(
                        f"Failed to analyze cleaned post {cleaned_post.id}: {e}"
                    )
                    continue

            if sentiment_results:
                stored_count = self.store_sentiment_analysis_batch(sentiment_results)
                logger.info(f"Analyzed and stored sentiment for {stored_count} posts")
                return stored_count
            else:
                logger.warning("No sentiment results to store")
                return 0

        except ImportError:
            logger.error(
                "Sentiment analyzer module not available - install transformers library"
            )
            return 0
        except Exception as e:
            logger.error(f"Failed to analyze sentiment: {e}")
            return 0

    def run_full_pipeline(
        self,
        posts_data: List[Dict],
        search_keyword: str = None,
        preserve_hashtags: bool = False,
        preserve_mentions: bool = False,
        model_name: str = "cardiffnlp/twitter-roberta-base-sentiment-latest",
    ) -> Dict[str, int]:
        """
        Run the complete pipeline from raw posts to sentiment analysis.

        Args:
            posts_data: List of raw post dictionaries
            search_keyword: Keyword used to search for posts
            preserve_hashtags: Whether to keep hashtags during cleaning
            preserve_mentions: Whether to keep mentions during cleaning
            model_name: Sentiment analysis model name

        Returns:
            Dict[str, int]: Results summary with counts
        """
        results = {
            "raw_posts_stored": 0,
            "posts_cleaned": 0,
            "posts_analyzed": 0,
            "errors": 0,
        }

        try:
            logger.info("Starting full sentiment analysis pipeline...")

            logger.info("Step 1: Storing raw posts...")
            results["raw_posts_stored"] = self.store_raw_posts(
                posts_data, search_keyword
            )

            logger.info("Step 2: Cleaning posts...")
            results["posts_cleaned"] = self.process_raw_posts_to_cleaned(
                preserve_hashtags=preserve_hashtags,
                preserve_mentions=preserve_mentions,
                limit=len(posts_data),
            )

            logger.info("Step 3: Analyzing sentiment...")
            results["posts_analyzed"] = self.analyze_cleaned_posts_sentiment(
                model_name=model_name, limit=results["posts_cleaned"]
            )

            logger.info("Full pipeline completed successfully")
            return results

        except Exception as e:
            logger.error(f"Full pipeline failed: {e}")
            results["errors"] = 1
            return results

    def log_processing_activity(
        self,
        process_type: str,
        status: str,
        message: str = None,
        records_processed: int = 0,
        processing_time: float = None,
        error_details: Dict = None,
    ):
        """Log processing activity."""
        self.db_ops.log_processing_activity(
            process_type=process_type,
            status=status,
            message=message,
            records_processed=records_processed,
            processing_time=processing_time,
            error_details=error_details,
        )


db_manager = None


def get_db_manager() -> SentiCheckDBManager:
    """Get the global database manager instance."""
    global db_manager
    if db_manager is None:
        db_manager = SentiCheckDBManager()
    return db_manager


def main():
    """
    Main function for testing and managing the SentiCheck database.
    Provides an interactive interface for various database operations.
    """
    print("=" * 60)
    print("SENTICHECK DATABASE MANAGER")
    print("=" * 60)

    try:
        # Initialize manager
        manager = get_db_manager()

        # Test connection
        print("\n1. Testing database connection...")
        if not manager.test_connection():
            print("❌ Database connection failed!")
            print("Please check your .env file and database configuration.")
            return
        print("✅ Database connection successful")

        # Show current stats
        print("\n2. Current database statistics:")
        stats = manager.get_database_stats()
        for key, value in stats.items():
            print(f"   {key.replace('_', ' ').title()}: {value}")

        # Interactive menu
        while True:
            print("\n" + "=" * 60)
            print("DATABASE OPERATIONS MENU")
            print("=" * 60)
            print("1. Show database statistics")
            print("2. Process raw posts to cleaned")
            print("3. Analyze sentiment for cleaned posts")
            print("4. Run full pipeline test")
            print("5. Create/recreate database tables")
            print("6. Show unprocessed posts count")
            print("7. Show unanalyzed posts count")
            print("8. Exit")

            choice = input("\nEnter your choice (1-8): ").strip()

            if choice == "1":
                print("\nDatabase Statistics:")
                stats = manager.get_database_stats()
                for key, value in stats.items():
                    print(f"  {key.replace('_', ' ').title()}: {value}")

            elif choice == "2":
                limit = int(input("Enter max posts to process (default 10): ") or "10")
                print(f"\nProcessing up to {limit} raw posts...")
                processed = manager.process_raw_posts_to_cleaned(limit=limit)
                print(f"✅ Processed {processed} posts to cleaned format")

            elif choice == "3":
                limit = int(input("Enter max posts to analyze (default 10): ") or "10")
                model = input(
                    "Model name (default: cardiffnlp/twitter-roberta-base-sentiment-latest): "
                ).strip()
                if not model:
                    model = "cardiffnlp/twitter-roberta-base-sentiment-latest"
                print(f"\nAnalyzing sentiment for up to {limit} posts...")
                analyzed = manager.analyze_cleaned_posts_sentiment(
                    model_name=model, limit=limit
                )
                print(f"✅ Analyzed sentiment for {analyzed} posts")

            elif choice == "4":
                print("\nRunning full pipeline test...")
                try:
                    from connectors.bluesky.fetch_posts import fetch_bluesky_posts

                    keyword = input("Search keyword (default: AI): ").strip() or "AI"
                    limit = int(input("Number of posts to fetch (default: 3): ") or "3")

                    print(f"Fetching {limit} posts for '{keyword}'...")
                    posts = fetch_bluesky_posts(keyword, limit)

                    if posts:
                        results = manager.run_full_pipeline(
                            posts, search_keyword=keyword
                        )
                        print(f"Pipeline Results:")
                        for key, value in results.items():
                            print(f"  {key.replace('_', ' ').title()}: {value}")
                    else:
                        print("❌ No posts fetched")

                except ImportError:
                    print("❌ Bluesky connector not available")
                except Exception as e:
                    print(f"❌ Pipeline test failed: {e}")

            elif choice == "5":
                confirm = input("This will recreate all tables. Are you sure? (y/N): ")
                if confirm.lower() == "y":
                    print("Creating/recreating database tables...")
                    manager.create_tables()
                    print("✅ Database tables created successfully")
                else:
                    print("Operation cancelled")

            elif choice == "6":
                unprocessed = manager.get_unprocessed_posts(limit=1000)
                print(f"Unprocessed posts: {len(unprocessed)}")
                if unprocessed:
                    show_details = input("Show first 5 posts? (y/N): ")
                    if show_details.lower() == "y":
                        for i, post in enumerate(unprocessed[:5], 1):
                            print(f"  [{i}] {post.author}: {post.text[:100]}...")

            elif choice == "7":
                unanalyzed = manager.get_unanalyzed_posts(limit=1000)
                print(f"Unanalyzed posts: {len(unanalyzed)}")
                if unanalyzed:
                    show_details = input("Show first 5 posts? (y/N): ")
                    if show_details.lower() == "y":
                        for i, post in enumerate(unanalyzed[:5], 1):
                            print(f"  [{i}] {post.cleaned_text[:100]}...")

            elif choice == "8":
                print("Goodbye!")
                break

            else:
                print("Invalid choice. Please enter 1-8.")

    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
    except Exception as e:
        print(f"❌ Error: {e}")
        logger.error(f"Main function error: {e}")


if __name__ == "__main__":
    main()
