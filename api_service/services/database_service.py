import logging
import sys
import os
from typing import List, Dict, Any


project_root = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from api_service.utils.text_cleaner import clean_bluesky_posts
from models.database import RawPost

logger = logging.getLogger(__name__)


class DatabaseService:

    def __init__(self):
        from models.db_manager import get_db_manager

        self.db_ops = get_db_manager()

    def get_database_stats(self) -> Dict[str, Any]:
        return self.db_ops.get_database_stats()

    def store_raw_posts(self, posts_data: List[Dict]) -> int:
        return self.db_ops.store_raw_posts(posts_data)

    def get_unprocessed_posts(self) -> List[RawPost]:
        return self.db_ops.get_unprocessed_posts()

    def store_cleaned_post(self, cleaned_post: Dict) -> int:
        return self.db_ops.store_cleaned_post(cleaned_post)

    def store_sentiment_analysis(self, sentiment_data: Dict) -> int:
        return self.db_ops.store_sentiment_analysis(sentiment_data)

    def process_raw_posts_to_cleaned(self) -> int:
        try:

            raw_posts = self.get_unprocessed_posts()
            if not raw_posts:
                logger.info("No unprocessed posts found")
                return 0

            logger.info(f"Processing {len(raw_posts)} raw posts...")

            posts_to_clean = []
            for post in raw_posts:
                post_dict = {
                    "id": post.id,
                    "text": post.text,
                    "author": post.author,
                    "author_handle": post.author_handle,
                    "post_uri": post.post_uri,
                    "search_keyword": post.search_keyword,
                    "created_at": post.created_at,
                }
                posts_to_clean.append(post_dict)

            cleaned_posts = clean_bluesky_posts(posts_to_clean)

            processed_count = 0
            for cleaned_post in cleaned_posts:
                try:
                    raw_post_id = cleaned_post.get("id")
                    if not raw_post_id:
                        logger.error("Missing raw_post_id in cleaned post data")
                        continue

                    result = self.db_ops.store_cleaned_post(
                        raw_post_id,
                        cleaned_post.get("text", ""),
                        cleaned_post.get("original_text", ""),
                        cleaned_post.get("search_keyword"),
                        cleaned_post.get("processing_metadata", {}),
                    )

                    if result is not None:
                        processed_count += 1
                    else:
                        logger.error(
                            f"Failed to store cleaned post for raw_post_id {raw_post_id}"
                        )

                except Exception as e:
                    logger.error(f"Error storing cleaned post: {e}")

            logger.info(f"Processed {processed_count} posts successfully")
            return processed_count

        except Exception as e:
            logger.error(f"Error processing raw posts to cleaned: {e}")
            return 0

    def analyze_cleaned_posts_sentiment(
        self,
        model_name: str = "cardiffnlp/twitter-roberta-base-sentiment-latest",
        limit: int = 1000,
    ) -> int:
        try:

            cleaned_posts = self.db_ops.get_unanalyzed_posts(limit)
            if not cleaned_posts:
                logger.info("No unanalyzed posts found")
                return 0

            try:
                from api_service.sentiment_analyzer import SentimentAnalyzer

                analyzer = SentimentAnalyzer.get_cached_analyzer(model_name)
                if not analyzer:
                    logger.error("Failed to get cached sentiment analyzer")
                    return 0
            except ImportError as e:
                logger.error(f"Failed to import SentimentAnalyzer: {e}")
                return 0

            logger.info(f"Analyzing sentiment for {len(cleaned_posts)} posts...")

            analyzed_count = 0
            sentiment_results = []
            post_ids_to_mark = []

            for post in cleaned_posts:
                try:
                    result = analyzer.analyze_text(post.cleaned_text)
                    if result:
                        sentiment_data = {
                            "cleaned_post_id": post.id,
                            "sentiment_label": result["sentiment_label"],
                            "confidence_score": result["confidence_score"],
                            "positive_score": result.get("positive_score", 0.0),
                            "negative_score": result.get("negative_score", 0.0),
                            "neutral_score": result.get("neutral_score", 0.0),
                            "model_name": result["model_name"],
                            "search_keyword": post.search_keyword,
                            "analyzed_at": result["analyzed_at"],
                        }

                        sentiment_results.append(sentiment_data)
                        post_ids_to_mark.append(post.id)
                        analyzed_count += 1

                except Exception as e:
                    logger.error(f"Error analyzing post {post.id}: {e}")

            if sentiment_results:
                self.db_ops.store_sentiment_analysis_batch(sentiment_results)

            logger.info(f"Analyzed {analyzed_count} posts successfully")
            return analyzed_count

        except ImportError as e:
            logger.error(f"Failed to import SentimentAnalyzer: {e}")
            return 0
        except Exception as e:
            logger.error(f"Error in sentiment analysis: {e}")
            return 0

    def get_sentiment_distribution(
        self, search_keyword: str = None, days: int = 30
    ) -> Dict[str, Any]:
        return self.db_ops.get_sentiment_distribution(search_keyword, days)

    def get_sentiment_over_time(
        self, search_keyword: str, days: int = 30
    ) -> List[Dict]:
        return self.db_ops.get_sentiment_over_time(search_keyword, days)

    def calculate_sentiment_trends(self) -> Dict[str, Any]:
        return self.db_ops.calculate_sentiment_trends()

    def get_average_confidence(
        self, search_keyword: str = None, days: int = 30
    ) -> float:
        return self.db_ops.get_average_confidence(search_keyword, days)

    def get_posts_by_date(self, search_keyword: str, days: int = 2) -> List[Dict]:
        return self.db_ops.get_posts_by_date(search_keyword, days)

    def get_keywords_with_counts(self) -> List[Dict]:
        return self.db_ops.get_keywords_with_counts()

    def get_keyword_specific_metrics(self, keyword: str, days: int) -> Dict[str, Any]:
        return self.db_ops.get_keyword_specific_metrics(keyword, days)

    def get_keyword_specific_kpis(self, keyword: str, days: int) -> Dict[str, Any]:
        return self.db_ops.get_keyword_specific_kpis(keyword, days)

    def get_text_analysis_for_keyword(self, keyword: str, days: int) -> List[Dict]:
        return self.db_ops.get_text_analysis_for_keyword(keyword, days)


database_service = None


def get_database_service() -> DatabaseService:
    global database_service
    if database_service is None:
        database_service = DatabaseService()
    return database_service
