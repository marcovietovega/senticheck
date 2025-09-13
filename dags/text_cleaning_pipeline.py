"""
Text Cleaning Pipeline DAG

Processes raw posts through text cleaning pipeline.
Cleans stored raw posts and prepares them for sentiment analysis.
"""

import os
import sys
import logging
from datetime import datetime, timedelta
from typing import Dict, Any

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from models.db_manager import SentiCheckDBManager

logger = logging.getLogger(__name__)

default_args = {
    "owner": "senticheck",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 11),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "text_cleaning_pipeline",
    default_args=default_args,
    description="Clean raw posts and prepare them for sentiment analysis",
    schedule=None,
    catchup=False,
    tags=["text-cleaning", "preprocessing", "senticheck"],
    max_active_runs=1,
)


def check_database_connection(**context) -> Dict[str, Any]:
    """Check database connection and get current statistics.

    Returns:
        Database statistics
    """
    try:
        logger.info("Checking database connection...")

        db_manager = SentiCheckDBManager()

        if not db_manager.test_connection():
            raise Exception("Database connection test failed")

        logger.info("Database connection successful")

        # Get current statistics
        stats = db_manager.get_database_stats()
        logger.info("Database Statistics:")
        for key, value in stats.items():
            logger.info("  %s: %s", key.replace('_', ' ').title(), value)

        return {
            "status": "success",
            "database_stats": stats,
            "timestamp": datetime.now().isoformat(),
        }

    except Exception as e:
        logger.error("Error checking database connection: %s", str(e))
        raise


def check_unprocessed_posts(**context) -> Dict[str, Any]:
    """Check how many raw posts are available for processing.

    Returns:
        Unprocessed posts information
    """
    try:
        logger.info("Checking for unprocessed posts...")

        db_manager = SentiCheckDBManager()

        # Get ALL unprocessed posts instead of using batch size
        logger.info("Getting all unprocessed posts from database...")
        unprocessed_posts = db_manager.get_unprocessed_posts(
            limit=None
        )  # No limit = get all
        unprocessed_count = len(unprocessed_posts)

        logger.info("Found %d total unprocessed posts to clean", unprocessed_count)

        if unprocessed_count == 0:
            logger.info("No posts to process - skipping cleaning pipeline")
            return {
                "status": "success",
                "unprocessed_count": 0,
                "message": "No posts to process",
                "timestamp": datetime.now().isoformat(),
            }

        # Show sample of posts to be processed
        sample_size = min(3, unprocessed_count)
        logger.info("Sample of posts to be processed (first %d):", sample_size)
        for i, post in enumerate(unprocessed_posts[:sample_size], 1):
            preview = post.text[:100] if post.text else "No text"
            logger.info("  [%d] %s: %s...", i, post.author, preview)

        return {
            "status": "success",
            "unprocessed_count": unprocessed_count,
            "sample_posts": [
                {
                    "id": post.id,
                    "author": post.author,
                    "text_preview": post.text[:100] if post.text else "No text",
                }
                for post in unprocessed_posts[:sample_size]
            ],
            "timestamp": datetime.now().isoformat(),
        }

    except Exception as e:
        logger.error("Error checking unprocessed posts: %s", str(e))
        raise


def clean_posts(**context) -> Dict[str, Any]:
    """
    Process raw posts through text cleaning pipeline.

    Returns:
        Dict with cleaning results
    """
    # Get unprocessed count from previous task
    check_result = context["ti"].xcom_pull(task_ids="check_unprocessed_posts")

    if check_result.get("unprocessed_count", 0) == 0:
        logger.info("No posts to clean - skipping")
        return {"status": "success", "cleaned_count": 0, "message": "No posts to clean"}

    try:
        logger.info("Starting text cleaning pipeline...")

        db_manager = SentiCheckDBManager()

        # Get configuration from Airflow Variables (with defaults)
        preserve_hashtags = (
            Variable.get("preserve_hashtags", default="false").lower() == "true"
        )
        preserve_mentions = (
            Variable.get("preserve_mentions", default="false").lower() == "true"
        )
        filter_hashtag_only = (
            Variable.get("filter_hashtag_only", default="true").lower() == "true"
        )
        min_content_words = int(Variable.get("min_content_words", default="3"))

        logger.info("Configuration:")
        logger.info("  Process ALL pending posts (no batch limit)")
        logger.info("  Preserve hashtags: %s", preserve_hashtags)
        logger.info("  Preserve mentions: %s", preserve_mentions)
        logger.info("  Filter hashtag-only posts: %s", filter_hashtag_only)
        logger.info("  Minimum content words: %d", min_content_words)

        # Process ALL posts (no limit)
        processed_count = db_manager.process_raw_posts_to_cleaned(
            preserve_hashtags=preserve_hashtags,
            preserve_mentions=preserve_mentions,
            filter_hashtag_only=filter_hashtag_only,
            min_content_words=min_content_words,
            limit=None,  # Process ALL unprocessed posts
        )

        logger.info("Successfully cleaned %d posts", processed_count)

        # Get updated statistics
        updated_stats = db_manager.get_database_stats()

        return {
            "status": "success",
            "cleaned_count": processed_count,
            "preserve_hashtags": preserve_hashtags,
            "preserve_mentions": preserve_mentions,
            "filter_hashtag_only": filter_hashtag_only,
            "min_content_words": min_content_words,
            "updated_stats": updated_stats,
            "timestamp": datetime.now().isoformat(),
        }

    except Exception as e:
        logger.error("Error cleaning posts: %s", str(e))
        raise


def validate_cleaned_posts(**context) -> Dict[str, Any]:
    """
    Validate the cleaning results and check data quality.

    Returns:
        Dict with validation results
    """
    try:
        logger.info("Validating cleaned posts...")

        db_manager = SentiCheckDBManager()

        # Get cleaning results from previous task
        clean_result = context["ti"].xcom_pull(task_ids="clean_posts")
        cleaned_count = clean_result.get("cleaned_count", 0)

        if cleaned_count == 0:
            logger.info("No posts were cleaned - validation complete")
            return {
                "status": "success",
                "validated_count": 0,
                "message": "No posts to validate",
            }

        # Get some recently cleaned posts for validation
        unanalyzed_posts = db_manager.get_unanalyzed_posts(limit=min(10, cleaned_count))

        validation_results = {
            "total_validated": len(unanalyzed_posts),
            "valid_posts": 0,
            "empty_posts": 0,
            "issues": [],
        }

        logger.info("Validating %d cleaned posts...", len(unanalyzed_posts))

        for post in unanalyzed_posts:
            if not post.cleaned_text or not post.cleaned_text.strip():
                validation_results["empty_posts"] += 1
                validation_results["issues"].append(
                    f"Post {post.id} has empty cleaned text"
                )
            else:
                validation_results["valid_posts"] += 1

        # Show validation summary
        logger.info("Validation Results:")
        logger.info("  Valid posts: %d", validation_results["valid_posts"])
        logger.info("  Empty posts: %d", validation_results["empty_posts"])

        if validation_results["issues"]:
            logger.info("  Issues found: %d", len(validation_results["issues"]))
            for issue in validation_results["issues"][:3]:  # Show first 3 issues
                logger.info("    - %s", issue)

        # Show sample of cleaned text
        if unanalyzed_posts:
            logger.info("Sample cleaned posts:")
            for i, post in enumerate(unanalyzed_posts[:3], 1):
                cleaned_preview = (
                    post.cleaned_text[:80] if post.cleaned_text else "No text"
                )
                logger.info("  [%d] %s...", i, cleaned_preview)

        success = validation_results["valid_posts"] > 0

        return {
            "status": "success" if success else "warning",
            "validation_results": validation_results,
            "cleaned_count_input": cleaned_count,
            "ready_for_analysis": validation_results["valid_posts"],
            "timestamp": datetime.now().isoformat(),
        }

    except Exception as e:
        logger.error("Error validating cleaned posts: %s", str(e))
        raise


# Define tasks
check_db_task = PythonOperator(
    task_id="check_database_connection",
    python_callable=check_database_connection,
    dag=dag,
)

check_posts_task = PythonOperator(
    task_id="check_unprocessed_posts",
    python_callable=check_unprocessed_posts,
    dag=dag,
)

clean_posts_task = PythonOperator(
    task_id="clean_posts",
    python_callable=clean_posts,
    dag=dag,
)

validate_task = PythonOperator(
    task_id="validate_cleaned_posts",
    python_callable=validate_cleaned_posts,
    dag=dag,
)

summary_task = BashOperator(
    task_id="pipeline_summary",
    bash_command="echo 'Text cleaning pipeline completed successfully'",
    dag=dag,
)

trigger_sentiment_analysis = TriggerDagRunOperator(
    task_id="trigger_sentiment_analysis",
    trigger_dag_id="sentiment_analysis_pipeline",
    wait_for_completion=False,
    dag=dag,
)

# Define task dependencies
(
    check_db_task
    >> check_posts_task
    >> clean_posts_task
    >> validate_task
    >> summary_task
    >> trigger_sentiment_analysis
)
