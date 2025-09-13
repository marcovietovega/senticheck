"""
Sentiment Analysis Pipeline DAG

Processes cleaned posts through sentiment analysis using a FastAPI service.
Analyzes sentiment via HTTP API calls and stores results in the database.
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


project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from models.db_manager import SentiCheckDBManager
from ml_service.client import create_sentiment_client

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
    "sentiment_analysis_pipeline",
    default_args=default_args,
    description="Analyze sentiment for cleaned posts via FastAPI service and store results",
    schedule=None,
    catchup=False,
    tags=["sentiment-analysis", "fastapi", "http-client", "ml", "senticheck"],
    max_active_runs=1,
)


def check_database_connection(**context) -> Dict[str, Any]:
    """Check database connection and get current statistics.

    Returns:
        Database statistics
    """
    try:
        logger.info("Checking database connection for sentiment analysis...")

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


def check_unanalyzed_posts(**context) -> Dict[str, Any]:
    """Check how many cleaned posts are available for sentiment analysis.

    Returns:
        Unanalyzed posts information
    """
    try:
        logger.info("Checking for unanalyzed posts...")

        db_manager = SentiCheckDBManager()

        logger.info("Getting all unanalyzed posts from database...")
        unanalyzed_posts = db_manager.get_unanalyzed_posts(limit=None)
        unanalyzed_count = len(unanalyzed_posts)

        logger.info("Found %d total unanalyzed posts to process", unanalyzed_count)

        if unanalyzed_count == 0:
            logger.info("No posts to analyze - skipping sentiment analysis pipeline")
            return {
                "status": "success",
                "unanalyzed_count": 0,
                "message": "No posts to analyze",
                "timestamp": datetime.now().isoformat(),
            }

        # Show sample of posts to be analyzed
        sample_size = min(3, unanalyzed_count)
        logger.info("Sample of posts to be analyzed (first %d):", sample_size)
        for i, post in enumerate(unanalyzed_posts[:sample_size], 1):
            preview = post.cleaned_text[:100] if post.cleaned_text else "No text"
            logger.info("  [%d] %s...", i, preview)

        return {
            "status": "success",
            "unanalyzed_count": unanalyzed_count,
            "sample_posts": [
                {
                    "id": post.id,
                    "text_preview": (
                        post.cleaned_text[:100] if post.cleaned_text else "No text"
                    ),
                    "raw_post_id": post.raw_post_id,
                }
                for post in unanalyzed_posts[:sample_size]
            ],
            "timestamp": datetime.now().isoformat(),
        }

    except Exception as e:
        logger.error("Error checking unanalyzed posts: %s", str(e))
        raise


def analyze_sentiment(**context) -> Dict[str, Any]:
    """Process cleaned posts through sentiment analysis using FastAPI service.

    Returns:
        Sentiment analysis results
    """
    # Get unanalyzed count from previous task
    check_result = context["ti"].xcom_pull(task_ids="check_unanalyzed_posts")

    if check_result.get("unanalyzed_count", 0) == 0:
        logger.info("No posts to analyze - skipping")
        return {
            "status": "success",
            "analyzed_count": 0,
            "message": "No posts to analyze",
        }

    try:
        logger.info("Starting sentiment analysis pipeline with FastAPI service...")

        db_manager = SentiCheckDBManager()

        model_name = Variable.get(
            "sentiment_model_name",
            default="cardiffnlp/twitter-roberta-base-sentiment-latest",
        )

        logger.info("Configuration:")
        logger.info("  Process ALL pending posts (no batch limit)")
        logger.info("  Model: %s", model_name)
        logger.info("  Service: Using FastAPI HTTP client (no deadlock risk)")

        # Step 1: Create HTTP client and check service health
        logger.info("Checking FastAPI service health...")
        with create_sentiment_client() as client:
            try:
                health = client.check_health()
                logger.info("Service is healthy: %s", health['status'])
                logger.info("   Model loaded: %s", health.get('model_name', 'Unknown'))
                logger.info("   Uptime: %.1fs", health.get('uptime_seconds', 0))
            except Exception as e:
                raise Exception(f"Sentiment analysis service is not available: {e}")

            # Step 2: Get ALL unanalyzed posts from database
            logger.info("Fetching ALL unanalyzed posts from database...")
            cleaned_posts = db_manager.get_unanalyzed_posts(
                limit=None
            )  # No limit = get all

            if not cleaned_posts:
                logger.info("No unanalyzed posts found in database")
                return {
                    "status": "success",
                    "analyzed_count": 0,
                    "message": "No unanalyzed posts found",
                }

            logger.info("Found %d posts to analyze", len(cleaned_posts))

            # Show sample of posts to be analyzed
            sample_size = min(3, len(cleaned_posts))
            logger.info("Sample posts to analyze (first %d):", sample_size)
            for i, post in enumerate(cleaned_posts[:sample_size], 1):
                preview = post.cleaned_text[:100] if post.cleaned_text else "No text"
                logger.info("  [%d] %s...", i, preview)

            # Step 3: Send posts to FastAPI service for analysis
            logger.info(
                "Sending %d posts to sentiment analysis service...", len(cleaned_posts)
            )
            sentiment_results = client.analyze_cleaned_posts(
                cleaned_posts=cleaned_posts, model_name=model_name
            )

            if not sentiment_results:
                raise Exception(
                    "No results returned from sentiment analysis service"
                )

            logger.info("Received %d sentiment analysis results", len(sentiment_results))

            # Step 4: Store results in database
            logger.info("Storing sentiment analysis results in database...")
            analyzed_count = db_manager.store_sentiment_analysis_batch(
                sentiment_results
            )

            if analyzed_count != len(sentiment_results):
                logger.warning(
                    "Expected to store %d results, but stored %d",
                    len(sentiment_results), analyzed_count
                )
            else:
                logger.info(
                    "Successfully stored %d sentiment analysis results", analyzed_count
                )

        # Get updated statistics
        updated_stats = db_manager.get_database_stats()

        return {
            "status": "success",
            "analyzed_count": analyzed_count,
            "model_name": model_name,
            "service_type": "FastAPI HTTP client",
            "results_received": len(sentiment_results),
            "results_stored": analyzed_count,
            "updated_stats": updated_stats,
            "timestamp": datetime.now().isoformat(),
        }

    except Exception as e:
        logger.error("Error in sentiment analysis pipeline: %s", str(e))
        raise


def validate_sentiment_results(**context) -> Dict[str, Any]:
    """Validate the sentiment analysis results and check data quality.

    Returns:
        Validation results
    """
    try:
        logger.info("Validating sentiment analysis results...")

        db_manager = SentiCheckDBManager()

        # Get analysis results from previous task
        analysis_result = context["ti"].xcom_pull(task_ids="analyze_sentiment")
        analyzed_count = analysis_result.get("analyzed_count", 0)

        if analyzed_count == 0:
            logger.info("No posts were analyzed - validation complete")
            return {
                "status": "success",
                "validated_count": 0,
                "message": "No posts to validate",
            }

        # Get database statistics to validate results
        stats = db_manager.get_database_stats()

        validation_results = {
            "total_analyzed_posts": stats.get("analyzed_posts", 0),
            "recent_analysis_count": analyzed_count,
            "validation_passed": True,
            "issues": [],
        }

        logger.info("Validation Results:")
        logger.info(
            "  Total analyzed posts in DB: %d", validation_results['total_analyzed_posts']
        )
        logger.info(
            "  Posts analyzed in this run: %d", validation_results['recent_analysis_count']
        )

        # Basic validation checks
        if validation_results["total_analyzed_posts"] == 0:
            validation_results["validation_passed"] = False
            validation_results["issues"].append("No analyzed posts found in database")

        if validation_results["recent_analysis_count"] == 0:
            validation_results["validation_passed"] = False
            validation_results["issues"].append("No posts were analyzed in this run")

        # Show any issues found
        if validation_results["issues"]:
            logger.info("  Issues found: %d", len(validation_results['issues']))
            for issue in validation_results["issues"]:
                logger.info("    - %s", issue)
        else:
            logger.info("  ✓ All validation checks passed")

        success = validation_results["validation_passed"]

        return {
            "status": "success" if success else "warning",
            "validation_results": validation_results,
            "analyzed_count_input": analyzed_count,
            "total_analyzed": validation_results["total_analyzed_posts"],
            "timestamp": datetime.now().isoformat(),
        }

    except Exception as e:
        logger.error("Error validating sentiment results: %s", str(e))
        raise


def generate_analysis_summary(**context) -> Dict[str, Any]:
    """Generate a summary of the sentiment analysis pipeline run.

    Returns:
        Pipeline summary
    """
    try:
        logger.info("Generating sentiment analysis pipeline summary...")

        db_manager = SentiCheckDBManager()

        analysis_result = context["ti"].xcom_pull(task_ids="analyze_sentiment")
        validation_result = context["ti"].xcom_pull(
            task_ids="validate_sentiment_results"
        )

        final_stats = db_manager.get_database_stats()

        summary = {
            "pipeline_run": {
                "analyzed_count": analysis_result.get("analyzed_count", 0),
                "model_used": analysis_result.get("model_name", "unknown"),
                "validation_passed": validation_result.get(
                    "validation_results", {}
                ).get("validation_passed", False),
            },
            "database_final_state": final_stats,
            "pipeline_completion_time": datetime.now().isoformat(),
        }

        logger.info("=" * 60)
        logger.info("SENTIMENT ANALYSIS PIPELINE SUMMARY")
        logger.info("=" * 60)
        logger.info("Posts analyzed: %d", summary['pipeline_run']['analyzed_count'])
        logger.info("Model used: %s", summary['pipeline_run']['model_used'])
        logger.info("Processing mode: ALL pending posts (no batch limit)")
        logger.info(
            "Validation: %s", 
            '✓ PASSED' if summary['pipeline_run']['validation_passed'] else 'FAILED'
        )
        logger.info("Final Database State:")
        for key, value in summary["database_final_state"].items():
            logger.info("  %s: %s", key.replace('_', ' ').title(), value)
        logger.info("=" * 60)

        return summary

    except Exception as e:
        logger.error("Error generating summary: %s", str(e))
        raise


# Define tasks
check_db_task = PythonOperator(
    task_id="check_database_connection",
    python_callable=check_database_connection,
    dag=dag,
)

check_posts_task = PythonOperator(
    task_id="check_unanalyzed_posts",
    python_callable=check_unanalyzed_posts,
    dag=dag,
)

analyze_task = PythonOperator(
    task_id="analyze_sentiment",
    python_callable=analyze_sentiment,
    dag=dag,
)

validate_task = PythonOperator(
    task_id="validate_sentiment_results",
    python_callable=validate_sentiment_results,
    dag=dag,
)

summary_task = PythonOperator(
    task_id="generate_pipeline_summary",
    python_callable=generate_analysis_summary,
    dag=dag,
)

completion_task = BashOperator(
    task_id="pipeline_completion",
    bash_command="echo 'Sentiment analysis pipeline completed successfully'",
    dag=dag,
)

# Define task dependencies
(
    check_db_task
    >> check_posts_task
    >> analyze_task
    >> validate_task
    >> summary_task
    >> completion_task
)
