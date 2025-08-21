"""
Sentiment Analysis Pipeline DAG - SentiCheck Project

This DAG processes cleaned posts through sentiment analysis pipeline using a FastAPI service.
It analyzes sentiment for cleaned posts via HTTP API calls and stores the results in the database.

Key Changes:
- Uses FastAPI HTTP service instead of direct model loading (eliminates deadlock issues)
- Includes service health checks before processing
- Maintains the same database workflow and validation steps
"""

import os
import sys
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
    schedule=None,  # Triggered by text_cleaning_pipeline
    catchup=False,
    tags=["sentiment-analysis", "fastapi", "http-client", "ml", "senticheck"],
    max_active_runs=1,
)


def check_database_connection(**context) -> Dict[str, Any]:
    """
    Check database connection and get current statistics.

    Returns:
        Dict with database statistics
    """
    try:
        print("Checking database connection for sentiment analysis...")

        db_manager = SentiCheckDBManager()

        if not db_manager.test_connection():
            raise Exception("Database connection test failed")

        print("✓ Database connection successful")

        # Get current statistics
        stats = db_manager.get_database_stats()
        print(f"Database Statistics:")
        for key, value in stats.items():
            print(f"  {key.replace('_', ' ').title()}: {value}")

        return {
            "status": "success",
            "database_stats": stats,
            "timestamp": datetime.now().isoformat(),
        }

    except Exception as e:
        print(f"✗ Error checking database connection: {str(e)}")
        raise


def check_unanalyzed_posts(**context) -> Dict[str, Any]:
    """
    Check how many cleaned posts are available for sentiment analysis.

    Returns:
        Dict with unanalyzed posts information
    """
    try:
        print("Checking for unanalyzed posts...")

        db_manager = SentiCheckDBManager()

        # Get ALL unanalyzed posts instead of using batch size
        print("Getting all unanalyzed posts from database...")
        unanalyzed_posts = db_manager.get_unanalyzed_posts(
            limit=None
        )  # No limit = get all
        unanalyzed_count = len(unanalyzed_posts)

        print(f"Found {unanalyzed_count} total unanalyzed posts to process")

        if unanalyzed_count == 0:
            print("No posts to analyze - skipping sentiment analysis pipeline")
            return {
                "status": "success",
                "unanalyzed_count": 0,
                "message": "No posts to analyze",
                "timestamp": datetime.now().isoformat(),
            }

        # Show sample of posts to be analyzed
        sample_size = min(3, unanalyzed_count)
        print(f"\nSample of posts to be analyzed (first {sample_size}):")
        for i, post in enumerate(unanalyzed_posts[:sample_size], 1):
            preview = post.cleaned_text[:100] if post.cleaned_text else "No text"
            print(f"  [{i}] {preview}...")

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
        print(f"✗ Error checking unanalyzed posts: {str(e)}")
        raise


def analyze_sentiment(**context) -> Dict[str, Any]:
    """
    Process cleaned posts through sentiment analysis pipeline using FastAPI service.

    This method now uses the HTTP client to communicate with the FastAPI service,
    eliminating the deadlock issues that occurred with direct model loading.

    Returns:
        Dict with sentiment analysis results
    """
    # Get unanalyzed count from previous task
    check_result = context["ti"].xcom_pull(task_ids="check_unanalyzed_posts")

    if check_result.get("unanalyzed_count", 0) == 0:
        print("No posts to analyze - skipping")
        return {
            "status": "success",
            "analyzed_count": 0,
            "message": "No posts to analyze",
        }

    try:
        print("Starting sentiment analysis pipeline with FastAPI service...")

        db_manager = SentiCheckDBManager()

        # Get configuration from Airflow Variables (with defaults)
        model_name = Variable.get(
            "sentiment_model_name",
            default="cardiffnlp/twitter-roberta-base-sentiment-latest",
        )

        print(f"Configuration:")
        print(f"  Process ALL pending posts (no batch limit)")
        print(f"  Model: {model_name}")
        print(f"  Service: Using FastAPI HTTP client (no deadlock risk)")

        # Step 1: Create HTTP client and check service health
        print("🔍 Checking FastAPI service health...")
        with create_sentiment_client() as client:
            try:
                health = client.check_health()
                print(f"✅ Service is healthy: {health['status']}")
                print(f"   Model loaded: {health.get('model_name', 'Unknown')}")
                print(f"   Uptime: {health.get('uptime_seconds', 0):.1f}s")
            except Exception as e:
                raise Exception(f"❌ Sentiment analysis service is not available: {e}")

            # Step 2: Get ALL unanalyzed posts from database
            print(f"📄 Fetching ALL unanalyzed posts from database...")
            cleaned_posts = db_manager.get_unanalyzed_posts(
                limit=None
            )  # No limit = get all

            if not cleaned_posts:
                print("No unanalyzed posts found in database")
                return {
                    "status": "success",
                    "analyzed_count": 0,
                    "message": "No unanalyzed posts found",
                }

            print(f"✅ Found {len(cleaned_posts)} posts to analyze")

            # Show sample of posts to be analyzed
            sample_size = min(3, len(cleaned_posts))
            print(f"Sample posts to analyze (first {sample_size}):")
            for i, post in enumerate(cleaned_posts[:sample_size], 1):
                preview = post.cleaned_text[:100] if post.cleaned_text else "No text"
                print(f"  [{i}] {preview}...")

            # Step 3: Send posts to FastAPI service for analysis
            print(
                f"🧠 Sending {len(cleaned_posts)} posts to sentiment analysis service..."
            )
            sentiment_results = client.analyze_cleaned_posts(
                cleaned_posts=cleaned_posts, model_name=model_name
            )

            if not sentiment_results:
                raise Exception(
                    "❌ No results returned from sentiment analysis service"
                )

            print(f"✅ Received {len(sentiment_results)} sentiment analysis results")

            # Step 4: Store results in database
            print("💾 Storing sentiment analysis results in database...")
            analyzed_count = db_manager.store_sentiment_analysis_batch(
                sentiment_results
            )

            if analyzed_count != len(sentiment_results):
                print(
                    f"⚠️  Warning: Expected to store {len(sentiment_results)} results, but stored {analyzed_count}"
                )
            else:
                print(
                    f"✅ Successfully stored {analyzed_count} sentiment analysis results"
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
        print(f"✗ Error in sentiment analysis pipeline: {str(e)}")
        import traceback

        traceback.print_exc()
        raise


def validate_sentiment_results(**context) -> Dict[str, Any]:
    """
    Validate the sentiment analysis results and check data quality.

    Returns:
        Dict with validation results
    """
    try:
        print("Validating sentiment analysis results...")

        db_manager = SentiCheckDBManager()

        # Get analysis results from previous task
        analysis_result = context["ti"].xcom_pull(task_ids="analyze_sentiment")
        analyzed_count = analysis_result.get("analyzed_count", 0)

        if analyzed_count == 0:
            print("No posts were analyzed - validation complete")
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

        print(f"Validation Results:")
        print(
            f"  Total analyzed posts in DB: {validation_results['total_analyzed_posts']}"
        )
        print(
            f"  Posts analyzed in this run: {validation_results['recent_analysis_count']}"
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
            print(f"  Issues found: {len(validation_results['issues'])}")
            for issue in validation_results["issues"]:
                print(f"    - {issue}")
        else:
            print("  ✓ All validation checks passed")

        success = validation_results["validation_passed"]

        return {
            "status": "success" if success else "warning",
            "validation_results": validation_results,
            "analyzed_count_input": analyzed_count,
            "total_analyzed": validation_results["total_analyzed_posts"],
            "timestamp": datetime.now().isoformat(),
        }

    except Exception as e:
        print(f"✗ Error validating sentiment results: {str(e)}")
        raise


def generate_analysis_summary(**context) -> Dict[str, Any]:
    """
    Generate a summary of the sentiment analysis pipeline run.

    Returns:
        Dict with pipeline summary
    """
    try:
        print("Generating sentiment analysis pipeline summary...")

        db_manager = SentiCheckDBManager()

        # Get results from previous tasks
        analysis_result = context["ti"].xcom_pull(task_ids="analyze_sentiment")
        validation_result = context["ti"].xcom_pull(
            task_ids="validate_sentiment_results"
        )

        # Get final database statistics
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

        print("=" * 60)
        print("SENTIMENT ANALYSIS PIPELINE SUMMARY")
        print("=" * 60)
        print(f"Posts analyzed: {summary['pipeline_run']['analyzed_count']}")
        print(f"Model used: {summary['pipeline_run']['model_used']}")
        print(f"Processing mode: ALL pending posts (no batch limit)")
        print(
            f"Validation: {'✓ PASSED' if summary['pipeline_run']['validation_passed'] else '✗ FAILED'}"
        )
        print("\nFinal Database State:")
        for key, value in summary["database_final_state"].items():
            print(f"  {key.replace('_', ' ').title()}: {value}")
        print("=" * 60)

        return summary

    except Exception as e:
        print(f"✗ Error generating summary: {str(e)}")
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
