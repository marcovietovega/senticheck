"""
Text Cleaning Pipeline DAG - SentiCheck Project

This DAG processes raw posts through text cleaning pipeline.
It cleans stored raw posts and prepares them for sentiment analysis.
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
    """
    Check database connection and get current statistics.

    Returns:
        Dict with database statistics
    """
    try:
        print("Checking database connection...")

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


def check_unprocessed_posts(**context) -> Dict[str, Any]:
    """
    Check how many raw posts are available for processing.

    Returns:
        Dict with unprocessed posts information
    """
    try:
        print("Checking for unprocessed posts...")

        db_manager = SentiCheckDBManager()

        # Get batch size from Airflow Variables (with default)
        batch_size = int(Variable.get("cleaning_batch_size", default=500))

        unprocessed_posts = db_manager.get_unprocessed_posts(limit=batch_size)
        unprocessed_count = len(unprocessed_posts)

        print(f"Found {unprocessed_count} unprocessed posts (batch size: {batch_size})")

        if unprocessed_count == 0:
            print("No posts to process - skipping cleaning pipeline")
            return {
                "status": "success",
                "unprocessed_count": 0,
                "batch_size": batch_size,
                "message": "No posts to process",
                "timestamp": datetime.now().isoformat(),
            }

        # Show sample of posts to be processed
        sample_size = min(3, unprocessed_count)
        print(f"\nSample of posts to be processed (first {sample_size}):")
        for i, post in enumerate(unprocessed_posts[:sample_size], 1):
            preview = post.text[:100] if post.text else "No text"
            print(f"  [{i}] {post.author}: {preview}...")

        return {
            "status": "success",
            "unprocessed_count": unprocessed_count,
            "batch_size": batch_size,
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
        print(f"✗ Error checking unprocessed posts: {str(e)}")
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
        print("No posts to clean - skipping")
        return {"status": "success", "cleaned_count": 0, "message": "No posts to clean"}

    try:
        print("Starting text cleaning pipeline...")

        db_manager = SentiCheckDBManager()

        # Get configuration from Airflow Variables (with defaults)
        batch_size = int(Variable.get("cleaning_batch_size", default=500))
        preserve_hashtags = (
            Variable.get("preserve_hashtags", default="false").lower() == "true"
        )
        preserve_mentions = (
            Variable.get("preserve_mentions", default="false").lower() == "true"
        )

        print(f"Configuration:")
        print(f"  Batch size: {batch_size}")
        print(f"  Preserve hashtags: {preserve_hashtags}")
        print(f"  Preserve mentions: {preserve_mentions}")

        # Process posts
        processed_count = db_manager.process_raw_posts_to_cleaned(
            preserve_hashtags=preserve_hashtags,
            preserve_mentions=preserve_mentions,
            limit=batch_size,
        )

        print(f"✓ Successfully cleaned {processed_count} posts")

        # Get updated statistics
        updated_stats = db_manager.get_database_stats()

        return {
            "status": "success",
            "cleaned_count": processed_count,
            "batch_size": batch_size,
            "preserve_hashtags": preserve_hashtags,
            "preserve_mentions": preserve_mentions,
            "updated_stats": updated_stats,
            "timestamp": datetime.now().isoformat(),
        }

    except Exception as e:
        print(f"✗ Error cleaning posts: {str(e)}")
        raise


def validate_cleaned_posts(**context) -> Dict[str, Any]:
    """
    Validate the cleaning results and check data quality.

    Returns:
        Dict with validation results
    """
    try:
        print("Validating cleaned posts...")

        db_manager = SentiCheckDBManager()

        # Get cleaning results from previous task
        clean_result = context["ti"].xcom_pull(task_ids="clean_posts")
        cleaned_count = clean_result.get("cleaned_count", 0)

        if cleaned_count == 0:
            print("No posts were cleaned - validation complete")
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

        print(f"Validating {len(unanalyzed_posts)} cleaned posts...")

        for post in unanalyzed_posts:
            if not post.cleaned_text or not post.cleaned_text.strip():
                validation_results["empty_posts"] += 1
                validation_results["issues"].append(
                    f"Post {post.id} has empty cleaned text"
                )
            else:
                validation_results["valid_posts"] += 1

        # Show validation summary
        print(f"Validation Results:")
        print(f"  Valid posts: {validation_results['valid_posts']}")
        print(f"  Empty posts: {validation_results['empty_posts']}")

        if validation_results["issues"]:
            print(f"  Issues found: {len(validation_results['issues'])}")
            for issue in validation_results["issues"][:3]:  # Show first 3 issues
                print(f"    - {issue}")

        # Show sample of cleaned text
        if unanalyzed_posts:
            print(f"\nSample cleaned posts:")
            for i, post in enumerate(unanalyzed_posts[:3], 1):
                cleaned_preview = (
                    post.cleaned_text[:80] if post.cleaned_text else "No text"
                )
                print(f"  [{i}] {cleaned_preview}...")

        success = validation_results["valid_posts"] > 0

        return {
            "status": "success" if success else "warning",
            "validation_results": validation_results,
            "cleaned_count_input": cleaned_count,
            "ready_for_analysis": validation_results["valid_posts"],
            "timestamp": datetime.now().isoformat(),
        }

    except Exception as e:
        print(f"✗ Error validating cleaned posts: {str(e)}")
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

# Define task dependencies
(check_db_task >> check_posts_task >> clean_posts_task >> validate_task >> summary_task)
