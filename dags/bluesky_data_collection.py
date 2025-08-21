"""
Bluesky Data Collection DAG - SentiCheck Project

This DAG fetches recent posts from Bluesky and stores them in the database.
It runs every 30 minutes to collect fresh posts for sentiment analysis.

Configuration (Airflow Variables):
- bluesky_search_keyword: Search keyword (default: "AI")
"""

import os
import sys
from datetime import datetime, timedelta
from typing import Dict, Any

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Variable
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from connectors.bluesky.fetch_posts import BlueskyConnector
from models.db_manager import SentiCheckDBManager

default_args = {
    "owner": "senticheck",
    "depends_on_past": False,
    "start_date": datetime(2025, 8, 20),  # Start from yesterday for daily collection
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


dag = DAG(
    "bluesky_data_collection",
    default_args=default_args,
    description="Collection of posts from Bluesky every 30 minutes for sentiment analysis",
    schedule="*/30 * * * *",  # Run every 30 minutes
    catchup=False,
    tags=["bluesky", "data-collection", "senticheck"],
    max_active_runs=1,
)


def check_environment(**context) -> Dict[str, Any]:
    """
    Check that all required environment variables are set.

    Returns:
        Dict with environment check results
    """
    required_vars = ["BLUESKY_HANDLE", "BLUESKY_APP_PASSWORD"]
    missing_vars = []

    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)

    if missing_vars:
        raise ValueError(f"Missing required environment variables: {missing_vars}")

    print("✓ All required environment variables are set")
    return {
        "status": "success",
        "checked_vars": required_vars,
        "timestamp": datetime.now().isoformat(),
    }


def fetch_bluesky_posts(**context) -> Dict[str, Any]:
    """
    Fetch recent posts from Bluesky (no date filtering - just get latest posts).

    Returns:
        Dict with fetching results
    """
    try:
        # Get configuration from Airflow Variables
        search_keyword = Variable.get("bluesky_search_keyword", default="AI")

        print(f"Starting Bluesky data collection:")
        print(
            f"  - Execution date: {context.get('execution_date', datetime.now()).strftime('%Y-%m-%d %H:%M')}"
        )
        print(f"  - Keyword: {search_keyword}")
        print(f"  - Mode: Recent posts (no date filtering)")

        connector = BlueskyConnector()
        if not connector.connect():
            raise Exception("Failed to connect to Bluesky")

        print("✓ Connected to Bluesky successfully")

        # Fetch recent posts without date filtering
        posts = connector.fetch_posts(
            keyword=search_keyword,
            lang="en",
        )

        print(f"✓ Fetched {len(posts)} recent posts from Bluesky")

        # Convert datetime objects to ISO strings for XCom serialization
        for post in posts:
            if post.get("timestamp") and hasattr(post["timestamp"], "isoformat"):
                post["timestamp"] = post["timestamp"].isoformat()
            if post.get("fetched_at") and hasattr(post["fetched_at"], "isoformat"):
                post["fetched_at"] = post["fetched_at"].isoformat()

        connector.disconnect()

        return {
            "posts": posts,
            "total_fetched": len(posts),
            "timestamp": datetime.now().isoformat(),
        }

    except Exception as e:
        print(f"✗ Error fetching posts: {e}")
        raise


def store_posts_in_database(**context) -> Dict[str, Any]:
    """
    Store fetched posts in the database.

    Returns:
        Dict with storage results
    """
    # Pull data from the previous task
    fetch_result = context["ti"].xcom_pull(task_ids="fetch_posts")

    if not fetch_result or not fetch_result.get("posts"):
        print("No posts to store")
        return {"status": "success", "stored_count": 0, "message": "No posts to store"}

    posts = fetch_result["posts"]
    date_info = fetch_result.get("date_range", {})

    # Get the search keyword used for fetching
    search_keyword = Variable.get("bluesky_search_keyword", default="AI")

    print(
        f"Storing {len(posts)} posts from {date_info.get('date_str', 'unknown date')} in database (keyword: {search_keyword})"
    )

    try:
        db_manager = SentiCheckDBManager()

        stored_count = db_manager.store_raw_posts(posts, search_keyword=search_keyword)

        print(f"✓ Successfully stored {stored_count} posts in database")

        return {
            "status": "success",
            "total_posts": len(posts),
            "stored_count": stored_count,
            "skipped_count": len(posts) - stored_count,
            "search_keyword": search_keyword,
            "date_range": date_info,
            "timestamp": datetime.now().isoformat(),
        }

    except Exception as e:
        print(f"✗ Error storing posts in database: {str(e)}")
        raise


def setup_database(**context) -> Dict[str, Any]:
    """
    Setup database schema and tables.

    Returns:
        Dict with setup results
    """
    try:
        print("Setting up database schema and tables...")

        db_manager = SentiCheckDBManager()

        # This will create tables if they don't exist
        db_manager.create_tables()

        print("✓ Database setup completed successfully")

        return {
            "status": "success",
            "message": "Database schema and tables created successfully",
        }

    except Exception as e:
        print(f"✗ Error setting up database: {str(e)}")
        raise


check_env_task = PythonOperator(
    task_id="check_environment",
    python_callable=check_environment,
    dag=dag,
)

setup_db_task = PythonOperator(
    task_id="setup_database",
    python_callable=setup_database,
    dag=dag,
)

fetch_posts_task = PythonOperator(
    task_id="fetch_posts",
    python_callable=fetch_bluesky_posts,
    dag=dag,
)

store_posts_task = PythonOperator(
    task_id="store_posts",
    python_callable=store_posts_in_database,
    dag=dag,
)


cleanup_task = BashOperator(
    task_id="cleanup",
    bash_command="echo 'Daily Bluesky data collection completed successfully'",
    dag=dag,
)


trigger_cleaning = TriggerDagRunOperator(
    task_id="trigger_text_cleaning",
    trigger_dag_id="text_cleaning_pipeline",
    wait_for_completion=False,
    dag=dag,
)

(
    check_env_task
    >> setup_db_task
    >> fetch_posts_task
    >> store_posts_task
    >> cleanup_task
    >> trigger_cleaning
)
