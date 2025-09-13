"""
Bluesky Data Collection DAG

Fetches recent posts from Bluesky and stores them in the database.
Runs every 30 minutes to collect fresh posts for sentiment analysis.
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
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from connectors.bluesky.fetch_posts import BlueskyConnector
from models.db_manager import SentiCheckDBManager

logger = logging.getLogger(__name__)

default_args = {
    "owner": "senticheck",
    "depends_on_past": False,
    "start_date": datetime(2025, 8, 20),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


dag = DAG(
    "bluesky_data_collection",
    default_args=default_args,
    description="Collects posts from Bluesky every 30 minutes for sentiment analysis",
    schedule="*/30 * * * *",
    catchup=False,
    tags=["bluesky", "data-collection", "senticheck"],
    max_active_runs=1,
)


def check_environment(**context) -> Dict[str, Any]:
    """Check that all required environment variables are set.
    
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

    logger.info("All required environment variables are set")
    return {
        "status": "success",
        "checked_vars": required_vars,
        "timestamp": datetime.now().isoformat(),
    }


def fetch_bluesky_posts(**context) -> Dict[str, Any]:
    """Fetch recent posts from Bluesky.
    
    Returns:
        Dict with fetched posts and metadata
    """
    import json
    
    try:
        # Get keywords from Airflow Variables
        keywords_var = Variable.get("bluesky_search_keywords", default="AI")
        
        # Handle both string and JSON array formats
        if isinstance(keywords_var, str):
            try:
                keywords = json.loads(keywords_var) if keywords_var.startswith('[') else [keywords_var]
            except json.JSONDecodeError:
                keywords = [keywords_var]
        else:
            keywords = keywords_var if isinstance(keywords_var, list) else [keywords_var]

        logger.info("Starting Bluesky data collection:")
        logger.info(
            "  - Execution date: %s",
            context.get('execution_date', datetime.now()).strftime('%Y-%m-%d %H:%M')
        )
        logger.info("  - Keywords: %s", keywords)
        logger.info("  - Mode: Recent posts")

        connector = BlueskyConnector()
        if not connector.connect():
            raise Exception("Failed to connect to Bluesky")

        logger.info("Connected to Bluesky successfully")

        # Fetch posts for each keyword
        all_posts = []
        for keyword in keywords:
            logger.info("Fetching posts for keyword: '%s'", keyword)
            
            posts = connector.fetch_posts(
                keyword=keyword,
                lang="en",
            )
            
            # Tag each post with the keyword that found it
            for post in posts:
                post['search_keyword'] = keyword
            
            all_posts.extend(posts)
            logger.info("Fetched %d posts for keyword '%s'", len(posts), keyword)

        logger.info("Total fetched %d posts from Bluesky across %d keywords", len(all_posts), len(keywords))

        # Convert datetime objects to ISO strings
        for post in all_posts:
            if post.get("timestamp") and hasattr(post["timestamp"], "isoformat"):
                post["timestamp"] = post["timestamp"].isoformat()
            if post.get("fetched_at") and hasattr(post["fetched_at"], "isoformat"):
                post["fetched_at"] = post["fetched_at"].isoformat()

        connector.disconnect()

        return {
            "posts": all_posts,
            "total_fetched": len(all_posts),
            "keywords_used": keywords,
            "timestamp": datetime.now().isoformat(),
        }

    except Exception as e:
        logger.error("Error fetching posts: %s", e)
        raise


def store_posts_in_database(**context) -> Dict[str, Any]:
    """Store fetched posts in the database.
    
    Returns:
        Dict with storage results
    """
    # Get data from previous task
    fetch_result = context["ti"].xcom_pull(task_ids="fetch_posts")

    if not fetch_result or not fetch_result.get("posts"):
        logger.info("No posts to store")
        return {"status": "success", "stored_count": 0, "message": "No posts to store"}

    posts = fetch_result["posts"]
    date_info = fetch_result.get("date_range", {})
    keywords_used = fetch_result.get("keywords_used", ["AI"])

    logger.info(
        "Storing %d posts from %s in database (keywords: %s)",
        len(posts),
        date_info.get('date_str', 'recent collection'),
        keywords_used
    )

    try:
        db_manager = SentiCheckDBManager()

        # Posts already have search_keyword in each post
        stored_count = db_manager.store_raw_posts(posts)

        logger.info("Successfully stored %d posts in database", stored_count)

        return {
            "status": "success",
            "total_posts": len(posts),
            "stored_count": stored_count,
            "skipped_count": len(posts) - stored_count,
            "keywords_used": keywords_used,
            "date_range": date_info,
            "timestamp": datetime.now().isoformat(),
        }

    except Exception as e:
        logger.error("Error storing posts in database: %s", str(e))
        raise


def setup_database(**context) -> Dict[str, Any]:
    """Set up database schema and tables.
    
    Returns:
        Dict with setup results
    """
    try:
        logger.info("Setting up database schema and tables...")

        db_manager = SentiCheckDBManager()
        db_manager.create_tables()

        logger.info("Database setup completed successfully")

        return {
            "status": "success",
            "message": "Database set up successfully",
        }

    except Exception as e:
        logger.error("Error setting up database: %s", str(e))
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
    bash_command="echo 'Bluesky data collection completed'",
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
