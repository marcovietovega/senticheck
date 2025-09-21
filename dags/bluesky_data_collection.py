"""
Bluesky Data Collection DAG

Fetches recent posts from Bluesky and stores them in the database.
Runs every hour to collect fresh posts for sentiment analysis.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Variable
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
import json
from utils.api_client import SentiCheckAPIClient, APIError

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
    description="Collects posts from Bluesky every hour for sentiment analysis",
    schedule="0 * * * *",
    catchup=False,
    tags=["bluesky", "data-collection", "senticheck"],
    max_active_runs=1,
)


def fetch_bluesky_posts(**context) -> Dict[str, Any]:
    """Fetch recent posts from Bluesky via API service.

    Returns:
        Dict with fetched posts and metadata
    """

    try:
        try:
            keywords_var = Variable.get("bluesky_search_keywords")
        except KeyError:
            keywords_var = "AI"

        if isinstance(keywords_var, str):
            try:
                keywords = (
                    json.loads(keywords_var)
                    if keywords_var.startswith("[")
                    else [keywords_var]
                )
            except json.JSONDecodeError:
                keywords = [keywords_var]
        else:
            keywords = (
                keywords_var if isinstance(keywords_var, list) else [keywords_var]
            )

        logger.info("Starting Bluesky data collection:")
        logger.info(
            "  - Execution date: %s",
            context.get("execution_date", datetime.now()).strftime("%Y-%m-%d %H:%M"),
        )
        logger.info("  - Keywords: %s", keywords)
        logger.info("  - Mode: Recent posts")

        api_client = SentiCheckAPIClient()

        total_stored = 0

        for keyword in keywords:
            logger.info("Fetching and storing posts for keyword: '%s'", keyword)

            result = api_client.fetch_and_store_bluesky(keyword, "en")
            total_stored += result.get("stored", 0)

            logger.info(
                "Fetched %d posts, stored %d for keyword '%s'",
                result.get("fetched", 0),
                result.get("stored", 0),
                keyword,
            )

        logger.info(
            "Total stored %d posts from Bluesky across %d keywords",
            total_stored,
            len(keywords),
        )

        return {
            "total_fetched": total_stored,
            "total_stored": total_stored,
            "keywords_used": keywords,
            "timestamp": datetime.now().isoformat(),
        }

    except APIError as e:
        logger.error("API error fetching posts: %s", e)
        raise
    except Exception as e:
        logger.error("Error fetching posts: %s", e)
        raise


fetch_and_store_posts_task = PythonOperator(
    task_id="fetch_and_store_posts",
    python_callable=fetch_bluesky_posts,
    dag=dag,
)


trigger_cleaning = TriggerDagRunOperator(
    task_id="trigger_text_cleaning",
    trigger_dag_id="text_cleaning_pipeline",
    wait_for_completion=False,
    dag=dag,
)

fetch_and_store_posts_task >> trigger_cleaning
