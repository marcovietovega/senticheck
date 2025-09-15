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
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.api_client import SentiCheckAPIClient, APIError


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


def clean_posts(**context) -> Dict[str, Any]:
    """
    Process raw posts through text cleaning pipeline via API service.

    Returns:
        Dict with cleaning results
    """
    try:
        logger.info("Starting text cleaning pipeline...")
        logger.info("Processing all unprocessed posts...")

        api_client = SentiCheckAPIClient()
        result = api_client.process_raw_posts()
        processed_count = result.get("processed", 0)

        logger.info("Successfully cleaned %d posts", processed_count)

        if processed_count == 0:
            logger.info("No posts to clean - pipeline complete")
            return {
                "status": "success",
                "cleaned_count": 0,
                "message": "No posts to clean",
            }

        updated_stats = api_client.get_stats()

        return {
            "status": "success",
            "cleaned_count": processed_count,
            "updated_stats": updated_stats,
            "timestamp": datetime.now().isoformat(),
        }

    except APIError as e:
        logger.error("API error cleaning posts: %s", str(e))
        raise
    except Exception as e:
        logger.error("Error cleaning posts: %s", str(e))
        raise


clean_posts_task = PythonOperator(
    task_id="clean_posts",
    python_callable=clean_posts,
    dag=dag,
)

trigger_sentiment_analysis = TriggerDagRunOperator(
    task_id="trigger_sentiment_analysis",
    trigger_dag_id="sentiment_analysis_pipeline",
    wait_for_completion=False,
    dag=dag,
)


clean_posts_task >> trigger_sentiment_analysis
