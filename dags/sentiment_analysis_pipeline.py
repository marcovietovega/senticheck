"""
Sentiment Analysis Pipeline DAG

Processes cleaned posts through sentiment analysis using a FastAPI service.
Analyzes sentiment via HTTP API calls and stores results in the database.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
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
    "sentiment_analysis_pipeline",
    default_args=default_args,
    description="Analyze sentiment for cleaned posts via FastAPI service and store results",
    schedule=None,
    catchup=False,
    tags=["sentiment-analysis", "fastapi", "http-client", "ml", "senticheck"],
    max_active_runs=1,
)


def analyze_sentiment(**context) -> Dict[str, Any]:
    """Process cleaned posts through sentiment analysis using API service.

    Returns:
        Sentiment analysis results
    """

    try:
        logger.info("Starting sentiment analysis pipeline via API service...")

        api_client = SentiCheckAPIClient()

        result = api_client.analyze_sentiment()
        analyzed_count = result.get("analyzed", 0)

        logger.info("Successfully analyzed %d posts", analyzed_count)

        updated_stats = api_client.get_stats()

        return {
            "status": "success",
            "analyzed_count": analyzed_count,
            "updated_stats": updated_stats,
            "timestamp": datetime.now().isoformat(),
        }

    except APIError as e:
        logger.error("API error in sentiment analysis pipeline: %s", str(e))
        raise
    except Exception as e:
        logger.error("Error in sentiment analysis pipeline: %s", str(e))
        raise


analyze_task = PythonOperator(
    task_id="analyze_sentiment",
    python_callable=analyze_sentiment,
    dag=dag,
)


completion_task = BashOperator(
    task_id="pipeline_completion",
    bash_command="echo 'Sentiment analysis pipeline completed successfully'",
    dag=dag,
)


analyze_task >> completion_task
