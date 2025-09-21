import azure.functions as func
import datetime
import json
import logging
import os

app = func.FunctionApp()

from utils.api_client import SentiCheckAPIClient, APIError


@app.timer_trigger(
    schedule="0 0 * * * *", arg_name="mytimer", run_on_startup=False, use_monitor=False
)
def data_pipeline_orchestrator(mytimer: func.TimerRequest) -> None:
    try:
        keywords_var = os.environ.get("BLUESKY_SEARCH_KEYWORDS", "AI")

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

        logging.info("Starting Bluesky data collection:")
        logging.info(
            "  - Execution time: %s", datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        )
        logging.info("  - Keywords: %s", keywords)
        logging.info("  - Mode: Recent posts")

        api_client = SentiCheckAPIClient()

        total_stored = 0

        for keyword in keywords:
            logging.info("Fetching and storing posts for keyword: '%s'", keyword)

            result = api_client.fetch_and_store_bluesky(keyword, "en")
            total_stored += result.get("stored", 0)

            logging.info(
                "Fetched %d posts, stored %d for keyword '%s'",
                result.get("fetched", 0),
                result.get("stored", 0),
                keyword,
            )

        logging.info(
            "Total stored %d posts from Bluesky across %d keywords",
            total_stored,
            len(keywords),
        )

        if total_stored > 0:
            logging.info("Starting text cleaning pipeline...")
            clean_result = api_client.process_raw_posts()
            cleaned_count = clean_result.get("processed", 0)
            logging.info("Successfully cleaned %d posts", cleaned_count)

            if cleaned_count > 0:
                logging.info("Starting sentiment analysis...")
                sentiment_result = api_client.analyze_sentiment()
                analyzed_count = sentiment_result.get("analyzed", 0)
                logging.info("Successfully analyzed %d posts", analyzed_count)
            else:
                logging.info("No posts to analyze - skipping sentiment analysis")
        else:
            logging.info("No new posts - skipping text cleaning and sentiment analysis")

        logging.info("Data pipeline completed successfully!")

    except APIError as e:
        logging.error("API error in pipeline: %s", e)
        raise
    except Exception as e:
        logging.error("Error in pipeline: %s", e)
        raise
