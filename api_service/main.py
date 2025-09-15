"""
FastAPI Sentiment Analysis Service

REST API for sentiment analysis using Hugging Face transformers.
Replaces direct model usage in Airflow to solve deadlock issues.
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import Optional
from contextlib import asynccontextmanager
import logging
import time
from datetime import datetime
import uvicorn


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from api_service.utils.sentiment_analyzer import SentimentAnalyzer
from api_service.services.database_service import get_database_service
from api_service.services.bluesky_service import get_bluesky_service
from api_service.config import config

analyzer = None
service_start_time = time.time()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan event handler for FastAPI application."""
    global analyzer, service_start_time

    logger.info("Starting SentiCheck Sentiment Analysis Service...")
    service_start_time = time.time()

    if SentimentAnalyzer is None:
        logger.error("SentimentAnalyzer class not available - check imports")
    else:
        try:
            logger.info("Initializing sentiment analyzer...")
            analyzer = SentimentAnalyzer()

            if analyzer.initialize():
                logger.info(
                    f"Sentiment analyzer initialized successfully with model: {analyzer.model_name}"
                )
            else:
                logger.error("Failed to initialize sentiment analyzer")
                analyzer = None
        except Exception as e:
            logger.error(f"Error during startup: {e}")
            analyzer = None

    yield
    logger.info("Shutting down SentiCheck Sentiment Analysis Service...")

    if analyzer:
        analyzer = None

    logger.info("Service shutdown complete")


app = FastAPI(
    title="SentiCheck API Service",
    description="Unified API for sentiment analysis, data operations, and social media connectors",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)


class SentimentResult(BaseModel):
    """Response model for sentiment analysis result."""

    text_id: Optional[str] = Field(None, description="Text identifier if provided")
    sentiment_label: str = Field(..., description="Predicted sentiment label")
    confidence_score: float = Field(..., ge=0.0, le=1.0, description="Confidence score")
    positive_score: Optional[float] = Field(
        None, ge=0.0, le=1.0, description="Positive sentiment score"
    )
    negative_score: Optional[float] = Field(
        None, ge=0.0, le=1.0, description="Negative sentiment score"
    )
    neutral_score: Optional[float] = Field(
        None, ge=0.0, le=1.0, description="Neutral sentiment score"
    )
    model_name: str = Field(..., description="Model used for analysis")
    processing_time_ms: float = Field(
        ..., description="Processing time in milliseconds"
    )


class HealthResponse(BaseModel):
    """Response model for health check."""

    status: str = Field(..., description="Service status")
    model_loaded: bool = Field(..., description="Whether the model is loaded")
    model_name: Optional[str] = Field(None, description="Currently loaded model name")
    uptime_seconds: float = Field(..., description="Service uptime in seconds")
    timestamp: str = Field(..., description="Current timestamp")


service_start_time = time.time()


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """
    Health check endpoint to verify service status.

    Returns:
        HealthResponse: Service health information
    """
    global analyzer

    uptime = time.time() - service_start_time

    return HealthResponse(
        status="healthy" if analyzer and analyzer.is_initialized else "unhealthy",
        model_loaded=analyzer is not None and analyzer.is_initialized,
        model_name=analyzer.model_name if analyzer else None,
        uptime_seconds=uptime,
        timestamp=datetime.now().isoformat(),
    )


@app.get("/data/stats")
async def get_database_stats():
    try:
        db_service = get_database_service()
        return db_service.get_database_stats()
    except Exception as e:
        logger.error(f"Error getting database stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/data/keywords")
async def get_keywords_with_counts():
    try:
        db_service = get_database_service()
        return db_service.get_keywords_with_counts()
    except Exception as e:
        logger.error(f"Error getting keywords: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/data/sentiment/distribution")
async def get_sentiment_distribution(search_keyword: str, days: int = 30):
    try:
        db_service = get_database_service()
        return db_service.get_sentiment_distribution(search_keyword, days)
    except Exception as e:
        logger.error(f"Error getting sentiment distribution: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/data/sentiment/over_time")
async def get_sentiment_over_time(search_keyword: str, days: int = 30):
    try:
        db_service = get_database_service()
        return db_service.get_sentiment_over_time(search_keyword, days)
    except Exception as e:
        logger.error(f"Error getting sentiment over time: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/data/sentiment/trends")
async def calculate_sentiment_trends():
    try:
        db_service = get_database_service()
        return db_service.calculate_sentiment_trends()
    except Exception as e:
        logger.error(f"Error calculating sentiment trends: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/data/posts/by_date")
async def get_posts_by_date(search_keyword: str, days: int = 2):
    try:
        db_service = get_database_service()
        return db_service.get_posts_by_date(search_keyword, days)
    except Exception as e:
        logger.error(f"Error getting posts by date: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/data/metrics/keyword/{keyword}")
async def get_keyword_metrics(keyword: str, days: int = 30):
    try:
        db_service = get_database_service()

        basic_metrics = db_service.get_keyword_specific_metrics(keyword, days)

        advanced_kpis = db_service.get_keyword_specific_kpis(keyword, days)

        combined_data = {**basic_metrics, **advanced_kpis}

        return combined_data
    except Exception as e:
        logger.error(f"Error getting keyword metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/data/text_analysis")
async def get_text_analysis(keyword: str, days: int):
    try:
        db_service = get_database_service()
        return db_service.get_text_analysis_for_keyword(keyword, days)
    except Exception as e:
        logger.error(f"Error getting text analysis: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/connector/bluesky/fetch_and_store")
async def fetch_and_store_bluesky_posts(keyword: str = "AI", lang: str = "en"):
    try:
        bluesky_service = get_bluesky_service()
        if not bluesky_service.connect():
            raise HTTPException(status_code=503, detail="Failed to connect to Bluesky")

        posts = bluesky_service.fetch_posts(keyword, lang)
        bluesky_service.disconnect()

        if posts:
            for post in posts:
                post["search_keyword"] = keyword

            db_service = get_database_service()
            stored_count = db_service.store_raw_posts(posts)

            return {
                "fetched": len(posts),
                "stored": stored_count,
                "keyword": keyword,
                "language": lang,
                "timestamp": datetime.now().isoformat(),
            }
        else:
            return {
                "fetched": 0,
                "stored": 0,
                "keyword": keyword,
                "language": lang,
                "timestamp": datetime.now().isoformat(),
            }

    except Exception as e:
        logger.error(f"Error fetching and storing Bluesky posts: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/pipeline/process_raw_posts")
async def process_raw_posts():
    try:
        db_service = get_database_service()
        processed_count = db_service.process_raw_posts_to_cleaned()

        return {
            "processed": processed_count,
            "timestamp": datetime.now().isoformat(),
        }
    except Exception as e:
        logger.error(f"Error processing raw posts: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/pipeline/analyze_sentiment")
async def analyze_sentiment_posts(
    limit: int = 1000,
    model_name: str = "cardiffnlp/twitter-roberta-base-sentiment-latest",
):
    try:
        db_service = get_database_service()
        analyzed_count = db_service.analyze_cleaned_posts_sentiment(
            model_name=model_name,
            limit=limit,
        )

        return {
            "analyzed": analyzed_count,
            "limit": limit,
            "model": model_name,
            "timestamp": datetime.now().isoformat(),
        }
    except Exception as e:
        logger.error(f"Error analyzing sentiment: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    try:
        host = config.host if config.host != "localhost" else "0.0.0.0"
        port = config.port
    except ImportError:
        import os

        host = "0.0.0.0"
        port = int(os.getenv("SENTIMENT_SERVICE_PORT", "8000"))

    uvicorn.run("main:app", host=host, port=port, reload=True, log_level="info")
