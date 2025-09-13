"""
FastAPI Sentiment Analysis Service

REST API for sentiment analysis using Hugging Face transformers.
Replaces direct model usage in Airflow to solve deadlock issues.
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import List, Optional
from contextlib import asynccontextmanager
import logging
import time
from datetime import datetime
import uvicorn

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    from .sentiment_analyzer import SentimentAnalyzer
except ImportError as e:
    logger.error(f"Failed to import SentimentAnalyzer: {e}")
    SentimentAnalyzer = None

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
    title="SentiCheck Sentiment Analysis API",
    description="REST API for sentiment analysis using Hugging Face transformers",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)


class TextItem(BaseModel):
    """Single text item for analysis."""

    text: str = Field(..., min_length=1, max_length=500, description="Text to analyze")
    id: Optional[str] = Field(None, description="Optional identifier for the text")


class BatchTextRequest(BaseModel):
    """Request model for batch text analysis."""

    texts: List[TextItem] = Field(
        ..., min_items=1, description="List of texts to analyze (no maximum limit)"
    )
    model_name: Optional[str] = Field(
        "cardiffnlp/twitter-roberta-base-sentiment-latest",
        description="Model name to use for analysis",
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


class BatchSentimentResponse(BaseModel):
    """Response model for batch sentiment analysis."""

    results: List[SentimentResult] = Field(
        ..., description="List of sentiment analysis results"
    )
    total_processed: int = Field(..., description="Number of texts processed")
    total_time_ms: float = Field(
        ..., description="Total processing time in milliseconds"
    )
    model_name: str = Field(..., description="Model used for analysis")


class HealthResponse(BaseModel):
    """Response model for health check."""

    status: str = Field(..., description="Service status")
    model_loaded: bool = Field(..., description="Whether the model is loaded")
    model_name: Optional[str] = Field(None, description="Currently loaded model name")
    uptime_seconds: float = Field(..., description="Service uptime in seconds")
    timestamp: str = Field(..., description="Current timestamp")


# Service startup time for uptime calculation
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


@app.post("/analyze/single", response_model=SentimentResult)
async def analyze_single_text(text_item: TextItem):
    """
    Analyze sentiment for a single text.

    Args:
        text_item: TextItem containing the text to analyze

    Returns:
        SentimentResult: Sentiment analysis result
    """
    global analyzer

    if not analyzer or not analyzer.is_initialized:
        raise HTTPException(
            status_code=503,
            detail="Sentiment analyzer not available. Check service health.",
        )

    try:
        start_time = time.time()

        # Perform sentiment analysis
        result = analyzer.analyze_text(text_item.text)

        if not result:
            raise HTTPException(
                status_code=422,
                detail="Failed to analyze text. Text may be empty or invalid.",
            )

        processing_time = (time.time() - start_time) * 1000  # Convert to milliseconds

        return SentimentResult(
            text_id=text_item.id,
            sentiment_label=result["sentiment_label"],
            confidence_score=result["confidence_score"],
            positive_score=result.get("positive_score"),
            negative_score=result.get("negative_score"),
            neutral_score=result.get("neutral_score"),
            model_name=result["model_name"],
            processing_time_ms=processing_time,
        )

    except Exception as e:
        logger.error(f"Error analyzing single text: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error during sentiment analysis: {str(e)}",
        )


@app.post("/analyze/batch", response_model=BatchSentimentResponse)
async def analyze_batch_texts(request: BatchTextRequest):
    """
    Analyze sentiment for a batch of texts.

    Args:
        request: BatchTextRequest containing texts to analyze

    Returns:
        BatchSentimentResponse: Batch sentiment analysis results
    """
    global analyzer

    if not analyzer or not analyzer.is_initialized:
        raise HTTPException(
            status_code=503,
            detail="Sentiment analyzer not available. Check service health.",
        )

    try:
        start_time = time.time()
        results = []
        batch_size = len(request.texts)

        logger.info(f"Processing batch of {batch_size} texts...")

        # Add warning for very large batches
        if batch_size > 5000:
            logger.warning(
                f"Processing very large batch ({batch_size} items) - this may take several minutes"
            )

        # Progress tracking for large batches
        progress_interval = max(
            100, batch_size // 10
        )  # Report progress every 10% or every 100 items

        for idx, text_item in enumerate(request.texts, 1):
            try:
                item_start_time = time.time()

                # Perform sentiment analysis for this text
                analysis_result = analyzer.analyze_text(text_item.text)

                if analysis_result:
                    item_processing_time = (time.time() - item_start_time) * 1000

                    results.append(
                        SentimentResult(
                            text_id=text_item.id,
                            sentiment_label=analysis_result["sentiment_label"],
                            confidence_score=analysis_result["confidence_score"],
                            positive_score=analysis_result.get("positive_score"),
                            negative_score=analysis_result.get("negative_score"),
                            neutral_score=analysis_result.get("neutral_score"),
                            model_name=analysis_result["model_name"],
                            processing_time_ms=item_processing_time,
                        )
                    )

                    # Log progress for large batches
                    if batch_size > 1000 and idx % progress_interval == 0:
                        elapsed_time = time.time() - start_time
                        progress_pct = (idx / batch_size) * 100
                        logger.info(
                            f"Progress: {idx}/{batch_size} ({progress_pct:.1f}%) processed in {elapsed_time:.1f}s"
                        )

                else:
                    logger.warning(f"Failed to analyze text: {text_item.text[:50]}...")

            except Exception as e:
                logger.error(f"Error analyzing text '{text_item.text[:50]}...': {e}")
                continue

        total_time = (time.time() - start_time) * 1000

        logger.info(
            f"Batch processing complete: {len(results)}/{len(request.texts)} texts processed in {total_time:.2f}ms"
        )

        return BatchSentimentResponse(
            results=results,
            total_processed=len(results),
            total_time_ms=total_time,
            model_name=analyzer.model_name,
        )

    except Exception as e:
        logger.error(f"Error in batch analysis: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error during batch sentiment analysis: {str(e)}",
        )


@app.get("/model/info")
async def get_model_info():
    """
    Get information about the currently loaded model.

    Returns:
        Dict: Model information
    """
    global analyzer

    if not analyzer or not analyzer.is_initialized:
        raise HTTPException(
            status_code=503,
            detail="Sentiment analyzer not available. Check service health.",
        )

    try:
        model_info = analyzer.get_model_info()
        return {
            "model_info": model_info,
            "service_status": "running",
            "timestamp": datetime.now().isoformat(),
        }
    except Exception as e:
        logger.error(f"Error getting model info: {e}")
        raise HTTPException(
            status_code=500, detail=f"Error retrieving model information: {str(e)}"
        )


if __name__ == "__main__":
    try:
        from config import config

        host = config.host if config.host != "localhost" else "0.0.0.0"
        port = config.port
    except ImportError:
        import os

        host = "0.0.0.0"
        port = int(os.getenv("SENTIMENT_SERVICE_PORT", "8000"))

    uvicorn.run("main:app", host=host, port=port, reload=True, log_level="info")
