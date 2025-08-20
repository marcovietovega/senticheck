"""
FastAPI Sentiment Analysis Service - SentiCheck Project

This service provides a REST API for sentiment analysis using Hugging Face transformers.
It replaces the direct model usage in Airflow to solve deadlock issues and improve scalability.
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from contextlib import asynccontextmanager
import logging
import time
from datetime import datetime
import asyncio
import uvicorn

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import our sentiment analyzer
try:
    import sys
    import os

    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from scripts.sentiment_analyzer import SentimentAnalyzer
except ImportError as e:
    logger.error(f"Failed to import SentimentAnalyzer: {e}")
    SentimentAnalyzer = None

# Global analyzer instance (will be initialized on startup)
analyzer = None

# Service startup time for uptime calculation
service_start_time = time.time()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan event handler for FastAPI application.
    Handles startup and shutdown events in the modern FastAPI way.
    """
    global analyzer, service_start_time

    # Startup events
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
                    f"✅ Sentiment analyzer initialized successfully with model: {analyzer.model_name}"
                )
            else:
                logger.error("❌ Failed to initialize sentiment analyzer")
                analyzer = None
        except Exception as e:
            logger.error(f"❌ Error during startup: {e}")
            analyzer = None

    # Yield control to the application
    yield

    # Shutdown events
    logger.info("Shutting down SentiCheck Sentiment Analysis Service...")

    if analyzer:
        # Clean up any resources if needed
        analyzer = None

    logger.info("✅ Service shutdown complete")


# Initialize FastAPI app with lifespan handler
app = FastAPI(
    title="SentiCheck Sentiment Analysis API",
    description="REST API for sentiment analysis using Hugging Face transformers",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)


# Pydantic models for request/response validation
class TextItem(BaseModel):
    """Single text item for analysis."""

    text: str = Field(
        ..., min_length=1, max_length=10000, description="Text to analyze"
    )
    id: Optional[str] = Field(None, description="Optional identifier for the text")


class BatchTextRequest(BaseModel):
    """Request model for batch text analysis."""

    texts: List[TextItem] = Field(
        ..., min_items=1, max_items=100, description="List of texts to analyze"
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

        logger.info(f"Processing batch of {len(request.texts)} texts...")

        for text_item in request.texts:
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
    # Run the service directly for development
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True, log_level="info")
