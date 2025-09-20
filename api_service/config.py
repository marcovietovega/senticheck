"""
Configuration settings for the SentiCheck ML Service

Contains configuration constants and environment variable handling
for both the FastAPI service and the client.
"""

import os
from dotenv import load_dotenv

load_dotenv()

FALLBACK_SERVICE_HOST = "localhost"
FALLBACK_SERVICE_PORT = 8000
FALLBACK_MODEL_NAME = "cardiffnlp/twitter-roberta-base-sentiment-latest"
FALLBACK_CLIENT_TIMEOUT = 30.0
FALLBACK_MAX_RETRIES = 3
FALLBACK_RETRY_DELAY = 1.0
FALLBACK_BATCH_SIZE = 1000
FALLBACK_TEXT_LENGTH = 500


MAX_BATCH_SIZE = 1000
MAX_TEXT_LENGTH = 500


class ServiceConfig:
    """Configuration class for the sentiment analysis service."""

    def __init__(self):
        """Initialize configuration from environment variables with defaults."""

        self.host = os.getenv("SENTIMENT_SERVICE_HOST", FALLBACK_SERVICE_HOST)
        self.port = int(os.getenv("SENTIMENT_SERVICE_PORT", FALLBACK_SERVICE_PORT))
        self.base_url = f"http://{self.host}:{self.port}"

        self.model_name = os.getenv("SENTIMENT_MODEL_NAME", FALLBACK_MODEL_NAME)

        self.timeout = float(
            os.getenv("SENTIMENT_CLIENT_TIMEOUT", FALLBACK_CLIENT_TIMEOUT)
        )
        self.max_retries = int(os.getenv("SENTIMENT_MAX_RETRIES", FALLBACK_MAX_RETRIES))
        self.retry_delay = float(
            os.getenv("SENTIMENT_RETRY_DELAY", FALLBACK_RETRY_DELAY)
        )

        self.batch_size = int(os.getenv("SENTIMENT_BATCH_SIZE", FALLBACK_BATCH_SIZE))
        self.max_text_length = int(
            os.getenv("SENTIMENT_MAX_TEXT_LENGTH", FALLBACK_TEXT_LENGTH)
        )

    def get_service_url(self) -> str:
        """Get the full service URL."""
        return self.base_url

    def __str__(self) -> str:
        """String representation of configuration."""
        return (
            f"ServiceConfig("
            f"url={self.base_url}, "
            f"model={self.model_name}, "
            f"timeout={self.timeout}s, "
            f"batch_size={self.batch_size}"
            f")"
        )


# Global configuration instance
config = ServiceConfig()


def get_service_url() -> str:
    """Get the sentiment analysis service URL."""
    return config.get_service_url()


def get_batch_size() -> int:
    """Get the configured batch size for processing."""
    return config.batch_size


def get_model_name() -> str:
    """Get the configured model name."""
    return config.model_name


def update_config_from_airflow_variables(variables_getter) -> None:
    """
    Update configuration from Airflow Variables.

    Args:
        variables_getter: Function to get Airflow Variables (usually Variable.get)
    """
    try:
        service_host = variables_getter(
            "sentiment_service_host", default_var=config.host
        )
        service_port = int(
            variables_getter("sentiment_service_port", default_var=config.port)
        )

        if service_host != config.host or service_port != config.port:
            config.host = service_host
            config.port = service_port
            config.base_url = f"http://{config.host}:{config.port}"

        config.model_name = variables_getter(
            "sentiment_model_name", default_var=config.model_name
        )

        config.batch_size = int(
            variables_getter("sentiment_batch_size", default_var=config.batch_size)
        )

        config.batch_size = min(config.batch_size, MAX_BATCH_SIZE)

    except Exception as e:
        pass
