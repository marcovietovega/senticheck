from datetime import datetime, timezone
from sqlalchemy import (
    Column,
    Integer,
    String,
    Text,
    DateTime,
    Float,
    Boolean,
    JSON,
    ForeignKey,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class RawPost(Base):
    """Model for storing raw posts from Bluesky."""

    __tablename__ = "raw_posts"

    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)

    # Bluesky specific identifiers
    post_uri = Column(String(500), unique=True, nullable=False, index=True)
    cid = Column(String(100), nullable=False, index=True)

    # Post content
    text = Column(Text, nullable=False)

    # Author information
    author = Column(String(255), nullable=True, default="Unknown")
    author_handle = Column(String(255), nullable=False, index=True)

    # Timestamps
    created_at = Column(DateTime, nullable=False)  # Original post timestamp
    fetched_at = Column(
        DateTime, nullable=False, default=lambda: datetime.now(timezone.utc)
    )  # When we fetched it

    # Search metadata
    search_keyword = Column(
        String(255), nullable=True, index=True
    )  # What keyword was used to find this

    # Processing status
    is_processed = Column(Boolean, default=False, index=True)

    # Relationships
    cleaned_post = relationship("CleanedPost", back_populates="raw_post", uselist=False)

    def __repr__(self):
        return f"<RawPost(id={self.id}, author={self.author}, created_at={self.created_at})>"


class CleanedPost(Base):
    """Model for storing cleaned text data."""

    __tablename__ = "cleaned_posts"

    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)

    # Foreign key to raw post
    raw_post_id = Column(
        Integer, ForeignKey("raw_posts.id"), nullable=False, unique=True, index=True
    )

    # Cleaned content
    cleaned_text = Column(Text, nullable=False)
    original_text = Column(Text, nullable=False)  # Store original for reference

    # Cleaning configuration
    preserve_hashtags = Column(Boolean, default=False)
    preserve_mentions = Column(Boolean, default=False)

    # Timestamps
    cleaned_at = Column(
        DateTime, nullable=False, default=lambda: datetime.now(timezone.utc)
    )

    # Processing status
    is_analyzed = Column(Boolean, default=False, index=True)

    # Relationships
    raw_post = relationship("RawPost", back_populates="cleaned_post")
    sentiment_analysis = relationship(
        "SentimentAnalysis", back_populates="cleaned_post", uselist=False
    )

    def __repr__(self):
        return f"<CleanedPost(id={self.id}, raw_post_id={self.raw_post_id}, cleaned_at={self.cleaned_at})>"


class SentimentAnalysis(Base):
    """Model for storing sentiment analysis results."""

    __tablename__ = "sentiment_analysis"

    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)

    # Foreign key to cleaned post
    cleaned_post_id = Column(
        Integer, ForeignKey("cleaned_posts.id"), nullable=False, unique=True, index=True
    )

    # Sentiment scores
    sentiment_label = Column(
        String(50), nullable=False, index=True
    )  # 'positive', 'negative', 'neutral'
    confidence_score = Column(Float, nullable=False)  # 0.0 to 1.0

    # Detailed scores (if available from model)
    positive_score = Column(Float, nullable=True)
    negative_score = Column(Float, nullable=True)
    neutral_score = Column(Float, nullable=True)

    # Model information
    model_name = Column(String(255), nullable=False)
    model_version = Column(String(100), nullable=True)

    # Timestamps
    analyzed_at = Column(
        DateTime, nullable=False, default=lambda: datetime.now(timezone.utc)
    )

    # Relationships
    cleaned_post = relationship("CleanedPost", back_populates="sentiment_analysis")

    def __repr__(self):
        return f"<SentimentAnalysis(id={self.id}, sentiment={self.sentiment_label}, confidence={self.confidence_score})>"
