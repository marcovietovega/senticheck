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

    id = Column(Integer, primary_key=True, autoincrement=True)
    post_uri = Column(String(500), unique=True, nullable=False, index=True)
    cid = Column(String(100), nullable=False, index=True)
    text = Column(Text, nullable=False)
    author = Column(String(255), nullable=True, default="Unknown")
    author_handle = Column(String(255), nullable=False, index=True)
    created_at = Column(DateTime, nullable=False)
    fetched_at = Column(
        DateTime, nullable=False, default=lambda: datetime.now(timezone.utc)
    )
    search_keyword = Column(String(255), nullable=True, index=True)
    is_processed = Column(Boolean, default=False, index=True)
    cleaned_post = relationship("CleanedPost", back_populates="raw_post", uselist=False)

    def __repr__(self):
        return f"<RawPost(id={self.id}, author={self.author}, created_at={self.created_at})>"


class CleanedPost(Base):
    """Model for storing cleaned text data."""

    __tablename__ = "cleaned_posts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    raw_post_id = Column(
        Integer, ForeignKey("raw_posts.id"), nullable=False, unique=True, index=True
    )
    cleaned_text = Column(Text, nullable=False)
    original_text = Column(Text, nullable=False)
    preserve_hashtags = Column(Boolean, default=False)
    preserve_mentions = Column(Boolean, default=False)
    cleaning_metadata = Column(JSON, nullable=True)
    cleaned_at = Column(
        DateTime, nullable=False, default=lambda: datetime.now(timezone.utc)
    )
    is_analyzed = Column(Boolean, default=False, index=True)
    raw_post = relationship("RawPost", back_populates="cleaned_post")
    sentiment_analysis = relationship(
        "SentimentAnalysis", back_populates="cleaned_post", uselist=False
    )

    def __repr__(self):
        return f"<CleanedPost(id={self.id}, raw_post_id={self.raw_post_id}, cleaned_at={self.cleaned_at})>"


class SentimentAnalysis(Base):
    """Model for storing sentiment analysis results."""

    __tablename__ = "sentiment_analysis"

    id = Column(Integer, primary_key=True, autoincrement=True)
    cleaned_post_id = Column(
        Integer, ForeignKey("cleaned_posts.id"), nullable=False, unique=True, index=True
    )
    sentiment_label = Column(String(50), nullable=False, index=True)
    confidence_score = Column(Float, nullable=False)
    positive_score = Column(Float, nullable=True)
    negative_score = Column(Float, nullable=True)
    neutral_score = Column(Float, nullable=True)
    model_name = Column(String(255), nullable=False)
    model_version = Column(String(100), nullable=True)
    analyzed_at = Column(
        DateTime, nullable=False, default=lambda: datetime.now(timezone.utc)
    )
    search_keyword = Column(String(255), nullable=True, index=True)
    cleaned_post = relationship("CleanedPost", back_populates="sentiment_analysis")

    def __repr__(self):
        return f"<SentimentAnalysis(id={self.id}, sentiment={self.sentiment_label}, confidence={self.confidence_score})>"
