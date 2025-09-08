from datetime import datetime, timedelta, timezone
import logging
from typing import Optional, List, Dict, Any, Tuple
from sqlalchemy import func, case


from .database import (
    RawPost,
    CleanedPost,
    SentimentAnalysis,
)
from .db_connection import get_db_connection


logger = logging.getLogger(__name__)


class DatabaseOperations:
    """Handles data operations for SentiCheck."""

    def __init__(self):
        self.db_connection = get_db_connection()

    def store_raw_posts(self, posts_data: List[Dict]) -> int:
        """Store raw posts in the database.

        Args:
            posts_data: List of post dictionaries (each should have search_keyword)

        Returns:
            Number of posts stored (excluding duplicates)
        """
        if not posts_data:
            logger.info("No posts to store")
            return 0

        # Try batch insert first
        try:
            return self._store_raw_posts_batch(posts_data)
        except Exception as e:
            logger.warning(
                f"Batch insert failed, falling back to individual inserts: {e}"
            )
            return self._store_raw_posts_individual(posts_data)

    def _store_raw_posts_batch(self, posts_data: List[Dict]) -> int:
        """Batch insert posts using PostgreSQL ON CONFLICT DO NOTHING."""
        from sqlalchemy.dialects.postgresql import insert

        stored_count = 0

        with self.db_connection.get_session() as session:
            # Prepare data for batch insert
            insert_data = []
            for post_data in posts_data:
                insert_data.append(
                    {
                        "post_uri": post_data.get("post_uri", ""),
                        "cid": post_data.get("cid", ""),
                        "text": post_data.get("text", ""),
                        "author": post_data.get("author") or "Unknown",
                        "author_handle": post_data.get("author_handle", ""),
                        "created_at": post_data.get("timestamp")
                        or post_data.get("fetched_at"),
                        "fetched_at": post_data.get("fetched_at"),
                        "search_keyword": post_data.get("search_keyword"),
                        "is_processed": False,
                    }
                )

            # Handle duplicates with ON CONFLICT
            stmt = insert(RawPost).values(insert_data)
            stmt = stmt.on_conflict_do_nothing(index_elements=["post_uri"])

            result = session.execute(stmt)
            stored_count = result.rowcount

        logger.info(
            f"Batch stored {stored_count} new posts out of {len(posts_data)} total"
        )
        return stored_count

    def _store_raw_posts_individual(self, posts_data: List[Dict]) -> int:
        """Store posts individually with duplicate checking."""
        stored_count = 0
        skipped_count = 0

        for post_data in posts_data:
            try:
                with self.db_connection.get_session() as session:
                    # Check if post exists
                    existing_post = (
                        session.query(RawPost)
                        .filter_by(post_uri=post_data.get("post_uri", ""))
                        .first()
                    )

                    if existing_post:
                        logger.debug(
                            f"Post already exists: {post_data.get('post_uri', '')}"
                        )
                        skipped_count += 1
                        continue

                    # Create new post
                    raw_post = RawPost(
                        post_uri=post_data.get("post_uri", ""),
                        cid=post_data.get("cid", ""),
                        text=post_data.get("text", ""),
                        author=post_data.get("author") or "Unknown",
                        author_handle=post_data.get("author_handle", ""),
                        created_at=post_data.get("timestamp")
                        or post_data.get("fetched_at"),
                        fetched_at=post_data.get("fetched_at"),
                        search_keyword=post_data.get("search_keyword"),
                    )

                    session.add(raw_post)
                    # Session commits when exiting context
                    stored_count += 1

            except Exception as e:
                logger.warning(
                    f"Failed to store post {post_data.get('post_uri', 'unknown')}: {e}"
                )
                continue

        logger.info(
            f"Individual stored {stored_count} new posts, skipped {skipped_count} duplicates out of {len(posts_data)} total"
        )
        return stored_count

    def get_unprocessed_posts(self, limit: Optional[int] = 100) -> List[RawPost]:
        """
        Get raw posts that haven't been cleaned yet.

        Args:
            limit: Maximum number of posts to return. If None, returns all unprocessed posts.

        Returns:
            List[RawPost]: List of unprocessed raw posts
        """
        with self.db_connection.get_session() as session:
            query = session.query(RawPost).filter_by(is_processed=False)
            if limit is not None:
                query = query.limit(limit)
            posts = query.all()
            session.expunge_all()
            return posts

    def store_cleaned_post(
        self,
        raw_post_id: int,
        cleaned_text: str,
        original_text: str,
        cleaning_metadata: Dict,
        preserve_hashtags: bool = False,
        preserve_mentions: bool = False,
    ) -> Optional[int]:
        """
        Store cleaned post data.

        Args:
            raw_post_id: ID of the raw post
            cleaned_text: The cleaned text
            original_text: The original text
            cleaning_metadata: Metadata about the cleaning process
            preserve_hashtags: Whether hashtags were preserved
            preserve_mentions: Whether mentions were preserved

        Returns:
            ID of the created cleaned post, or None if failed
        """
        try:
            with self.db_connection.get_session() as session:
                cleaned_post = CleanedPost(
                    raw_post_id=raw_post_id,
                    cleaned_text=cleaned_text,
                    original_text=original_text,
                    cleaning_metadata=cleaning_metadata,
                    preserve_hashtags=preserve_hashtags,
                    preserve_mentions=preserve_mentions,
                )

                session.add(cleaned_post)
                session.flush()

                raw_post = session.query(RawPost).filter_by(id=raw_post_id).first()
                if raw_post:
                    raw_post.is_processed = True

                cleaned_post_id = cleaned_post.id

            logger.debug(f"Stored cleaned post with ID: {cleaned_post_id}")
            return cleaned_post_id

        except Exception as e:
            logger.error(f"Failed to store cleaned post: {e}")
            return None

    def get_unanalyzed_posts(self, limit: Optional[int] = 100) -> List[CleanedPost]:
        """
        Get cleaned posts that haven't been analyzed for sentiment yet.

        Args:
            limit: Maximum number of posts to return. If None, returns all unanalyzed posts.

        Returns:
            List[CleanedPost]: List of unanalyzed cleaned posts
        """
        with self.db_connection.get_session() as session:
            query = session.query(CleanedPost).filter_by(is_analyzed=False)
            if limit is not None:
                query = query.limit(limit)
            posts = query.all()

            session.expunge_all()
            return posts

    def store_sentiment_analysis(
        self,
        cleaned_post_id: int,
        sentiment_label: str,
        confidence_score: float,
        positive_score: float = None,
        negative_score: float = None,
        neutral_score: float = None,
        model_name: str = "unknown",
        model_version: str = None,
        search_keyword: str = None,
    ) -> Optional[int]:
        """
        Store sentiment analysis results.

        Args:
            cleaned_post_id: ID of the cleaned post
            sentiment_label: The predicted sentiment ('positive', 'negative', 'neutral')
            confidence_score: Confidence score (0.0 to 1.0)
            positive_score: Positive sentiment score
            negative_score: Negative sentiment score
            neutral_score: Neutral sentiment score
            model_name: Name of the model used
            model_version: Version of the model

        Returns:
            ID of the created sentiment analysis, or None if failed
        """
        try:
            with self.db_connection.get_session() as session:
                # Get search_keyword from raw_post if not provided
                if search_keyword is None:
                    cleaned_post = session.query(CleanedPost).filter_by(id=cleaned_post_id).first()
                    if cleaned_post and cleaned_post.raw_post:
                        search_keyword = cleaned_post.raw_post.search_keyword
                
                sentiment_analysis = SentimentAnalysis(
                    cleaned_post_id=cleaned_post_id,
                    sentiment_label=sentiment_label,
                    confidence_score=confidence_score,
                    positive_score=positive_score,
                    negative_score=negative_score,
                    neutral_score=neutral_score,
                    model_name=model_name,
                    model_version=model_version,
                    search_keyword=search_keyword,
                )

                session.add(sentiment_analysis)
                session.flush()

                cleaned_post = (
                    session.query(CleanedPost).filter_by(id=cleaned_post_id).first()
                )
                if cleaned_post:
                    cleaned_post.is_analyzed = True

                sentiment_analysis_id = sentiment_analysis.id

            logger.debug(f"Stored sentiment analysis with ID: {sentiment_analysis_id}")
            return sentiment_analysis_id

        except Exception as e:
            logger.error(f"Failed to store sentiment analysis: {e}")
            return None

    def store_sentiment_analysis_batch(self, sentiment_results: List[Dict]) -> int:
        """
        Store multiple sentiment analysis results in batch.

        Args:
            sentiment_results: List of sentiment analysis dictionaries with:
                - cleaned_post_id: int
                - sentiment_label: str
                - confidence_score: float
                - positive_score: float (optional)
                - negative_score: float (optional)
                - neutral_score: float (optional)
                - model_name: str
                - model_version: str (optional)

        Returns:
            Number of sentiment analyses stored
        """
        stored_count = 0

        with self.db_connection.get_session() as session:
            for result in sentiment_results:
                try:
                    # Get search_keyword from result or fetch from raw_post
                    search_keyword = result.get("search_keyword")
                    if search_keyword is None:
                        cleaned_post = (
                            session.query(CleanedPost)
                            .filter_by(id=result["cleaned_post_id"])
                            .first()
                        )
                        if cleaned_post and cleaned_post.raw_post:
                            search_keyword = cleaned_post.raw_post.search_keyword
                    
                    sentiment_analysis = SentimentAnalysis(
                        cleaned_post_id=result["cleaned_post_id"],
                        sentiment_label=result["sentiment_label"],
                        confidence_score=result["confidence_score"],
                        positive_score=result.get("positive_score"),
                        negative_score=result.get("negative_score"),
                        neutral_score=result.get("neutral_score"),
                        model_name=result.get("model_name", "unknown"),
                        model_version=result.get("model_version"),
                        search_keyword=search_keyword,
                    )

                    session.add(sentiment_analysis)

                    cleaned_post = (
                        session.query(CleanedPost)
                        .filter_by(id=result["cleaned_post_id"])
                        .first()
                    )
                    if cleaned_post:
                        cleaned_post.is_analyzed = True

                    stored_count += 1

                except Exception as e:
                    logger.error(
                        f"Failed to store sentiment analysis for post {result.get('cleaned_post_id')}: {e}"
                    )
                    continue

        logger.info(
            f"Stored {stored_count} sentiment analyses out of {len(sentiment_results)} total"
        )
        return stored_count

    def get_database_stats(self) -> Dict[str, Any]:
        """Get database statistics using SentimentAnalysis as source of truth for analyzed posts.

        Returns:
            Database statistics
        """
        try:
            with self.db_connection.get_session() as session:
                analyzed_posts_count = session.query(SentimentAnalysis).count()

                raw_posts_count = session.query(RawPost).count()
                cleaned_posts_count = session.query(CleanedPost).count()

                unprocessed_posts = (
                    session.query(RawPost).filter_by(is_processed=False).count()
                )
                unanalyzed_posts = (
                    session.query(CleanedPost).filter_by(is_analyzed=False).count()
                )

                return {
                    "raw_posts": raw_posts_count,
                    "cleaned_posts": cleaned_posts_count,
                    "analyzed_posts": analyzed_posts_count,
                    "unprocessed_posts": unprocessed_posts,
                    "unanalyzed_posts": unanalyzed_posts,
                }

        except Exception as e:
            logger.error(f"Failed to get database stats: {e}")
            return {}

    def get_sentiment_distribution(self) -> List[Tuple[str, int]]:
        """Get sentiment distribution.

        Returns:
            List of tuples containing sentiment labels and their counts
        """
        with self.db_connection.get_session() as session:
            result = (
                session.query(
                    SentimentAnalysis.sentiment_label,
                    func.count(SentimentAnalysis.id).label("count"),
                )
                .group_by(SentimentAnalysis.sentiment_label)
                .order_by(SentimentAnalysis.sentiment_label)
                .all()
            )
            return [(row.sentiment_label, row.count) for row in result]

    def get_sentiment_over_time(self, days: int) -> List[Dict[str, Any]]:
        """Get sentiment over time.

        Args:
            days: Number of days to look back

        Returns:
            List of dictionaries containing sentiment data over time
        """
        with self.db_connection.get_session() as session:
            end_date = datetime.now(timezone.utc).date()
            start_date = end_date - timedelta(days=days)

            result = (
                session.query(
                    func.date(SentimentAnalysis.analyzed_at).label("date"),
                    func.count(
                        func.case(
                            [(SentimentAnalysis.sentiment_label == "positive", 1)]
                        )
                    ).label("positive"),
                    func.count(
                        func.case(
                            [(SentimentAnalysis.sentiment_label == "negative", 1)]
                        )
                    ).label("negative"),
                    func.count(
                        func.case([(SentimentAnalysis.sentiment_label == "neutral", 1)])
                    ).label("neutral"),
                )
                .filter(func.date(SentimentAnalysis.analyzed_at) >= start_date)
                .group_by(func.date(SentimentAnalysis.analyzed_at))
                .order_by(func.date(SentimentAnalysis.analyzed_at))
                .all()
            )

            return [
                {
                    "date": row.date,
                    "positive": row.positive,
                    "negative": row.negative,
                    "neutral": row.neutral,
                }
                for row in result
            ]

    def get_average_confidence(self) -> float:
        """Get average confidence score.

        Returns:
            Average confidence score
        """
        with self.db_connection.get_session() as session:
            result = session.query(
                func.avg(SentimentAnalysis.confidence_score)
            ).scalar()

            return float((result or 0.0) * 100)

    def get_today_posts_count(self) -> int:
        """Get today's post count.

        Returns:
            Count of today's posts
        """
        with self.db_connection.get_session() as session:
            today = datetime.now(timezone.utc).date()
            result = (
                session.query(func.count(SentimentAnalysis.id))
                .filter(func.date(SentimentAnalysis.analyzed_at) == today)
                .scalar()
            )
            return int(result or 0)

    def get_posts_by_date_range(self, days: int) -> List[Tuple[str, int]]:
        """Get post counts by date for the last N days.

        Args:
            days: Number of days to look back

        Returns:
            List of tuples containing date and post count
        """
        with self.db_connection.get_session() as session:
            end_date = datetime.now(timezone.utc).date()
            start_date = end_date - timedelta(days=days)

            result = (
                session.query(
                    func.date(SentimentAnalysis.analyzed_at).label("date"),
                    func.count(SentimentAnalysis.id).label("count"),
                )
                .filter(func.date(SentimentAnalysis.analyzed_at) >= start_date)
                .group_by(func.date(SentimentAnalysis.analyzed_at))
                .order_by(func.date(SentimentAnalysis.analyzed_at))
                .all()
            )

            return [(str(row.date), row.count) for row in result]

    def get_keywords_with_counts(self) -> List[tuple]:
        """
        Get all available keywords with their analyzed post counts.

        Returns:
            List of tuples (keyword, count)
        """
        try:
            with self.db_connection.get_session() as session:
                result = (
                    session.query(
                        SentimentAnalysis.search_keyword,
                        func.count(SentimentAnalysis.id).label("post_count"),
                    )
                    .filter(SentimentAnalysis.search_keyword.isnot(None))
                    .group_by(SentimentAnalysis.search_keyword)
                    .order_by(func.count(SentimentAnalysis.id).desc())
                    .all()
                )
                return [(row.search_keyword, row.post_count) for row in result]
        except Exception as e:
            logger.error(f"Error getting keywords with counts: {e}")
            return []

    def get_keyword_specific_metrics(self, keyword: str) -> Dict[str, Any]:
        """
        Get sentiment metrics for a specific keyword.

        Args:
            keyword: The keyword to analyze

        Returns:
            Dictionary with keyword-specific metrics
        """
        try:
            with self.db_connection.get_session() as session:
                sentiment_result = (
                    session.query(
                        SentimentAnalysis.sentiment_label,
                        func.count(SentimentAnalysis.id).label("count"),
                        func.avg(SentimentAnalysis.confidence_score).label("avg_conf"),
                    )
                    .filter(SentimentAnalysis.search_keyword == keyword)
                    .group_by(SentimentAnalysis.sentiment_label)
                    .all()
                )

                total_posts = 0
                sentiment_counts = {"positive": 0, "negative": 0, "neutral": 0}
                total_confidence = 0

                for row in sentiment_result:
                    sentiment = row.sentiment_label
                    count = row.count
                    avg_conf = row.avg_conf or 0

                    sentiment_counts[sentiment] = count
                    total_posts += count
                    total_confidence += avg_conf * count

                today = datetime.now(timezone.utc).date()
                posts_today = (
                    session.query(func.count(SentimentAnalysis.id))
                    .filter(SentimentAnalysis.search_keyword == keyword)
                    .filter(func.date(SentimentAnalysis.analyzed_at) == today)
                    .scalar()
                ) or 0

                if total_posts > 0:
                    positive_pct = sentiment_counts["positive"] / total_posts * 100
                    negative_pct = sentiment_counts["negative"] / total_posts * 100
                    neutral_pct = sentiment_counts["neutral"] / total_posts * 100
                    avg_confidence = (
                        total_confidence / total_posts if total_confidence > 0 else 0
                    )
                else:
                    positive_pct = negative_pct = neutral_pct = avg_confidence = 0

                return {
                    "total_posts": total_posts,
                    "positive_percentage": round(positive_pct, 1),
                    "negative_percentage": round(negative_pct, 1),
                    "neutral_percentage": round(neutral_pct, 1),
                    "avg_confidence": round(avg_confidence * 100, 1),
                    "posts_today": posts_today,
                }

        except Exception as e:
            logger.error(f"Error getting keyword metrics for {keyword}: {e}")
            return {}

    def get_unified_kpi_metrics(
        self, selected_keywords: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Get unified KPI metrics using SentimentAnalysis as single source of truth.

        Args:
            selected_keywords: List of keywords to filter by, None for all keywords

        Returns:
            Dictionary with all KPI metrics: total_posts, sentiment percentages,
            avg_confidence, posts_today
        """
        try:
            with self.db_connection.get_session() as session:
                # Base query for sentiment analysis data
                base_query = session.query(
                    SentimentAnalysis.sentiment_label,
                    func.count(SentimentAnalysis.id).label("count"),
                    func.avg(SentimentAnalysis.confidence_score).label("avg_conf"),
                )

                # Add keyword filtering if needed
                if selected_keywords is not None and selected_keywords:
                    base_query = base_query.filter(
                        SentimentAnalysis.search_keyword.in_(selected_keywords)
                    )

                sentiment_result = base_query.group_by(
                    SentimentAnalysis.sentiment_label
                ).all()

                total_posts = 0
                sentiment_counts = {"positive": 0, "negative": 0, "neutral": 0}
                total_confidence_weighted = 0

                for row in sentiment_result:
                    sentiment = row.sentiment_label
                    count = row.count
                    avg_conf = row.avg_conf or 0

                    sentiment_counts[sentiment] = count
                    total_posts += count
                    total_confidence_weighted += avg_conf * count

                today = datetime.now().date()
                today_query = session.query(func.count(SentimentAnalysis.id))

                if selected_keywords is not None and selected_keywords:
                    today_query = today_query.filter(
                        SentimentAnalysis.search_keyword.in_(selected_keywords)
                    )
                
                today_query = today_query.filter(
                    func.date(SentimentAnalysis.analyzed_at) == today
                )

                posts_today = today_query.scalar() or 0

                if total_posts > 0:
                    positive_pct = sentiment_counts["positive"] / total_posts * 100
                    negative_pct = sentiment_counts["negative"] / total_posts * 100
                    neutral_pct = sentiment_counts["neutral"] / total_posts * 100
                    avg_confidence = total_confidence_weighted / total_posts * 100
                else:
                    positive_pct = negative_pct = neutral_pct = avg_confidence = 0

                return {
                    "total_posts": total_posts,
                    "positive_percentage": round(positive_pct, 1),
                    "negative_percentage": round(negative_pct, 1),
                    "neutral_percentage": round(neutral_pct, 1),
                    "avg_confidence": round(avg_confidence, 1),
                    "posts_today": posts_today,
                }

        except Exception as e:
            logger.error(f"Error getting unified KPI metrics: {e}")
            return {
                "total_posts": 0,
                "positive_percentage": 0.0,
                "negative_percentage": 0.0,
                "neutral_percentage": 0.0,
                "avg_confidence": 0.0,
                "posts_today": 0,
            }

    def get_keyword_specific_kpis(self, selected_keyword: str) -> Dict[str, Any]:
        """
        Get enhanced KPI metrics for a specific keyword.

        Args:
            selected_keyword: Single keyword to analyze

        Returns:
            Dictionary with keyword-specific KPI metrics
        """
        try:
            with self.db_connection.get_session() as session:
                results = {}

                week_posts = self._get_posts_this_week(session, selected_keyword)
                results.update(week_posts)

                confidence = self._get_keyword_confidence(session, selected_keyword)
                results["confidence_score"] = confidence

                momentum = self._get_sentiment_momentum(session, selected_keyword)
                results.update(momentum)

                rank = self._get_keyword_rank(session, selected_keyword)
                results.update(rank)

                daily_avg = self._get_daily_average(session, selected_keyword)
                results["daily_average"] = daily_avg

                peak = self._get_peak_performance(session, selected_keyword)
                results.update(peak)

                return results

        except Exception as e:
            logger.error(
                f"Error getting keyword-specific KPIs for {selected_keyword}: {e}"
            )
            return {
                "posts_this_week": 0,
                "week_trend": 0.0,
                "confidence_score": 0.0,
                "sentiment_momentum": "stable",
                "momentum_change": 0.0,
                "keyword_rank": 0,
                "total_keywords": 0,
                "daily_average": 0.0,
                "peak_sentiment": 0.0,
                "peak_date": None,
            }

    def _get_posts_this_week(self, session, keyword: str) -> Dict[str, Any]:
        """Get posts this week with trend vs last week."""
        from datetime import timedelta

        today = datetime.now(timezone.utc).date()
        week_start = today - timedelta(days=today.weekday())
        last_week_start = week_start - timedelta(days=7)

        this_week = (
            session.query(func.count(SentimentAnalysis.id))
            .filter(
                SentimentAnalysis.search_keyword == keyword,
                func.date(SentimentAnalysis.analyzed_at) >= week_start,
            )
            .scalar()
            or 0
        )

        last_week = (
            session.query(func.count(SentimentAnalysis.id))
            .filter(
                SentimentAnalysis.search_keyword == keyword,
                func.date(SentimentAnalysis.analyzed_at) >= last_week_start,
                func.date(SentimentAnalysis.analyzed_at) < week_start,
            )
            .scalar()
            or 0
        )

        trend = 0.0
        if last_week > 0:
            trend = ((this_week - last_week) / last_week) * 100
        elif this_week > 0:
            trend = 100.0

        return {"posts_this_week": this_week, "week_trend": round(trend, 1)}

    def _get_keyword_confidence(self, session, keyword: str) -> float:
        """Get average confidence score for this keyword."""
        confidence = (
            session.query(func.avg(SentimentAnalysis.confidence_score))
            .filter(SentimentAnalysis.search_keyword == keyword)
            .scalar()
        )

        return round((confidence or 0) * 100, 1)

    def _get_sentiment_momentum(self, session, keyword: str) -> Dict[str, Any]:
        """Calculate if sentiment is improving or declining."""
        from datetime import timedelta

        today = datetime.now(timezone.utc).date()
        three_days_ago = today - timedelta(days=3)
        week_ago = today - timedelta(days=7)

        recent_positive = (
            session.query(func.count(SentimentAnalysis.id))
            .filter(
                SentimentAnalysis.search_keyword == keyword,
                SentimentAnalysis.sentiment_label == "positive",
                func.date(SentimentAnalysis.analyzed_at) >= three_days_ago,
            )
            .scalar()
            or 0
        )

        recent_total = (
            session.query(func.count(SentimentAnalysis.id))
            .filter(
                SentimentAnalysis.search_keyword == keyword,
                func.date(SentimentAnalysis.analyzed_at) >= three_days_ago,
            )
            .scalar()
            or 0
        )

        earlier_positive = (
            session.query(func.count(SentimentAnalysis.id))
            .filter(
                SentimentAnalysis.search_keyword == keyword,
                SentimentAnalysis.sentiment_label == "positive",
                func.date(SentimentAnalysis.analyzed_at) >= week_ago,
                func.date(SentimentAnalysis.analyzed_at) < three_days_ago,
            )
            .scalar()
            or 0
        )

        earlier_total = (
            session.query(func.count(SentimentAnalysis.id))
            .filter(
                SentimentAnalysis.search_keyword == keyword,
                func.date(SentimentAnalysis.analyzed_at) >= week_ago,
                func.date(SentimentAnalysis.analyzed_at) < three_days_ago,
            )
            .scalar()
            or 0
        )

        recent_pct = (recent_positive / recent_total * 100) if recent_total > 0 else 0
        earlier_pct = (
            (earlier_positive / earlier_total * 100) if earlier_total > 0 else 0
        )

        momentum_change = recent_pct - earlier_pct

        if momentum_change > 5:
            momentum = "improving"
        elif momentum_change < -5:
            momentum = "declining"
        else:
            momentum = "stable"

        return {
            "sentiment_momentum": momentum,
            "momentum_change": round(momentum_change, 1),
        }

    def _get_keyword_rank(self, session, keyword: str) -> Dict[str, Any]:
        """Get rank of this keyword by total posts vs other keywords."""
        keyword_counts = (
            session.query(
                SentimentAnalysis.search_keyword, func.count(SentimentAnalysis.id).label("post_count")
            )
            .filter(SentimentAnalysis.search_keyword.isnot(None))
            .group_by(SentimentAnalysis.search_keyword)
            .order_by(func.count(SentimentAnalysis.id).desc())
            .all()
        )

        total_keywords = len(keyword_counts)
        keyword_rank = 0

        for i, (kw, count) in enumerate(keyword_counts, 1):
            if kw == keyword:
                keyword_rank = i
                break

        return {"keyword_rank": keyword_rank, "total_keywords": total_keywords}

    def _get_daily_average(self, session, keyword: str) -> float:
        """Get average posts per day for this keyword."""
        from datetime import timedelta

        thirty_days_ago = datetime.now(timezone.utc).date() - timedelta(days=30)

        total_posts = (
            session.query(func.count(SentimentAnalysis.id))
            .filter(
                SentimentAnalysis.search_keyword == keyword,
                func.date(SentimentAnalysis.analyzed_at) >= thirty_days_ago,
            )
            .scalar()
            or 0
        )

        return round(total_posts / 30, 1)

    def _get_peak_performance(self, session, keyword: str) -> Dict[str, Any]:
        """Get the best sentiment day for this keyword."""
        from datetime import timedelta

        thirty_days_ago = datetime.now(timezone.utc).date() - timedelta(days=30)

        daily_sentiment = (
            session.query(
                func.date(SentimentAnalysis.analyzed_at).label("date"),
                func.avg(
                    case(
                        (SentimentAnalysis.sentiment_label == "positive", 1.0),
                        (SentimentAnalysis.sentiment_label == "neutral", 0.5),
                        else_=0.0,
                    )
                ).label("avg_sentiment"),
                func.count(SentimentAnalysis.id).label("post_count"),
            )
            .filter(
                SentimentAnalysis.search_keyword == keyword,
                func.date(SentimentAnalysis.analyzed_at) >= thirty_days_ago,
            )
            .group_by(func.date(SentimentAnalysis.analyzed_at))
            .having(func.count(SentimentAnalysis.id) >= 5)
            .order_by(
                func.avg(
                    case(
                        (SentimentAnalysis.sentiment_label == "positive", 1.0),
                        (SentimentAnalysis.sentiment_label == "neutral", 0.5),
                        else_=0.0,
                    )
                ).desc()
            )
            .first()
        )

        if daily_sentiment:
            return {
                "peak_sentiment": round(daily_sentiment.avg_sentiment * 100, 1),
                "peak_date": daily_sentiment.date.strftime("%Y-%m-%d"),
            }
        else:
            return {"peak_sentiment": 0.0, "peak_date": None}

    def get_keyword_insights(
        self, selected_keywords: Optional[List[str]], days: int = 7
    ) -> Dict[str, Any]:
        """
        Get comprehensive insights for selected keywords.

        Args:
            selected_keywords: List of keywords to analyze, None for all
            days: Number of days to analyze (7, 15, or 30)

        Returns:
            Dictionary with insights data organized by category
        """
        try:
            with self.db_connection.get_session() as session:
                if not selected_keywords:
                    return self._get_platform_insights(session, days)
                elif len(selected_keywords) == 1:
                    return self._get_single_keyword_insights(
                        session, selected_keywords[0], days
                    )
                else:
                    return self._get_multi_keyword_insights(
                        session, selected_keywords, days
                    )

        except Exception as e:
            logger.error(f"Error getting keyword insights: {e}")
            return {
                "trend_analysis": {},
                "volume_stats": {},
                "performance_metrics": {},
                "activity_patterns": {},
            }

    def _get_single_keyword_insights(
        self, session, keyword: str, days: int
    ) -> Dict[str, Any]:
        """Get insights for a single keyword."""
        from datetime import timedelta

        end_date = datetime.now(timezone.utc).date()
        start_date = end_date - timedelta(days=days)
        previous_start = start_date - timedelta(days=days)

        current_sentiment = (
            session.query(func.avg(SentimentAnalysis.positive_score))
            .filter(
                SentimentAnalysis.search_keyword == keyword,
                func.date(SentimentAnalysis.analyzed_at) >= start_date,
            )
            .scalar()
            or 0.0
        )

        previous_sentiment = (
            session.query(func.avg(SentimentAnalysis.positive_score))
            .filter(
                SentimentAnalysis.search_keyword == keyword,
                func.date(SentimentAnalysis.analyzed_at) >= previous_start,
                func.date(SentimentAnalysis.analyzed_at) < start_date,
            )
            .scalar()
            or 0.0
        )

        sentiment_change = 0.0
        if previous_sentiment > 0:
            sentiment_change = (
                (current_sentiment - previous_sentiment) / previous_sentiment
            ) * 100

        best_day_data = (
            session.query(
                func.date(SentimentAnalysis.analyzed_at),
                func.avg(SentimentAnalysis.positive_score),
            )
            .filter(
                SentimentAnalysis.search_keyword == keyword,
                func.date(SentimentAnalysis.analyzed_at) >= start_date,
            )
            .group_by(func.date(SentimentAnalysis.analyzed_at))
            .order_by(func.avg(SentimentAnalysis.positive_score).desc())
            .first()
        )

        worst_day_data = (
            session.query(
                func.date(SentimentAnalysis.analyzed_at),
                func.avg(SentimentAnalysis.positive_score),
            )
            .filter(
                SentimentAnalysis.search_keyword == keyword,
                func.date(SentimentAnalysis.analyzed_at) >= start_date,
            )
            .group_by(func.date(SentimentAnalysis.analyzed_at))
            .order_by(func.avg(SentimentAnalysis.positive_score).asc())
            .first()
        )

        current_posts = (
            session.query(func.count(SentimentAnalysis.id))
            .filter(
                SentimentAnalysis.search_keyword == keyword,
                func.date(SentimentAnalysis.analyzed_at) >= start_date,
            )
            .scalar()
            or 0
        )

        all_keywords_volume = (
            session.query(
                SentimentAnalysis.search_keyword,
                func.count(SentimentAnalysis.id).label("post_count"),
            )
            .filter(
                func.date(SentimentAnalysis.analyzed_at) >= start_date,
                SentimentAnalysis.search_keyword.isnot(None)
            )
            .group_by(SentimentAnalysis.search_keyword)
            .order_by(func.count(SentimentAnalysis.id).desc())
            .all()
        )

        keyword_rank = 1
        total_keywords = len(all_keywords_volume)
        for i, (kw, count) in enumerate(all_keywords_volume, 1):
            if kw == keyword:
                keyword_rank = i
                break

        avg_confidence = (
            session.query(func.avg(SentimentAnalysis.confidence_score))
            .filter(
                SentimentAnalysis.search_keyword == keyword,
                func.date(SentimentAnalysis.analyzed_at) >= start_date,
            )
            .scalar()
            or 0.0
        )

        hourly_activity = (
            session.query(
                func.extract("hour", SentimentAnalysis.analyzed_at).label("hour"),
                func.count(SentimentAnalysis.id).label("post_count"),
            )
            .filter(
                SentimentAnalysis.search_keyword == keyword,
                func.date(SentimentAnalysis.analyzed_at) >= start_date,
            )
            .group_by(func.extract("hour", SentimentAnalysis.analyzed_at))
            .order_by(func.count(SentimentAnalysis.id).desc())
            .limit(3)
            .all()
        )

        peak_hours = (
            [f"{int(hour):02d}:00" for hour, count in hourly_activity]
            if hourly_activity
            else []
        )

        return {
            "trend_analysis": {
                "sentiment_change": round(sentiment_change, 1),
                "current_sentiment": round(current_sentiment * 100, 1),
                "trend_direction": (
                    "improving"
                    if sentiment_change > 5
                    else "declining" if sentiment_change < -5 else "stable"
                ),
                "best_day": best_day_data[0] if best_day_data else None,
                "best_sentiment": (
                    round(best_day_data[1] * 100, 1) if best_day_data else 0.0
                ),
                "worst_day": worst_day_data[0] if worst_day_data else None,
                "worst_sentiment": (
                    round(worst_day_data[1] * 100, 1) if worst_day_data else 0.0
                ),
            },
            "volume_stats": {
                "total_posts": current_posts,
                "keyword_rank": keyword_rank,
                "total_keywords": total_keywords,
                "daily_average": round(current_posts / days, 1),
            },
            "performance_metrics": {
                "avg_confidence": round(avg_confidence * 100, 1),
                "quality_rating": (
                    "high"
                    if avg_confidence > 0.8
                    else "medium" if avg_confidence > 0.6 else "low"
                ),
            },
            "activity_patterns": {
                "peak_hours": peak_hours[:2],
                "analysis_period": f"{days} days",
            },
        }

    def _get_multi_keyword_insights(
        self, session, keywords: List[str], days: int
    ) -> Dict[str, Any]:
        """Get comparative insights for multiple keywords."""
        from datetime import timedelta

        end_date = datetime.now(timezone.utc).date()
        start_date = end_date - timedelta(days=days)

        keyword_data = {}
        for keyword in keywords:
            posts_count = (
                session.query(func.count(SentimentAnalysis.id))
                .filter(
                    SentimentAnalysis.search_keyword == keyword,
                    func.date(SentimentAnalysis.analyzed_at) >= start_date,
                )
                .scalar()
                or 0
            )

            avg_sentiment = (
                session.query(func.avg(SentimentAnalysis.positive_score))
                .filter(
                    SentimentAnalysis.search_keyword == keyword,
                    func.date(SentimentAnalysis.analyzed_at) >= start_date,
                )
                .scalar()
                or 0.0
            )

            keyword_data[keyword] = {
                "posts": posts_count,
                "sentiment": round(avg_sentiment * 100, 1),
            }

        top_performer = max(keyword_data.items(), key=lambda x: x[1]["posts"])
        best_sentiment = max(keyword_data.items(), key=lambda x: x[1]["sentiment"])

        return {
            "trend_analysis": {
                "comparison_type": "multiple_keywords",
                "selected_count": len(keywords),
            },
            "volume_stats": {
                "top_performer": top_performer[0],
                "top_posts": top_performer[1]["posts"],
                "total_posts": sum(data["posts"] for data in keyword_data.values()),
            },
            "performance_metrics": {
                "best_sentiment_keyword": best_sentiment[0],
                "best_sentiment_score": best_sentiment[1]["sentiment"],
                "avg_sentiment": round(
                    sum(data["sentiment"] for data in keyword_data.values())
                    / len(keywords),
                    1,
                ),
            },
            "activity_patterns": {
                "keywords_analyzed": list(keywords),
                "analysis_period": f"{days} days",
            },
        }

    def _get_platform_insights(self, session, days: int) -> Dict[str, Any]:
        """Get platform-wide insights when all keywords are selected."""
        from datetime import timedelta

        end_date = datetime.now(timezone.utc).date()
        start_date = end_date - timedelta(days=days)

        total_posts = (
            session.query(func.count(SentimentAnalysis.id))
            .filter(func.date(SentimentAnalysis.analyzed_at) >= start_date)
            .scalar()
            or 0
        )

        avg_platform_sentiment = (
            session.query(func.avg(SentimentAnalysis.positive_score))
            .filter(func.date(SentimentAnalysis.analyzed_at) >= start_date)
            .scalar()
            or 0.0
        )

        top_keywords = (
            session.query(
                SentimentAnalysis.search_keyword,
                func.count(SentimentAnalysis.id).label("post_count"),
            )
            .filter(
                func.date(SentimentAnalysis.analyzed_at) >= start_date,
                SentimentAnalysis.search_keyword.isnot(None)
            )
            .group_by(SentimentAnalysis.search_keyword)
            .order_by(func.count(SentimentAnalysis.id).desc())
            .limit(3)
            .all()
        )

        return {
            "trend_analysis": {
                "platform_sentiment": round(avg_platform_sentiment * 100, 1),
                "analysis_type": "platform_wide",
            },
            "volume_stats": {
                "total_posts": total_posts,
                "daily_average": round(total_posts / days, 1),
                "top_keywords": [kw for kw, _ in top_keywords],
            },
            "performance_metrics": {
                "platform_health": (
                    "healthy" if avg_platform_sentiment > 0.5 else "mixed"
                ),
                "total_keywords": len(top_keywords),
            },
            "activity_patterns": {
                "scope": "all_keywords",
                "analysis_period": f"{days} days",
            },
        }


db_operations = None


def get_db_operations() -> DatabaseOperations:
    """Get the global database operations instance."""
    global db_operations
    if db_operations is None:
        db_operations = DatabaseOperations()
    return db_operations
