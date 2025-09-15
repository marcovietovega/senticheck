"""
Database optimization utilities for SentiCheck.
Includes index creation and query optimization.
"""

import logging
from sqlalchemy import text, Index
from .database import Base, RawPost, CleanedPost, SentimentAnalysis
from .db_connection import DatabaseConnection

logger = logging.getLogger(__name__)


class DatabaseOptimizer:
    """Database optimization utilities."""

    def __init__(self, db_connection: DatabaseConnection):
        self.db_connection = db_connection

    def create_performance_indexes(self) -> bool:
        """Create performance indexes for common queries.

        Returns:
            True if successful, False otherwise
        """
        try:
            with self.db_connection.get_session() as session:
                # Composite index for unanalyzed posts query
                session.execute(text("""
                    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_cleaned_posts_unanalyzed
                    ON cleaned_posts (is_analyzed, search_keyword, cleaned_at DESC)
                    WHERE is_analyzed = false
                """))

                # Index for sentiment analysis by keyword and date
                session.execute(text("""
                    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sentiment_keyword_date
                    ON sentiment_analysis (search_keyword, analyzed_at DESC, sentiment_label)
                """))

                # Index for raw posts processing
                session.execute(text("""
                    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_raw_posts_unprocessed
                    ON raw_posts (is_processed, search_keyword, created_at DESC)
                    WHERE is_processed = false
                """))

                # Index for time-series queries
                session.execute(text("""
                    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sentiment_time_series
                    ON sentiment_analysis (DATE(analyzed_at), sentiment_label)
                """))

                # Partial index for recent posts
                session.execute(text("""
                    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_raw_posts_recent
                    ON raw_posts (created_at DESC)
                    WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
                """))

                session.commit()
                logger.info("Performance indexes created successfully")
                return True

        except Exception as e:
            logger.error(f"Error creating performance indexes: {e}")
            return False

    def analyze_table_statistics(self) -> bool:
        """Update table statistics for query optimization.

        Returns:
            True if successful, False otherwise
        """
        try:
            with self.db_connection.get_session() as session:
                # Update statistics for all tables
                tables = ['raw_posts', 'cleaned_posts', 'sentiment_analysis']

                for table in tables:
                    session.execute(text(f"ANALYZE {table}"))
                    logger.debug(f"Updated statistics for table: {table}")

                session.commit()
                logger.info("Table statistics updated successfully")
                return True

        except Exception as e:
            logger.error(f"Error updating table statistics: {e}")
            return False

    def get_index_usage_stats(self) -> dict:
        """Get index usage statistics.

        Returns:
            Dictionary with index usage information
        """
        try:
            with self.db_connection.get_session() as session:
                result = session.execute(text("""
                    SELECT
                        schemaname,
                        tablename,
                        indexname,
                        idx_scan as scans,
                        idx_tup_read as tuples_read,
                        idx_tup_fetch as tuples_fetched
                    FROM pg_stat_user_indexes
                    WHERE schemaname = 'public'
                    ORDER BY idx_scan DESC
                """))

                indexes = []
                for row in result:
                    indexes.append({
                        'schema': row.schemaname,
                        'table': row.tablename,
                        'index': row.indexname,
                        'scans': row.scans,
                        'tuples_read': row.tuples_read,
                        'tuples_fetched': row.tuples_fetched,
                    })

                return {'indexes': indexes}

        except Exception as e:
            logger.error(f"Error getting index usage stats: {e}")
            return {'indexes': [], 'error': str(e)}

    def get_slow_queries(self, limit: int = 10) -> dict:
        """Get slow query statistics.

        Args:
            limit: Number of queries to return

        Returns:
            Dictionary with slow query information
        """
        try:
            with self.db_connection.get_session() as session:
                result = session.execute(text(f"""
                    SELECT
                        query,
                        calls,
                        total_time,
                        mean_time,
                        rows
                    FROM pg_stat_statements
                    WHERE query NOT LIKE '%pg_stat_statements%'
                    ORDER BY mean_time DESC
                    LIMIT {limit}
                """))

                queries = []
                for row in result:
                    queries.append({
                        'query': row.query[:200] + '...' if len(row.query) > 200 else row.query,
                        'calls': row.calls,
                        'total_time': round(row.total_time, 2),
                        'mean_time': round(row.mean_time, 2),
                        'rows': row.rows,
                    })

                return {'slow_queries': queries}

        except Exception as e:
            logger.warning(f"pg_stat_statements not available: {e}")
            return {'slow_queries': [], 'note': 'pg_stat_statements extension not enabled'}

    def optimize_vacuum_settings(self) -> bool:
        """Optimize autovacuum settings for better performance.

        Returns:
            True if successful, False otherwise
        """
        try:
            with self.db_connection.get_session() as session:
                # Optimize autovacuum for frequently updated tables
                session.execute(text("""
                    ALTER TABLE cleaned_posts SET (
                        autovacuum_vacuum_scale_factor = 0.1,
                        autovacuum_analyze_scale_factor = 0.05
                    )
                """))

                session.execute(text("""
                    ALTER TABLE sentiment_analysis SET (
                        autovacuum_vacuum_scale_factor = 0.2,
                        autovacuum_analyze_scale_factor = 0.1
                    )
                """))

                session.commit()
                logger.info("Autovacuum settings optimized")
                return True

        except Exception as e:
            logger.error(f"Error optimizing vacuum settings: {e}")
            return False


def optimize_database_performance(db_connection: DatabaseConnection) -> dict:
    """Run complete database optimization.

    Args:
        db_connection: Database connection instance

    Returns:
        Dictionary with optimization results
    """
    optimizer = DatabaseOptimizer(db_connection)
    results = {}

    logger.info("Starting database optimization...")

    # Create performance indexes
    results['indexes_created'] = optimizer.create_performance_indexes()

    # Update table statistics
    results['statistics_updated'] = optimizer.analyze_table_statistics()

    # Optimize vacuum settings
    results['vacuum_optimized'] = optimizer.optimize_vacuum_settings()

    # Get performance stats
    results['index_stats'] = optimizer.get_index_usage_stats()
    results['slow_queries'] = optimizer.get_slow_queries()

    logger.info("Database optimization completed")
    return results