#!/usr/bin/env python3
"""
Migration script to populate search_keyword in sentiment_analysis table.

This script backfills the new search_keyword column in the sentiment_analysis table
by joining with cleaned_posts and raw_posts to get the keyword that was used
to find each post.
"""

import sys
import os
from pathlib import Path

# Add the parent directory to sys.path to import from models
parent_dir = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(parent_dir))

from models.db_connection import DatabaseConnection
from models.database import SentimentAnalysis, CleanedPost, RawPost
from sqlalchemy import text
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def migrate_search_keywords():
    """Migrate search_keyword data to sentiment_analysis table."""
    
    db_connection = DatabaseConnection()
    
    try:
        with db_connection.get_session() as session:
            logger.info("Starting migration of search_keyword to sentiment_analysis table...")
            
            # First, check if column exists
            result = session.execute(text("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name = 'sentiment_analysis' AND column_name = 'search_keyword'
            """))
            
            if not result.fetchone():
                logger.info("Adding search_keyword column to sentiment_analysis table...")
                session.execute(text("""
                    ALTER TABLE sentiment_analysis 
                    ADD COLUMN search_keyword VARCHAR(255)
                """))
                session.commit()
                logger.info("Column added successfully")
            else:
                logger.info("search_keyword column already exists")
            
            # Update existing records with search_keyword from raw_posts
            logger.info("Updating existing sentiment_analysis records with search_keyword...")
            
            update_query = text("""
                UPDATE sentiment_analysis 
                SET search_keyword = rp.search_keyword
                FROM cleaned_posts cp
                JOIN raw_posts rp ON cp.raw_post_id = rp.id
                WHERE sentiment_analysis.cleaned_post_id = cp.id
                AND sentiment_analysis.search_keyword IS NULL
            """)
            
            result = session.execute(update_query)
            updated_count = result.rowcount
            session.commit()
            
            logger.info(f"Updated {updated_count} sentiment_analysis records with search_keyword")
            
            # Add index on search_keyword for better performance
            logger.info("Creating index on search_keyword column...")
            try:
                session.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_sentiment_analysis_search_keyword 
                    ON sentiment_analysis(search_keyword)
                """))
                session.commit()
                logger.info("Index created successfully")
            except Exception as e:
                logger.warning(f"Index may already exist: {e}")
            
            # Verify migration
            verification_query = text("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(search_keyword) as records_with_keyword,
                    COUNT(CASE WHEN search_keyword IS NULL THEN 1 END) as records_without_keyword
                FROM sentiment_analysis
            """)
            
            result = session.execute(verification_query).fetchone()
            logger.info(f"Migration verification:")
            logger.info(f"  Total sentiment records: {result.total_records}")
            logger.info(f"  Records with keyword: {result.records_with_keyword}")
            logger.info(f"  Records without keyword: {result.records_without_keyword}")
            
            if result.records_without_keyword > 0:
                logger.warning(f"Warning: {result.records_without_keyword} records still have NULL search_keyword")
                logger.warning("This may indicate orphaned sentiment records or missing raw_post data")
            else:
                logger.info("✅ Migration completed successfully - all records have search_keyword")
                
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        raise
    finally:
        db_connection.close()


if __name__ == "__main__":
    migrate_search_keywords()