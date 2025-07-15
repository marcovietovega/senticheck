import re
import logging
from typing import List, Dict, Optional
import html
from urllib.parse import urlparse

# Configure logging
logger = logging.getLogger(__name__)


class TextCleaner:
    """Text cleaning utility for social media posts."""

    def __init__(self):
        """Initialize the text cleaner with regex patterns."""
        # URL patterns
        self.url_pattern = re.compile(
            r"http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+"
        )

        # Mention patterns (@username)
        self.mention_pattern = re.compile(r"@[\w\.-]+")

        # Hashtag patterns (#hashtag)
        self.hashtag_pattern = re.compile(r"#[\w]+")

        # Emoji pattern (basic Unicode ranges)
        self.emoji_pattern = re.compile(
            r"[\U0001F600-\U0001F64F]|"  # emoticons
            r"[\U0001F300-\U0001F5FF]|"  # symbols & pictographs
            r"[\U0001F680-\U0001F6FF]|"  # transport & map symbols
            r"[\U0001F1E0-\U0001F1FF]|"  # flags (iOS)
            r"[\U00002702-\U000027B0]|"  # dingbats
            r"[\U000024C2-\U0001F251]"  # enclosed characters
        )

        # Extra whitespace pattern
        self.whitespace_pattern = re.compile(r"\s+")

        # Special characters to remove
        self.special_chars_pattern = re.compile(r'[^\w\s\.,!?;:\'"()-]')

    def clean_post(
        self,
        post_data: Dict,
        preserve_hashtags: bool = False,
        preserve_mentions: bool = False,
    ) -> Dict:
        """
        Clean a single post's text content.

        Args:
            post_data (Dict): Post data dictionary with 'text' field
            preserve_hashtags (bool): Whether to keep hashtags
            preserve_mentions (bool): Whether to keep mentions

        Returns:
            Dict: Updated post data with cleaned text and metadata
        """
        if not post_data or "text" not in post_data:
            logger.warning("Invalid post data provided for cleaning")
            return post_data

        original_text = post_data["text"]

        try:
            cleaned_text = self.clean_text(
                original_text,
                preserve_hashtags=preserve_hashtags,
                preserve_mentions=preserve_mentions,
            )

            # Create cleaned post data
            cleaned_post = post_data.copy()
            cleaned_post["original_text"] = original_text
            cleaned_post["text"] = cleaned_text
            cleaned_post["cleaning_metadata"] = {
                "original_length": len(original_text),
                "cleaned_length": len(cleaned_text),
                "urls_removed": len(self.url_pattern.findall(original_text)),
                "mentions_removed": (
                    0
                    if preserve_mentions
                    else len(self.mention_pattern.findall(original_text))
                ),
                "hashtags_removed": (
                    0
                    if preserve_hashtags
                    else len(self.hashtag_pattern.findall(original_text))
                ),
                "has_emojis": bool(self.emoji_pattern.search(original_text)),
            }

            return cleaned_post

        except Exception as e:
            logger.error(f"Error cleaning post text: {e}")
            return post_data

    def clean_text(
        self,
        text: str,
        preserve_hashtags: bool = False,
        preserve_mentions: bool = False,
    ) -> str:
        """
        Clean individual text string.

        Args:
            text (str): Raw text to clean
            preserve_hashtags (bool): Whether to keep hashtags
            preserve_mentions (bool): Whether to keep mentions

        Returns:
            str: Cleaned text
        """
        if not text or not isinstance(text, str):
            return ""

        # Decode HTML entities
        text = html.unescape(text)

        # Remove URLs
        text = self.url_pattern.sub("", text)

        # Handle mentions
        if not preserve_mentions:
            text = self.mention_pattern.sub("", text)

        # Handle hashtags
        if not preserve_hashtags:
            # Remove # but keep the word
            text = self.hashtag_pattern.sub(lambda m: m.group(0)[1:], text)

        # Remove emojis
        text = self.emoji_pattern.sub("", text)

        # Remove extra special characters (but keep basic punctuation)
        text = self.special_chars_pattern.sub("", text)

        # Normalize whitespace
        text = self.whitespace_pattern.sub(" ", text)

        # Strip leading/trailing whitespace
        text = text.strip()

        return text

    def clean_posts_batch(
        self,
        posts: List[Dict],
        preserve_hashtags: bool = False,
        preserve_mentions: bool = False,
    ) -> List[Dict]:
        """
        Clean a batch of posts.

        Args:
            posts (List[Dict]): List of post dictionaries
            preserve_hashtags (bool): Whether to keep hashtags
            preserve_mentions (bool): Whether to keep mentions

        Returns:
            List[Dict]: List of cleaned post dictionaries
        """
        if not posts:
            return []

        cleaned_posts = []
        for post in posts:
            cleaned_post = self.clean_post(
                post,
                preserve_hashtags=preserve_hashtags,
                preserve_mentions=preserve_mentions,
            )

            # Only include posts with meaningful content after cleaning
            if cleaned_post.get("text", "").strip():
                cleaned_posts.append(cleaned_post)
            else:
                logger.info("Skipping post with no meaningful content after cleaning")

        logger.info(f"Cleaned {len(cleaned_posts)} out of {len(posts)} posts")
        return cleaned_posts

    def get_cleaning_stats(self, posts: List[Dict]) -> Dict:
        """
        Get statistics about the cleaning process.

        Args:
            posts (List[Dict]): List of cleaned posts with metadata

        Returns:
            Dict: Cleaning statistics
        """
        if not posts:
            return {}

        total_posts = len(posts)
        total_urls = sum(
            post.get("cleaning_metadata", {}).get("urls_removed", 0) for post in posts
        )
        total_mentions = sum(
            post.get("cleaning_metadata", {}).get("mentions_removed", 0)
            for post in posts
        )
        total_hashtags = sum(
            post.get("cleaning_metadata", {}).get("hashtags_removed", 0)
            for post in posts
        )
        posts_with_emojis = sum(
            1
            for post in posts
            if post.get("cleaning_metadata", {}).get("has_emojis", False)
        )

        avg_original_length = (
            sum(
                post.get("cleaning_metadata", {}).get("original_length", 0)
                for post in posts
            )
            / total_posts
        )
        avg_cleaned_length = (
            sum(
                post.get("cleaning_metadata", {}).get("cleaned_length", 0)
                for post in posts
            )
            / total_posts
        )

        return {
            "total_posts_cleaned": total_posts,
            "total_urls_removed": total_urls,
            "total_mentions_removed": total_mentions,
            "total_hashtags_removed": total_hashtags,
            "posts_with_emojis": posts_with_emojis,
            "emoji_percentage": (posts_with_emojis / total_posts) * 100,
            "avg_original_length": round(avg_original_length, 2),
            "avg_cleaned_length": round(avg_cleaned_length, 2),
            "avg_length_reduction": round(
                ((avg_original_length - avg_cleaned_length) / avg_original_length)
                * 100,
                2,
            ),
        }


def clean_bluesky_posts(
    posts: List[Dict], preserve_hashtags: bool = False, preserve_mentions: bool = False
) -> List[Dict]:
    """
    Convenience function to clean Bluesky posts.

    Args:
        posts (List[Dict]): List of post dictionaries from Bluesky
        preserve_hashtags (bool): Whether to keep hashtags
        preserve_mentions (bool): Whether to keep mentions

    Returns:
        List[Dict]: List of cleaned post dictionaries
    """
    cleaner = TextCleaner()
    return cleaner.clean_posts_batch(posts, preserve_hashtags, preserve_mentions)


def print_cleaning_stats(posts: List[Dict]):
    """
    Print cleaning statistics in a readable format.

    Args:
        posts (List[Dict]): List of cleaned posts with metadata
    """
    cleaner = TextCleaner()
    stats = cleaner.get_cleaning_stats(posts)

    if not stats:
        print("No cleaning statistics available.")
        return

    print(f"\n{'='*50}")
    print("TEXT CLEANING STATISTICS")
    print(f"{'='*50}")
    print(f"Total posts cleaned: {stats['total_posts_cleaned']}")
    print(f"URLs removed: {stats['total_urls_removed']}")
    print(f"Mentions removed: {stats['total_mentions_removed']}")
    print(f"Hashtags removed: {stats['total_hashtags_removed']}")
    print(
        f"Posts with emojis: {stats['posts_with_emojis']} ({stats['emoji_percentage']:.1f}%)"
    )
    print(f"Average original length: {stats['avg_original_length']} characters")
    print(f"Average cleaned length: {stats['avg_cleaned_length']} characters")
    print(f"Average length reduction: {stats['avg_length_reduction']:.1f}%")


if __name__ == "__main__":
    # Example usage
    import sys
    import os

    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    from connectors.bluesky.fetch_posts import fetch_bluesky_posts

    # Fetch some posts
    posts = fetch_bluesky_posts("Nintendo Switch 2", 3)

    if posts:
        print("ORIGINAL POSTS:")
        for i, post in enumerate(posts, 1):
            print(f"\n[{i}] {post.get('text', '')}")

        # Clean the posts
        cleaned_posts = clean_bluesky_posts(posts, preserve_hashtags=True)

        print("\nCLEANED POSTS:")
        for i, post in enumerate(cleaned_posts, 1):
            print(f"\n[{i}] {post.get('text', '')}")

        # Show cleaning statistics
        print_cleaning_stats(cleaned_posts)
    else:
        print("No posts found to clean.")
