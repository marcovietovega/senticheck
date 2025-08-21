import re
import logging
from typing import List, Dict
import html

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

    def analyze_post_content(self, text: str) -> Dict[str, any]:
        """
        Analyze post content to determine if it's hashtag-heavy or spam.

        Args:
            text (str): Text to analyze

        Returns:
            Dict: Analysis results
        """
        if not text:
            return {
                "hashtag_count": 0,
                "total_words": 0,
                "content_words": 0,
                "hashtag_ratio": 0.0,
                "is_hashtag_only": True,
                "is_spam": False,
                "content_length": 0,
                "recommendation": "filter",
            }

        # Count hashtags
        hashtags = re.findall(r"#\w+", text)
        hashtag_count = len(hashtags)

        # Remove hashtags to count actual content
        text_without_hashtags = re.sub(r"#\w+", "", text)

        # Remove mentions to focus on content
        text_without_mentions = re.sub(r"@[\w\.-]+", "", text_without_hashtags)

        # Remove URLs
        text_without_urls = self.url_pattern.sub("", text_without_mentions)

        # Remove emojis for word counting
        text_without_emojis = self.emoji_pattern.sub("", text_without_urls)

        # Clean up whitespace and special chars for analysis
        clean_content = re.sub(r"[^\w\s]", " ", text_without_emojis)
        clean_content = re.sub(r"\s+", " ", clean_content).strip()

        # Count words
        content_words = len(clean_content.split()) if clean_content else 0
        total_words = len(text.split())

        # Calculate ratios
        hashtag_ratio = hashtag_count / total_words if total_words > 0 else 1.0
        content_length = len(clean_content)

        # Determine if post is hashtag-only or spam
        is_hashtag_only = (
            content_words == 0
            or (content_length < 10 and hashtag_count > 0)
            or hashtag_ratio > 0.8
        )

        is_spam = (
            hashtag_count > 15
            or (hashtag_count > 8 and content_words < 3)
            or hashtag_ratio > 0.7
        )

        # Make recommendation
        if is_hashtag_only:
            recommendation = "filter"
        elif is_spam:
            recommendation = "review"
        elif hashtag_count > 10:
            recommendation = "review"
        else:
            recommendation = "keep"

        return {
            "hashtag_count": hashtag_count,
            "total_words": total_words,
            "content_words": content_words,
            "hashtag_ratio": round(hashtag_ratio, 3),
            "is_hashtag_only": is_hashtag_only,
            "is_spam": is_spam,
            "content_length": content_length,
            "recommendation": recommendation,
            "hashtags": hashtags,
        }

    def clean_post(
        self,
        post_data: Dict,
        preserve_hashtags: bool = False,
        preserve_mentions: bool = False,
        filter_hashtag_only: bool = True,
        min_content_words: int = 3,
    ) -> Dict:
        """
        Clean a single post's text content.

        Args:
            post_data (Dict): Post data dictionary with 'text' field
            preserve_hashtags (bool): Whether to keep hashtags
            preserve_mentions (bool): Whether to keep mentions
            filter_hashtag_only (bool): Whether to filter posts that are mostly hashtags
            min_content_words (int): Minimum number of content words required

        Returns:
            Dict: Updated post data with cleaned text and metadata, or None if filtered
        """
        if not post_data or "text" not in post_data:
            logger.warning("Invalid post data provided for cleaning")
            return post_data

        original_text = post_data["text"]

        try:
            # Analyze content before cleaning
            content_analysis = self.analyze_post_content(original_text)

            # Filter hashtag-only posts if enabled
            if filter_hashtag_only and content_analysis["is_hashtag_only"]:
                post_id = post_data.get("id", "unknown")
                logger.info(
                    f"Filtered hashtag-only post (ID: {post_id}): {content_analysis['hashtag_count']} hashtags, {content_analysis['content_words']} content words"
                )
                return None

            # Filter posts with insufficient content
            if content_analysis["content_words"] < min_content_words:
                post_id = post_data.get("id", "unknown")
                logger.info(
                    f"Filtered post with insufficient content (ID: {post_id}): {content_analysis['content_words']} words (min: {min_content_words})"
                )
                return None

            cleaned_text = self.clean_text(
                original_text,
                preserve_hashtags=preserve_hashtags,
                preserve_mentions=preserve_mentions,
            )

            # Create cleaned post data
            cleaned_post = post_data.copy()
            cleaned_post["original_text"] = original_text
            cleaned_post["text"] = cleaned_text
            cleaned_post["content_analysis"] = content_analysis

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

        # Remove extra special characters
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
        filter_hashtag_only: bool = True,
        min_content_words: int = 3,
    ) -> List[Dict]:
        """
        Clean a batch of posts.

        Args:
            posts (List[Dict]): List of post dictionaries
            preserve_hashtags (bool): Whether to keep hashtags
            preserve_mentions (bool): Whether to keep mentions
            filter_hashtag_only (bool): Whether to filter posts that are mostly hashtags
            min_content_words (int): Minimum number of content words required

        Returns:
            List[Dict]: List of cleaned post dictionaries
        """
        if not posts:
            return []

        cleaned_posts = []
        filtered_count = 0

        for post in posts:
            cleaned_post = self.clean_post(
                post,
                preserve_hashtags=preserve_hashtags,
                preserve_mentions=preserve_mentions,
                filter_hashtag_only=filter_hashtag_only,
                min_content_words=min_content_words,
            )

            # Skip posts that were filtered out (None return)
            if cleaned_post is None:
                filtered_count += 1
                continue

            # Only include posts with content after cleaning
            if cleaned_post.get("text", "").strip():
                cleaned_posts.append(cleaned_post)
            else:
                logger.info("Skipping post with no content after cleaning")

        logger.info(f"Cleaned {len(cleaned_posts)} out of {len(posts)} posts")
        if filtered_count > 0:
            logger.info(
                f"Filtered out {filtered_count} hashtag-only or low-content posts"
            )

        return cleaned_posts


def clean_bluesky_posts(
    posts: List[Dict],
    preserve_hashtags: bool = False,
    preserve_mentions: bool = False,
    filter_hashtag_only: bool = True,
    min_content_words: int = 3,
) -> List[Dict]:
    """
    Function to clean Bluesky posts.

    Args:
        posts (List[Dict]): List of post dictionaries from Bluesky
        preserve_hashtags (bool): Whether to keep hashtags
        preserve_mentions (bool): Whether to keep mentions
        filter_hashtag_only (bool): Whether to filter posts that are mostly hashtags
        min_content_words (int): Minimum number of content words required

    Returns:
        List[Dict]: List of cleaned post dictionaries
    """
    cleaner = TextCleaner()
    return cleaner.clean_posts_batch(
        posts,
        preserve_hashtags,
        preserve_mentions,
        filter_hashtag_only,
        min_content_words,
    )


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

    else:
        print("No posts found to clean.")
