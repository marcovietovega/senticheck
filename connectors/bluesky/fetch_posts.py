import os
from dotenv import load_dotenv
from atproto import Client

load_dotenv()


def fetch_bluesky_posts(keyword="AI", limit=50):
    print("Fetching posts from Bluesky...")
    print(f"Using environment variables for authentication.")

    handle = os.getenv("BLUESKY_HANDLE")
    app_password = os.getenv("BLUESKY_APP_PASSWORD")

    print(f"Handle: {handle}")
    print(f"App Password: {app_password}")

    print(f"Keyword: {keyword}, Limit: {limit}")

    client = Client()
    profile = client.login(handle, app_password)
    print("Welcome,", profile.display_name)

    params = {
        "limit": limit,
        "lang": "en",
        "q": keyword,
    }

    results = client.app.bsky.feed.search_posts(params)
    posts = results.posts

    for post in posts:
        text = post["record"]["text"]
        author = post["author"]["display_name"]
        date = post["record"]["created_at"]
        print("-" * 40)
        print(f"Date: {date}")
        print(f"Author: {author}")
        print(text)


fetch_bluesky_posts("Nintendo Switch 2", 5)
