#!/usr/bin/env python3
"""
Test pytubefix scraping for YouTube 'About' pages:
- Supports video ID, video URL, channel URL, or playlist URL
- For videos: fetches channel and then scrapes the About page
- For channels: directly scrapes About page
- For playlists: fetches title and metadata only (no About)
"""

import re
import sys
from urllib.parse import urlparse, parse_qs

from pytubefix import YouTube
from pytubefix.contrib.channel import Channel
from pytubefix.contrib.playlist import Playlist


# -------------------------------
# Helper: Extract subscriber count from about_html
# -------------------------------
def extract_subscriber_count_from_html(html: str):
    """Extracts the subscriber count from a channel's About HTML."""
    if not html:
        return None

    match = re.search(r'([\d.,]+)\s*([MK]?)\s*subscribers', html, re.IGNORECASE)
    if not match:
        return None

    num, suffix = match.groups()
    try:
        val = float(num.replace(",", ""))
        if suffix.upper() == "M":
            val *= 1_000_000
        elif suffix.upper() == "K":
            val *= 1_000
        return int(val)
    except Exception:
        return None


# -------------------------------
# Helper: Normalize and clean YouTube URLs
# -------------------------------
def normalize_youtube_url(url: str) -> str:
    """Removes unnecessary query params like &list=, &ab_channel= etc."""
    parsed = urlparse(url)

    # Handle video URLs (keep only 'v' param)
    if "watch" in parsed.path:
        query = parse_qs(parsed.query)
        if "v" in query:
            video_id = query["v"][0]
            return f"https://www.youtube.com/watch?v={video_id}"

    # Handle playlist URLs (keep only 'list')
    if "playlist" in parsed.path:
        query = parse_qs(parsed.query)
        if "list" in query:
            list_id = query["list"][0]
            return f"https://www.youtube.com/playlist?list={list_id}"

    # Handle channel URLs directly
    if "channel/" in parsed.path or "@" in parsed.path:
        return url.split("?")[0]

    return url


# -------------------------------
# Print Helpers
# -------------------------------
def print_video_and_channel_info(yt):
    """Prints video info and its channel About page details."""
    print(f"\n=== VIDEO INFO ===")
    print(f"Title: {yt.title}")
    print(f"Channel: {yt.author}")
    print(f"Views: {yt.views}")
    print(f"Duration: {yt.length}s")
    print(f"Upload Date: {yt.publish_date}")
    print(f"Channel URL: {yt.channel_url}")

    print("\n=== FETCHING CHANNEL ABOUT ===")
    ch = Channel(yt.channel_url)
    print_channel_info(ch)


def print_channel_info(ch):
    """Prints channel name, URL, subscriber count, and HTML snippet."""
    print(f"\n=== CHANNEL INFO ===")
    print(f"Channel Name: {ch.channel_name}")
    print(f"Channel URL:  {ch.channel_url}")

    subs = extract_subscriber_count_from_html(ch.about_html)
    print(f"Subscriber Count: {subs}")

    print("\n--- RAW about_html (first 500 chars) ---")
    print(ch.about_html[:500])


# -------------------------------
# Main Logic
# -------------------------------
def inspect_entity(identifier: str):
    identifier = identifier.strip()
    identifier = normalize_youtube_url(identifier)

    # --- Case 1: Video ID ---
    if len(identifier) == 11 and "http" not in identifier:
        video_url = f"https://www.youtube.com/watch?v={identifier}"
        print(f"🎥 Detected Video ID → URL: {video_url}")
        yt = YouTube(video_url)
        print_video_and_channel_info(yt)
        return

    # --- Case 2: Video URL ---
    if "watch?v=" in identifier:
        print(f"🎥 Detected Video URL: {identifier}")
        yt = YouTube(identifier)
        print_video_and_channel_info(yt)
        return

    # --- Case 3: Channel URL ---
    if "channel/" in identifier or "@" in identifier:
        print(f"📺 Detected Channel URL: {identifier}")
        ch = Channel(identifier)
        print_channel_info(ch)
        return

    # --- Case 4: Playlist URL ---
    if "playlist?" in identifier:
        print(f"🎞️ Detected Playlist URL: {identifier}")
        pl = Playlist(identifier)
        print(f"\n=== PLAYLIST INFO ===")
        print(f"Title: {pl.title}")
        print(f"Playlist URL: {pl.playlist_url}")
        print(f"Number of Videos: {len(pl.video_urls)}")
        print("Sample Videos:")
        for v in pl.video_urls[:5]:
            print(f"- {v}")
        return

    # --- Fallback ---
    print("❌ Unknown identifier type. Please pass a video ID, video URL, channel URL, or playlist URL.")


# -------------------------------
# Entrypoint
# -------------------------------
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python about_scrapper.py <video_id|video_url|channel_url|playlist_url>")
        sys.exit(1)

    identifier = sys.argv[1]
    inspect_entity(identifier)
