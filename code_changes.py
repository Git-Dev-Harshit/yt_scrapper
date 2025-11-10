#!/usr/bin/env python3
"""
Fetch YouTube search results & transcripts using pytubefix:
- Handles videos, shorts, playlists, and channels
- Fetches channel subscriber count and transcript where possible
- Rotating residential proxies (if needed)
- Saves data to JSON for validation
"""

import os
import re
import json
import time
import random
import datetime
import logging
import concurrent.futures
from typing import List, Optional, Dict
from dateutil import parser as dateparser
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from pytubefix import YouTube
from pytubefix.contrib.search import Search
from pytubefix.contrib.channel import Channel
from pytubefix.contrib.playlist import Playlist

# ---------- Config ----------
load_dotenv()

TOPICS_FILE = "topics_2.txt"
OUTPUT_FILE = "output_scraped.json"
MAX_PER_TOPIC = 2
WORKERS = 4
RATE_LIMIT_SECONDS = 3

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s"
)
log = logging.getLogger("yt_scraper")

# ---------- Helpers ----------
def read_topics(path: str) -> List[str]:
    if not os.path.exists(path):
        log.error(f"Missing {path}")
        return []
    with open(path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


def within_last_n_days(upload_date: str, n: int = 30) -> bool:
    try:
        dt = dateparser.parse(upload_date)
        cutoff = datetime.datetime.now(datetime.timezone.utc) - relativedelta(days=n)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt >= cutoff
    except Exception:
        return False


# ---------- Channel Subscriber Count ----------
_channel_cache: Dict[str, Optional[int]] = {}

def parse_subscriber_count(text: str) -> Optional[int]:
    if not text:
        return None
    text = text.lower()
    if "no subscribers" in text or "hidden" in text:
        return None

    match = re.search(r"([\d.,]+)\s*([MK]?)\s*subscribers", text, re.IGNORECASE)
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


def get_channel_subscribers_pytubefix(channel_url: str) -> Optional[int]:
    if not channel_url:
        return None
    if channel_url in _channel_cache:
        return _channel_cache[channel_url]

    try:
        ch = Channel(channel_url)
        subs = parse_subscriber_count(ch.about_html)
        _channel_cache[channel_url] = subs
        return subs
    except Exception as e:
        log.warning(f"[Channel] Subscriber fetch failed for {channel_url}: {e}")
        _channel_cache[channel_url] = None
        return None


# ---------- Transcript ----------
def fetch_transcript(video_id: str) -> Optional[str]:
    url = f"https://www.youtube.com/watch?v={video_id}"
    try:
        yt = YouTube(url)
        captions = yt.captions
        if not captions:
            return None

        caption = None
        preferred_codes = ['en', 'a.en']
        for code in preferred_codes:
            if code in captions:
                caption = captions[code]
                break

        if not caption:
            first_key = list(captions.lang_code_index.keys())[0]
            caption = captions[first_key]

        srt_text = caption.generate_srt_captions()
        lines = [
            line.strip() for line in srt_text.splitlines()
            if "-->" not in line and line.strip() and not line.isdigit()
        ]
        return " ".join(lines)
    except Exception as e:
        log.warning(f"[Transcript] Failed for {video_id}: {e}")
        return None


# ---------- Entity Handlers ----------
def handle_video(item, topic: str) -> Optional[dict]:
    try:
        yt = YouTube(item.watch_url)
    except Exception as e:
        log.warning(f"[{topic}] Skipping video load error: {e}")
        return None

    upload_date = yt.publish_date.isoformat() if yt.publish_date else None
    if not upload_date or not within_last_n_days(upload_date, 30):
        return None

    time.sleep(RATE_LIMIT_SECONDS)
    transcript = fetch_transcript(yt.video_id)
    subs = get_channel_subscribers_pytubefix(yt.channel_url)

    return {
        "type": "video",
        "video_id": yt.video_id,
        "title": yt.title,
        "url": yt.watch_url,
        "channel_name": yt.author,
        "channel_url": yt.channel_url,
        "subscriber_count": subs,
        "upload_date": upload_date,
        "duration_seconds": int(yt.length),
        "view_count": yt.views,
        "like_count": getattr(yt, "likes", None),
        "description": yt.description,
        "language_codes": list(yt.captions.lang_code_index.keys()) if yt.captions else [],
        "thumbnail_url": yt.thumbnail_url,
        "transcript": transcript,
        "label": topic,
    }


def handle_channel(item, topic: str) -> Optional[dict]:
    try:
        ch = Channel(item.channel_url)
        subs = parse_subscriber_count(ch.about_html)
        return {
            "type": "channel",
            "title": ch.channel_name,
            "url": ch.channel_url,
            "subscriber_count": subs,
            "video_count": ch.video_count,
            "description": ch.about_html[:500],
            "topic": topic,
        }
    except Exception as e:
        log.warning(f"[{topic}] Channel scrape failed: {e}")
        return None


def handle_playlist(item, topic: str) -> Optional[dict]:
    try:
        pl = Playlist(item.playlist_url)
        return {
            "type": "playlist",
            "title": pl.title,
            "url": pl.playlist_url,
            "video_count": len(pl.video_urls),
            "videos_sample": pl.video_urls[:5],
            "topic": topic,
        }
    except Exception as e:
        log.warning(f"[{topic}] Playlist scrape failed: {e}")
        return None


def handle_short(item, topic: str) -> Optional[dict]:
    # pytubefix treats Shorts as normal videos
    try:
        yt = YouTube(item.watch_url)
        return {
            "type": "short",
            "video_id": yt.video_id,
            "title": yt.title,
            "url": yt.watch_url,
            "channel_name": yt.author,
            "view_count": yt.views,
            "duration_seconds": yt.length,
            "topic": topic,
        }
    except Exception as e:
        log.warning(f"[{topic}] Short fetch failed: {e}")
        return None


# ---------- Process Topic ----------
def process_topic(topic: str) -> List[dict]:
    topic = topic.strip()
    log.info(f"=== Topic: {topic} ===")
    results = []

    try:
        search_results = Search(topic).results
    except Exception as e:
        log.error(f"Search failed for topic '{topic}': {e}")
        return results

    count = 0
    for item in search_results:
        if count >= MAX_PER_TOPIC:
            break

        obj = None
        if hasattr(item, "watch_url"):  # likely video or short
            if "shorts" in item.watch_url:
                obj = handle_short(item, topic)
            else:
                obj = handle_video(item, topic)
        elif hasattr(item, "channel_url"):  # channel
            obj = handle_channel(item, topic)
        elif hasattr(item, "playlist_url"):  # playlist
            obj = handle_playlist(item, topic)

        if obj:
            results.append(obj)
            count += 1
            log.info(f"[{topic}] ✅ Saved {obj['type']}: {obj.get('title', obj.get('url'))}")

    log.info(f"[{topic}] Done. {count} items scraped.")
    return results


# ---------- Main ----------
def main():
    topics = read_topics(TOPICS_FILE)
    if not topics:
        log.error("No topics found in topics.txt")
        return

    all_data = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = [ex.submit(process_topic, topic) for topic in topics]
        for fut in concurrent.futures.as_completed(futures):
            try:
                all_data.extend(fut.result())
            except Exception as e:
                log.error(f"Worker crashed: {e}")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)

    log.info(f"✅ All done. Saved {len(all_data)} records to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
