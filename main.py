#!/usr/bin/env python3
"""
Fetch YouTube videos & transcripts using:
- Official YouTube Data API v3 for metadata
- youtube-transcript-api (latest API) for transcripts
- Rotating residential proxies from proxies.txt

Schema:
CREATE TABLE scraped_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    uri TEXT UNIQUE NOT NULL,
    datetime TEXT NOT NULL,
    source TEXT NOT NULL,
    label TEXT NOT NULL,
    content TEXT NOT NULL,
    content_size_bytes INTEGER NOT NULL,
    created_at TEXT NOT NULL
);
"""

import os
import json
import time
import random
import datetime
import logging
import sqlite3
import threading
import concurrent.futures
from typing import List, Optional
from dateutil import parser as dateparser
from dateutil.relativedelta import relativedelta

from googleapiclient.discovery import build
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api.proxies import GenericProxyConfig
from dotenv import load_dotenv
import isodate

# ---------- Config ----------
load_dotenv()

API_KEY = os.getenv("YOUTUBE_API_KEY")
TOPICS_FILE = "topics.txt"
PROXIES_FILE = "proxies.txt"
DB_FILE = "youtube_scraped_data.db"
MAX_PER_TOPIC = 30
WORKERS = 2
RATE_LIMIT_SECONDS = 2  # limit requests
API_SERVICE_NAME = "youtube"
API_VERSION = "v3"

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(threadName)s: %(message)s"
)
log = logging.getLogger("yt_scraper")

# ---------- Database ----------
CREATE_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
CREATE TABLE IF NOT EXISTS scraped_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    uri TEXT UNIQUE NOT NULL,
    datetime TEXT NOT NULL,
    source TEXT NOT NULL,
    label TEXT NOT NULL,
    content TEXT NOT NULL,
    content_size_bytes INTEGER NOT NULL,
    created_at TEXT NOT NULL
);
"""

_db_lock = threading.Lock()


def open_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
    with conn:
        for stmt in CREATE_SQL.strip().split(";"):
            s = stmt.strip()
            if s:
                conn.execute(s)
    return conn


def upsert_scraped(conn: sqlite3.Connection, rec: dict):
    """Insert or update a record in the scraped_data table."""
    with _db_lock:
        content_json = json.dumps(rec, ensure_ascii=False)
        uri = rec.get("url")
        label = rec.get("label", "")
        now = datetime.datetime.utcnow().isoformat()

        data = (
            uri,
            now,
            "YouTube API",
            label,
            content_json,
            len(content_json.encode("utf-8")),
            now
        )

        with conn:
            conn.execute("""
                INSERT INTO scraped_data (uri, datetime, source, label, content, content_size_bytes, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(uri) DO UPDATE SET
                    datetime=excluded.datetime,
                    label=excluded.label,
                    content=excluded.content,
                    content_size_bytes=excluded.content_size_bytes,
                    created_at=excluded.created_at;
            """, data)


# ---------- Utilities ----------
def read_topics(path: str) -> List[str]:
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


def load_proxies(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


PROXIES = load_proxies(PROXIES_FILE) if os.path.exists(PROXIES_FILE) else []
log.info(f"Loaded {len(PROXIES)} proxies.")


def get_random_proxy() -> Optional[GenericProxyConfig]:
    if not PROXIES:
        return None
    proxy = random.choice(PROXIES)
    try:
        ip, port, user, pwd = proxy.split(":")
        proxy_url = f"http://{user}:{pwd}@{ip}:{port}"
        return GenericProxyConfig(http_url=proxy_url, https_url=proxy_url)
    except Exception:
        return None


# ---------- Transcript Fetch (using pytubefix captions) ----------
from pytubefix import YouTube

def fetch_transcript(video_id: str) -> Optional[str]:
    """
    Fetch transcript using pytubefix captions.
    Handles both manual and auto-generated captions.
    Falls back to the first available caption if no English ones are found.
    """
    url = f"https://www.youtube.com/watch?v={video_id}"
    try:
        yt = YouTube(url)
    except Exception as e:
        log.warning(f"[TRANSCRIPT] {video_id}: failed to load video ({e})")
        return None

    # List available captions
    try:
        captions = yt.captions
        if not captions:
            log.info(f"[TRANSCRIPT] {video_id}: no captions available")
            return None
    except Exception as e:
        log.warning(f"[TRANSCRIPT] {video_id}: caption fetch failed ({e})")
        return None

    # Preferred English → Auto English → First available
    caption = None
    preferred_codes = ['en', 'a.en']
    for code in preferred_codes:
        if code in captions:
            caption = captions[code]
            break

    if not caption:
        try:
            first_key = list(captions.lang_code_index.keys())[0]
            caption = captions[first_key]
        except Exception as e:
            log.warning(f"[TRANSCRIPT] {video_id}: caption selection failed ({e})")
            return None

    # Try generating transcript text
    try:
        srt_text = caption.generate_srt_captions()
        transcript_lines = []
        for line in srt_text.splitlines():
            if "-->" not in line and line.strip() and not line.isdigit():
                transcript_lines.append(line.strip())
        transcript_text = " ".join(transcript_lines)
        log.info(f"[TRANSCRIPT] {video_id}: fetched caption ({caption.code})")
        return transcript_text
    except Exception as e:
        log.warning(f"[TRANSCRIPT] {video_id}: failed to parse caption ({e})")
        return None

# ---------- YouTube API ----------
def build_youtube_client():
    if not API_KEY:
        raise RuntimeError("❌ Missing YOUTUBE_API_KEY. Set it in .env or environment variables.")
    return build(API_SERVICE_NAME, API_VERSION, developerKey=API_KEY, cache_discovery=False)


def search_videos(youtube, query: str, max_results: int = 5) -> List[dict]:
    try:
        response = youtube.search().list(
            q=query,
            part="snippet",
            type="video",
            order="date",
            maxResults=max_results,
            publishedAfter=(datetime.datetime.utcnow() - relativedelta(days=30)).isoformat("T") + "Z",
        ).execute()
        return response.get("items", [])
    except Exception as e:
        log.error(f"[YouTube API] Search error for '{query}': {e}")
        return []


def get_video_details(youtube, video_ids: List[str]) -> List[dict]:
    try:
        response = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=",".join(video_ids)
        ).execute()
        return response.get("items", [])
    except Exception as e:
        log.error(f"[YouTube API] details error: {e}")
        return []


def get_channel_subscribers(youtube, channel_id: str) -> Optional[int]:
    try:
        response = youtube.channels().list(part="statistics", id=channel_id).execute()
        items = response.get("items", [])
        if items:
            return int(items[0]["statistics"].get("subscriberCount", 0))
    except Exception as e:
        log.warning(f"[Channel] Error fetching subscribers for {channel_id}: {e}")
    return None


# ---------- Main Processing ----------
def process_topic(topic: str, conn: sqlite3.Connection):
    topic = topic.strip()
    if not topic:
        return
    log.info(f"=== Topic: {topic} ===")

    youtube = build_youtube_client()
    search_results = search_videos(youtube, topic, MAX_PER_TOPIC)
    if not search_results:
        log.info(f"[{topic}] No results.")
        return

    video_ids = [item["id"]["videoId"] for item in search_results if "videoId" in item["id"]]
    details = get_video_details(youtube, video_ids)

    for video in details:
        snippet = video["snippet"]
        stats = video.get("statistics", {})
        content = video.get("contentDetails", {})
        vid = video["id"]
        upload_date = snippet.get("publishedAt")

        if not within_last_n_days(upload_date, 30):
            continue

        log.info(f"[{topic}] Processing {vid} - {snippet.get('title')}")
        time.sleep(RATE_LIMIT_SECONDS)

        transcript_text = fetch_transcript(vid)

        channel_id = snippet.get("channelId")
        subscriber_count = get_channel_subscribers(youtube, channel_id) if channel_id else None

        record = {
            "Video_id": vid,
            "title": snippet.get("title"),
            "url": f"https://www.youtube.com/watch?v={vid}",
            "channel_name": snippet.get("channelTitle"),
            "upload_date": upload_date,
            "duration_seconds": int(isodate.parse_duration(content.get("duration")).total_seconds()) if content.get("duration") else None,
            "language": snippet.get("defaultLanguage"),
            "description": snippet.get("description"),
            "thumbnails": snippet.get("thumbnails", {}),
            "view_count": int(stats.get("viewCount", 0)) if "viewCount" in stats else None,
            "like_count": int(stats.get("likeCount", 0)) if "likeCount" in stats else None,
            "subscriber_count": subscriber_count,
            "transcript": transcript_text,
            "label": topic,
        }

        upsert_scraped(conn, record)
        log.info(f"[{topic}] Saved {vid}")


# ---------- Entrypoint ----------
def main():
    conn = open_db(DB_FILE)
    topics = read_topics(TOPICS_FILE)
    if not topics:
        log.error("No topics found in topics.txt")
        return

    log.info(f"Starting scrape for {len(topics)} topics using official YouTube API.")
    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = [ex.submit(process_topic, topic, conn) for topic in topics]
        for fut in concurrent.futures.as_completed(futures):
            try:
                fut.result()
            except Exception as e:
                log.error(f"Worker crashed: {e}")

    log.info("✅ All done. Inspect DB file: %s", DB_FILE)


if __name__ == "__main__":
    main()
