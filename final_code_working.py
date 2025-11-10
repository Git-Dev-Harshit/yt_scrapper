#!/usr/bin/env python3
"""
Fetch YouTube search results & transcripts using pytubefix:
- Handles videos, shorts, playlists, and channels
- Fetches channel subscriber count (via About page) and transcripts where possible
- Saves data to SQLite and upserts in real-time so you can query while scraping.

Schema:
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

import os
import re
import json
import time
import datetime
import logging
import sqlite3
import threading
import concurrent.futures
import random
from typing import List, Optional, Dict
from urllib.parse import urlparse, parse_qs
from dateutil import parser as dateparser
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

from pytubefix import YouTube
from pytubefix.contrib.search import Search
from pytubefix.contrib.channel import Channel
from pytubefix.contrib.playlist import Playlist

# ---------- Config ----------
load_dotenv()

TOPICS_FILE = "topics.txt"
DB_FILE = "youtube_scraped_data.db"
PROXIES_FILE = "proxies.txt"
MAX_PER_TOPIC = 100000
WORKERS = 6
RATE_LIMIT_SECONDS = 2
SOURCE_NAME = "3"

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(threadName)s: %(message)s"
)
log = logging.getLogger("yt_scraper")

# ---------- Database schema & lock ----------
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


def load_proxies(path: str) -> List[str]:
    """Load proxies from file. Format: ip:port:username:password"""
    if not os.path.exists(path):
        log.warning(f"Proxies file not found: {path}. Running without proxies.")
        return []
    
    proxies = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    # Parse format: ip:port:username:password
                    parts = line.split(":")
                    if len(parts) >= 4:
                        ip, port, username, password = parts[0], parts[1], parts[2], parts[3]
                        proxy_url = f"http://{username}:{password}@{ip}:{port}"
                        proxies.append(proxy_url)
        log.info(f"Loaded {len(proxies)} proxies from {path}")
    except Exception as e:
        log.error(f"Error loading proxies: {e}")
    
    return proxies


class ProxyRotator:
    """Manages rotating proxy selection"""
    def __init__(self, proxies: List[str]):
        self.proxies = proxies
        self.current_index = 0
        self.lock = threading.Lock()
    
    def get_next_proxy(self) -> Optional[Dict]:
        """Get next proxy in rotation"""
        if not self.proxies:
            return None
        
        with self.lock:
            proxy = self.proxies[self.current_index]
            self.current_index = (self.current_index + 1) % len(self.proxies)
        
        return {
            "http": proxy,
            "https": proxy
        }
    
    def get_random_proxy(self) -> Optional[Dict]:
        """Get random proxy for diversity"""
        if not self.proxies:
            return None
        
        proxy = random.choice(self.proxies)
        return {
            "http": proxy,
            "https": proxy
        }


# Load proxies at startup
_proxies_list = load_proxies(PROXIES_FILE)
_proxy_rotator = ProxyRotator(_proxies_list) if _proxies_list else None


def open_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
    # Apply schema and pragmas
    with conn:
        for stmt in CREATE_SQL.strip().split(";"):
            s = stmt.strip()
            if s:
                conn.execute(s)
    return conn


def upsert_scraped(conn: sqlite3.Connection, uri: str, label: str, rec: dict):
    """
    Insert or update a record in scraped_data.
    - uri: unique uri for the record (video url, channel url, playlist url)
    - label: topic / label
    - rec: arbitrary dict -> stored as JSON in content column
    """
    with _db_lock:
        content_json = json.dumps(rec, ensure_ascii=False)
        now = datetime.datetime.utcnow().isoformat()
        data = (
            uri,
            now,
            SOURCE_NAME,
            label,
            content_json,
            len(content_json.encode("utf-8")),
            now,
        )

        with conn:
            conn.execute("""
                INSERT INTO scraped_data (uri, datetime, source, label, content, content_size_bytes, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(uri) DO UPDATE SET
                    datetime=excluded.datetime,
                    source=excluded.source,
                    label=excluded.label,
                    content=excluded.content,
                    content_size_bytes=excluded.content_size_bytes,
                    created_at=excluded.created_at;
            """, data)


# ---------- Utilities ----------
def read_topics(path: str) -> List[str]:
    if not os.path.exists(path):
        log.error(f"Missing topics file: {path}")
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


def normalize_youtube_url(url: str) -> str:
    """Cleans YouTube URLs by removing unnecessary query params."""
    parsed = urlparse(url)
    if "watch" in parsed.path:
        query = parse_qs(parsed.query)
        if "v" in query:
            return f"https://www.youtube.com/watch?v={query['v'][0]}"
    if "playlist" in parsed.path:
        query = parse_qs(parsed.query)
        if "list" in query:
            return f"https://www.youtube.com/playlist?list={query['list'][0]}"
    if "channel/" in parsed.path or "@" in parsed.path:
        return url.split("?")[0]
    return url


# ---------- Channel Subscriber Count ----------
_channel_cache: Dict[str, Optional[int]] = {}


def extract_subscriber_count_from_html(html: str) -> Optional[int]:
    if not html:
        return None
    # attempts to catch "1.2M subscribers", "123,456 subscribers", etc.
    match = re.search(r'([\d.,]+)\s*([MK]?)\s*subscribers', html, re.IGNORECASE)
    if not match:
        return None
    num, suffix = match.groups()
    try:
        val = float(num.replace(",", ""))
        if suffix and suffix.upper() == "M":
            val *= 1_000_000
        elif suffix and suffix.upper() == "K":
            val *= 1_000
        return int(val)
    except Exception:
        return None


def get_channel_subscribers(channel_url: str) -> Optional[int]:
    if not channel_url:
        return None
    channel_url = normalize_youtube_url(channel_url)
    if channel_url in _channel_cache:
        return _channel_cache[channel_url]

    try:
        # Use rotating proxy
        proxies = _proxy_rotator.get_next_proxy() if _proxy_rotator else None
        ch = Channel(channel_url, proxies=proxies) if proxies else Channel(channel_url)
        subs = extract_subscriber_count_from_html(ch.about_html)
        _channel_cache[channel_url] = subs
        return subs
    except Exception as e:
        log.warning(f"[Channel] Failed to fetch subscribers for {channel_url} (proxy attempt): {e}")
        # Retry without proxy
        try:
            ch = Channel(channel_url)
            subs = extract_subscriber_count_from_html(ch.about_html)
            _channel_cache[channel_url] = subs
            return subs
        except Exception as e2:
            log.warning(f"[Channel] Failed to fetch subscribers for {channel_url} (fallback): {e2}")
            _channel_cache[channel_url] = None
            return None


# ---------- Transcript ----------
def fetch_transcript(video_id: str) -> Optional[str]:
    """Fetch transcript (auto/manual) if available using pytubefix captions."""
    url = f"https://www.youtube.com/watch?v={video_id}"
    try:
        # Use rotating proxy
        proxies = _proxy_rotator.get_next_proxy() if _proxy_rotator else None
        yt = YouTube(url, proxies=proxies) if proxies else YouTube(url)
    except Exception as e:
        log.warning(f"[Transcript] Failed to load video {video_id} (attempt 1): {e}")
        # Retry without proxy as fallback
        try:
            yt = YouTube(url)
        except Exception as e2:
            log.warning(f"[Transcript] Failed to load video {video_id} (attempt 2): {e2}")
            return None

    try:
        captions = yt.captions
        if not captions:
            return None
    except Exception:
        return None

    caption = None
    for code in ["en", "a.en"]:
        if code in captions:
            caption = captions[code]
            break

    if not caption:
        try:
            first_key = list(captions.lang_code_index.keys())[0]
            caption = captions[first_key]
        except Exception:
            return None

    try:
        srt_text = caption.generate_srt_captions()
        lines = [
            line.strip() for line in srt_text.splitlines()
            if "-->" not in line and line.strip() and not line.isdigit()
        ]
        return " ".join(lines)
    except Exception as e:
        log.warning(f"[Transcript] Failed to parse captions for {video_id}: {e}")
        return None


# ---------- Entity Handlers ----------
def build_video_record(yt: YouTube, topic: str) -> dict:
    upload_date = yt.publish_date.isoformat() if yt.publish_date else None
    transcript = fetch_transcript(yt.video_id)
    subs = get_channel_subscribers(yt.channel_url)
    record = {
        "type": "video",
        "video_id": yt.video_id,
        "title": yt.title,
        "url": yt.watch_url,
        "channel_name": yt.author,
        "channel_url": yt.channel_url,
        "subscriber_count": subs,
        "upload_date": upload_date,
        "duration_seconds": int(yt.length) if yt.length is not None else None,
        "view_count": yt.views,
        "like_count": getattr(yt, "likes", None),
        "description": yt.description,
        "language_codes": list(yt.captions.lang_code_index.keys()) if yt.captions else [],
        "thumbnail_url": yt.thumbnail_url,
        "transcript": transcript,
        "label": topic,
    }
    return record


def handle_video(item, topic: str, conn: sqlite3.Connection):
    try:
        # Use rotating proxy
        proxies = _proxy_rotator.get_next_proxy() if _proxy_rotator else None
        yt = YouTube(item.watch_url, proxies=proxies) if proxies else YouTube(item.watch_url)
    except Exception as e:
        log.warning(f"[{topic}] Skipping video load error (proxy attempt): {e}")
        # Retry without proxy
        try:
            yt = YouTube(item.watch_url)
        except Exception as e2:
            log.warning(f"[{topic}] Skipping video load error (fallback): {e2}")
            return False

    upload_date = yt.publish_date.isoformat() if yt.publish_date else None
    if not upload_date or not within_last_n_days(upload_date, 30):
        return False

    # be kind to remote endpoints
    time.sleep(RATE_LIMIT_SECONDS)

    record = build_video_record(yt, topic)
    uri = normalize_youtube_url(record["url"])
    upsert_scraped(conn, uri, topic, record)
    log.info(f"[{topic}] ✅ Saved video {record['video_id']} - {record['title']}")
    return True


def handle_short(item, topic: str, conn: sqlite3.Connection):
    try:
        # Use rotating proxy
        proxies = _proxy_rotator.get_next_proxy() if _proxy_rotator else None
        yt = YouTube(item.watch_url, proxies=proxies) if proxies else YouTube(item.watch_url)
    except Exception as e:
        log.warning(f"[{topic}] Short fetch failed (proxy attempt): {e}")
        # Retry without proxy
        try:
            yt = YouTube(item.watch_url)
        except Exception as e2:
            log.warning(f"[{topic}] Short fetch failed (fallback): {e2}")
            return False

    time.sleep(RATE_LIMIT_SECONDS)

    subs = get_channel_subscribers(yt.channel_url)
    record = {
        "type": "short",
        "video_id": yt.video_id,
        "title": yt.title,
        "url": yt.watch_url,
        "channel_name": yt.author,
        "channel_url": yt.channel_url,
        "subscriber_count": subs,
        "view_count": yt.views,
        "duration_seconds": yt.length,
        "label": topic,
    }
    uri = normalize_youtube_url(record["url"])
    upsert_scraped(conn, uri, topic, record)
    log.info(f"[{topic}] ✅ Saved short {record['video_id']} - {record['title']}")
    return True


def handle_channel(item, topic: str, conn: sqlite3.Connection):
    try:
        # Use rotating proxy
        proxies = _proxy_rotator.get_next_proxy() if _proxy_rotator else None
        ch = Channel(item.channel_url, proxies=proxies) if proxies else Channel(item.channel_url)
    except Exception as e:
        log.warning(f"[{topic}] Channel scrape failed (proxy attempt): {e}")
        # Retry without proxy
        try:
            ch = Channel(item.channel_url)
        except Exception as e2:
            log.warning(f"[{topic}] Channel scrape failed (fallback): {e2}")
            return False

    subs = extract_subscriber_count_from_html(ch.about_html)
    record = {
        "type": "channel",
        "title": ch.channel_name,
        "url": ch.channel_url,
        "subscriber_count": subs,
        "description_snippet": (ch.about_html or "")[:500],
        "label": topic,
    }
    uri = normalize_youtube_url(record["url"])
    upsert_scraped(conn, uri, topic, record)
    log.info(f"[{topic}] ✅ Saved channel {record['title']}")
    return True


def handle_playlist(item, topic: str, conn: sqlite3.Connection):
    try:
        # Use rotating proxy
        proxies = _proxy_rotator.get_next_proxy() if _proxy_rotator else None
        pl = Playlist(item.playlist_url, proxies=proxies) if proxies else Playlist(item.playlist_url)
    except Exception as e:
        log.warning(f"[{topic}] Playlist scrape failed (proxy attempt): {e}")
        # Retry without proxy
        try:
            pl = Playlist(item.playlist_url)
        except Exception as e2:
            log.warning(f"[{topic}] Playlist scrape failed (fallback): {e2}")
            return False

    subs = None
    if hasattr(pl, "owner_url"):
        subs = get_channel_subscribers(pl.owner_url)

    record = {
        "type": "playlist",
        "title": pl.title,
        "url": pl.playlist_url,
        "video_count": len(pl.video_urls) if hasattr(pl, "video_urls") else None,
        "videos_sample": (pl.video_urls[:5] if hasattr(pl, "video_urls") else []),
        "subscriber_count": subs,
        "label": topic,
    }
    uri = normalize_youtube_url(record["url"])
    upsert_scraped(conn, uri, topic, record)
    log.info(f"[{topic}] ✅ Saved playlist {record['title']}")
    return True


# ---------- Process Topic ----------
def process_topic(topic: str, conn: sqlite3.Connection):
    topic = topic.strip()
    if not topic:
        return
    log.info(f"=== Topic: {topic} ===")

    try:
        search_results = Search(topic).results
    except Exception as e:
        log.error(f"Search failed for topic '{topic}': {e}")
        return

    count = 0
    for item in search_results:
        if count >= MAX_PER_TOPIC:
            break

        saved = False
        try:
            if hasattr(item, "watch_url"):  # video or short
                if "shorts" in item.watch_url:
                    saved = handle_short(item, topic, conn)
                else:
                    saved = handle_video(item, topic, conn)
            elif hasattr(item, "channel_url"):
                saved = handle_channel(item, topic, conn)
            elif hasattr(item, "playlist_url"):
                saved = handle_playlist(item, topic, conn)
        except Exception as e:
            log.warning(f"[{topic}] Error handling item: {e}")
            saved = False

        if saved:
            count += 1

    log.info(f"[{topic}] Done. {count} items scraped.")


# ---------- Main ----------
def main():
    conn = open_db(DB_FILE)
    topics = read_topics(TOPICS_FILE)
    if not topics:
        log.error("No topics found in topics file.")
        return

    log.info(f"Starting scrape for {len(topics)} topics.")
    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = [ex.submit(process_topic, topic, conn) for topic in topics]
        for fut in concurrent.futures.as_completed(futures):
            try:
                fut.result()
            except Exception as e:
                log.error(f"Worker crashed: {e}")

    log.info(f"✅ All done. Inspect DB file: {DB_FILE}")


if __name__ == "__main__":
    main()