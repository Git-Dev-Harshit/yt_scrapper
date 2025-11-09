#!/usr/bin/env python3
"""
Fetch YouTube videos & transcripts using pytubefix:
- Video search, metadata, transcripts, and channel subscriber count
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
import re
import json
import time
import random
import datetime
import logging
import sqlite3
import threading
import concurrent.futures
from typing import List, Optional, Dict
from dateutil import parser as dateparser
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
import isodate

from pytubefix import YouTube
from pytubefix.contrib.search import Search
from pytubefix.contrib.channel import Channel
from youtube_transcript_api.proxies import GenericProxyConfig

# ---------- Config ----------
load_dotenv()

TOPICS_FILE = "topics.txt"
PROXIES_FILE = "proxies.txt"
DB_FILE = "youtube_scraped_data.db"
MAX_PER_TOPIC = 30
WORKERS = 6
RATE_LIMIT_SECONDS = 3
SOURCE_NAME = "3"

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
            SOURCE_NAME,
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
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


PROXIES = load_proxies(PROXIES_FILE)
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


# ---------- Helper: Channel Subscriber Count ----------
_channel_cache: Dict[str, Optional[int]] = {}


def parse_subscriber_count(text: str) -> Optional[int]:
    """
    Convert strings like '1.2M subscribers' -> 1200000.
    Handles 'No subscribers', 'subscribers hidden', and unexpected formats gracefully.
    """
    if not text:
        return None

    lower_text = text.lower()
    if "no subscribers" in lower_text or "hidden" in lower_text:
        return None

    match = re.search(r"([\d.,]+)\s*([MK]?)\s*subscribers", text, re.IGNORECASE)
    if not match:
        return None

    try:
        num_str, suffix = match.groups()
        num = float(num_str.replace(",", ""))
        if suffix.upper() == "M":
            num *= 1_000_000
        elif suffix.upper() == "K":
            num *= 1_000
        return int(num)
    except Exception:
        return None


def get_channel_subscribers_pytubefix(channel_url: str) -> Optional[int]:
    """
    Try to get subscriber count by parsing Channel.about_html.
    Returns None if hidden/unavailable. Uses in-memory cache to avoid duplicate requests.
    """
    if not channel_url:
        return None
    if channel_url in _channel_cache:
        return _channel_cache[channel_url]

    try:
        channel = Channel(channel_url)
        about_html = channel.about_html
        subs = parse_subscriber_count(about_html)
        if subs is None:
            log.info(f"[Channel] {channel.channel_name}: subscribers hidden or unavailable")
        else:
            log.info(f"[Channel] {channel.channel_name}: {subs} subscribers")
        _channel_cache[channel_url] = subs
        return subs
    except Exception as e:
        log.warning(f"[Channel] Failed to get subscriber count for {channel_url}: {e}")
        _channel_cache[channel_url] = None
        return None


# ---------- Transcript Fetch ----------
def fetch_transcript(video_id: str) -> Optional[str]:
    """
    Fetch transcript using pytubefix captions.
    """
    url = f"https://www.youtube.com/watch?v={video_id}"
    try:
        yt = YouTube(url)
    except Exception as e:
        log.warning(f"[TRANSCRIPT] {video_id}: failed to load video ({e})")
        return None

    try:
        captions = yt.captions
        if not captions:
            return None
    except Exception as e:
        log.warning(f"[TRANSCRIPT] {video_id}: caption fetch failed ({e})")
        return None

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
        except Exception:
            return None

    try:
        srt_text = caption.generate_srt_captions()
        transcript_lines = [
            line.strip() for line in srt_text.splitlines()
            if "-->" not in line and line.strip() and not line.isdigit()
        ]
        return " ".join(transcript_lines)
    except Exception as e:
        log.warning(f"[TRANSCRIPT] {video_id}: failed to parse caption ({e})")
        return None


# ---------- Main Processing ----------
def process_topic(topic: str, conn: sqlite3.Connection):
    topic = topic.strip()
    if not topic:
        return
    log.info(f"=== Topic: {topic} ===")

    search_results = Search(topic).results
    if not search_results:
        log.info(f"[{topic}] No search results.")
        return

    count = 0
    for result in search_results:
        if count >= MAX_PER_TOPIC:
            break

        video_url = result.watch_url
        try:
            yt = YouTube(video_url)
        except Exception as e:
            log.warning(f"[{topic}] Failed to load video {video_url}: {e}")
            continue

        upload_date = yt.publish_date.isoformat() if yt.publish_date else None
        if not upload_date or not within_last_n_days(upload_date, 30):
            continue

        time.sleep(RATE_LIMIT_SECONDS)

        transcript_text = fetch_transcript(yt.video_id)
        subscriber_count = get_channel_subscribers_pytubefix(yt.channel_url)

        record = {
            "Video_id": yt.video_id,
            "title": yt.title,
            "url": yt.watch_url,
            "channel_name": yt.author,
            "upload_date": upload_date,
            "duration_seconds": int(yt.length),
            "language": list(yt.captions.lang_code_index.keys()),
            "description": yt.description,
            "thumbnails": yt.thumbnail_url,
            "view_count": yt.views,
            "like_count": getattr(yt, "likes", None),
            "subscriber_count": subscriber_count,
            "transcript": transcript_text,
            "label": topic,
        }

        upsert_scraped(conn, record)
        count += 1
        log.info(f"[{topic}] Saved {yt.video_id} ({yt.title})")

    log.info(f"[{topic}] Completed. Total saved: {count}")


# ---------- Entrypoint ----------
def main():
    conn = open_db(DB_FILE)
    topics = read_topics(TOPICS_FILE)
    if not topics:
        log.error("No topics found in topics.txt")
        return

    log.info(f"Starting scrape for {len(topics)} topics using pytubefix.")
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
