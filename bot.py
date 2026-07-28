"""
LinkRemover Pro Bot
===================
A production-ready Telegram bot that removes links from forwarded messages
while preserving all formatting, emoji, and content.

Author: LinkRemover Pro
Version: 2.0.0
Python: 3.12+
Framework: aiogram 3.x
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import sqlite3
import time
import traceback
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from logging.handlers import TimedRotatingFileHandler
from typing import Any, Optional, Union

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramForbiddenError, TelegramRetryAfter
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.types import (
    Animation,
    Audio,
    BotCommand,
    BotCommandScopeDefault,
    CallbackQuery,
    Contact,
    Document,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaAnimation,
    InputMediaAudio,
    InputMediaDocument,
    InputMediaPhoto,
    InputMediaVideo,
    Location,
    Message,
    MessageEntity,
    PhotoSize,
    Poll,
    Sticker,
    Video,
    Voice,
)

# ============================================================
# CONFIGURATION
# ============================================================

BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
ADMIN_IDS_RAW: str = os.getenv("ADMIN_IDS", "")
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()
DB_PATH: str = os.getenv("DB_PATH", "linkremover.db")
BOT_VERSION: str = "2.0.0"
BOT_NAME: str = "LinkRemover Pro"

ADMIN_IDS: list[int] = []
for _aid in ADMIN_IDS_RAW.split(","):
    _aid = _aid.strip()
    if _aid.isdigit():
        ADMIN_IDS.append(int(_aid))

# Rate limiting: max requests per window
RATE_LIMIT_REQUESTS: int = int(os.getenv("RATE_LIMIT_REQUESTS", "10"))
RATE_LIMIT_WINDOW: int = int(os.getenv("RATE_LIMIT_WINDOW", "30"))

# ============================================================
# LOGGING
# ============================================================

def setup_logging() -> logging.Logger:
    """Configure professional logging with console + daily rotating file."""
    log_format = (
        "%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s"
    )
    date_format = "%Y-%m-%d %H:%M:%S"

    numeric_level = getattr(logging, LOG_LEVEL, logging.INFO)
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)

    # Remove existing handlers
    root_logger.handlers.clear()

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(logging.Formatter(log_format, date_format))
    root_logger.addHandler(console_handler)

    # File handler with daily rotation (keep 30 days)
    try:
        os.makedirs("logs", exist_ok=True)
        file_handler = TimedRotatingFileHandler(
            filename="logs/bot.log",
            when="midnight",
            interval=1,
            backupCount=30,
            encoding="utf-8",
        )
        file_handler.setLevel(numeric_level)
        file_handler.setFormatter(logging.Formatter(log_format, date_format))
        root_logger.addHandler(file_handler)
    except Exception as exc:
        root_logger.warning("Could not create file log handler: %s", exc)

    # Silence noisy libraries
    logging.getLogger("aiogram").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)

    return logging.getLogger("linkremover")


logger = setup_logging()

# ============================================================
# DATABASE
# ============================================================

_DB_LOCK = asyncio.Lock()


def get_db_connection() -> sqlite3.Connection:
    """Return a SQLite connection with WAL mode for concurrency."""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db() -> None:
    """Initialize all database tables."""
    conn = get_db_connection()
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                id              INTEGER PRIMARY KEY,
                telegram_id     INTEGER UNIQUE NOT NULL,
                username        TEXT,
                first_name      TEXT,
                last_name       TEXT,
                join_date       TEXT NOT NULL DEFAULT (datetime('now')),
                last_seen       TEXT NOT NULL DEFAULT (datetime('now')),
                is_banned       INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS settings (
                id                  INTEGER PRIMARY KEY,
                telegram_id         INTEGER UNIQUE NOT NULL,
                remove_links        INTEGER NOT NULL DEFAULT 1,
                remove_usernames    INTEGER NOT NULL DEFAULT 0,
                remove_hashtags     INTEGER NOT NULL DEFAULT 0,
                remove_emails       INTEGER NOT NULL DEFAULT 0,
                remove_phones       INTEGER NOT NULL DEFAULT 0,
                custom_header       TEXT,
                custom_footer       TEXT,
                language            TEXT NOT NULL DEFAULT 'en',
                remember_enabled    INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY (telegram_id) REFERENCES users(telegram_id)
            );

            CREATE TABLE IF NOT EXISTS statistics (
                id              INTEGER PRIMARY KEY,
                telegram_id     INTEGER NOT NULL,
                stat_date       TEXT NOT NULL DEFAULT (date('now')),
                messages_cleaned INTEGER NOT NULL DEFAULT 0,
                photos_cleaned  INTEGER NOT NULL DEFAULT 0,
                videos_cleaned  INTEGER NOT NULL DEFAULT 0,
                documents_cleaned INTEGER NOT NULL DEFAULT 0,
                audios_cleaned  INTEGER NOT NULL DEFAULT 0,
                voices_cleaned  INTEGER NOT NULL DEFAULT 0,
                animations_cleaned INTEGER NOT NULL DEFAULT 0,
                errors          INTEGER NOT NULL DEFAULT 0,
                total_processing_ms INTEGER NOT NULL DEFAULT 0,
                UNIQUE(telegram_id, stat_date)
            );

            CREATE TABLE IF NOT EXISTS global_stats (
                id              INTEGER PRIMARY KEY CHECK (id = 1),
                total_users     INTEGER NOT NULL DEFAULT 0,
                total_messages  INTEGER NOT NULL DEFAULT 0,
                total_photos    INTEGER NOT NULL DEFAULT 0,
                total_videos    INTEGER NOT NULL DEFAULT 0,
                total_documents INTEGER NOT NULL DEFAULT 0,
                total_audios    INTEGER NOT NULL DEFAULT 0,
                total_voices    INTEGER NOT NULL DEFAULT 0,
                total_animations INTEGER NOT NULL DEFAULT 0,
                total_errors    INTEGER NOT NULL DEFAULT 0,
                bot_start_time  TEXT NOT NULL DEFAULT (datetime('now'))
            );

            INSERT OR IGNORE INTO global_stats (id) VALUES (1);

            CREATE TABLE IF NOT EXISTS broadcast_log (
                id          INTEGER PRIMARY KEY,
                admin_id    INTEGER NOT NULL,
                message     TEXT NOT NULL,
                sent_count  INTEGER NOT NULL DEFAULT 0,
                fail_count  INTEGER NOT NULL DEFAULT 0,
                sent_at     TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS word_replacements (
                id          INTEGER PRIMARY KEY,
                telegram_id INTEGER NOT NULL,
                find_word   TEXT NOT NULL,
                replace_word TEXT NOT NULL,
                UNIQUE(telegram_id, find_word)
            );
        """)
        conn.commit()
        logger.info("Database initialized successfully at: %s", DB_PATH)
    finally:
        conn.close()


# --------------- DB helpers (synchronous, called in executor) ---------------

def db_upsert_user(
    telegram_id: int,
    username: Optional[str],
    first_name: Optional[str],
    last_name: Optional[str],
) -> None:
    conn = get_db_connection()
    try:
        conn.execute("""
            INSERT INTO users (telegram_id, username, first_name, last_name, join_date, last_seen)
            VALUES (?, ?, ?, ?, datetime('now'), datetime('now'))
            ON CONFLICT(telegram_id) DO UPDATE SET
                username   = excluded.username,
                first_name = excluded.first_name,
                last_name  = excluded.last_name,
                last_seen  = datetime('now')
        """, (telegram_id, username, first_name, last_name))

        conn.execute("""
            INSERT OR IGNORE INTO settings (telegram_id) VALUES (?)
        """, (telegram_id,))

        conn.execute("""
            UPDATE global_stats SET total_users = (
                SELECT COUNT(*) FROM users
            ) WHERE id = 1
        """)
        conn.commit()
    finally:
        conn.close()


def db_get_settings(telegram_id: int) -> dict[str, Any]:
    conn = get_db_connection()
    try:
        row = conn.execute(
            "SELECT * FROM settings WHERE telegram_id = ?", (telegram_id,)
        ).fetchone()
        if row:
            return dict(row)
        return {
            "telegram_id": telegram_id,
            "remove_links": 1,
            "remove_usernames": 0,
            "remove_hashtags": 0,
            "remove_emails": 0,
            "remove_phones": 0,
            "custom_header": None,
            "custom_footer": None,
            "language": "en",
            "remember_enabled": 0,
        }
    finally:
        conn.close()


def db_update_setting(telegram_id: int, field: str, value: Any) -> None:
    allowed_fields = {
        "remove_links", "remove_usernames", "remove_hashtags",
        "remove_emails", "remove_phones", "custom_header",
        "custom_footer", "language", "remember_enabled",
    }
    if field not in allowed_fields:
        raise ValueError(f"Invalid setting field: {field}")
    conn = get_db_connection()
    try:
        conn.execute(
            f"INSERT OR IGNORE INTO settings (telegram_id) VALUES (?)",
            (telegram_id,),
        )
        conn.execute(
            f"UPDATE settings SET {field} = ? WHERE telegram_id = ?",
            (value, telegram_id),
        )
        conn.commit()
    finally:
        conn.close()


def db_record_stat(telegram_id: int, media_type: str, processing_ms: int) -> None:
    """Record per-user and global statistics."""
    field_map = {
        "message":   ("messages_cleaned",   "total_messages"),
        "photo":     ("photos_cleaned",     "total_photos"),
        "video":     ("videos_cleaned",     "total_videos"),
        "document":  ("documents_cleaned",  "total_documents"),
        "audio":     ("audios_cleaned",     "total_audios"),
        "voice":     ("voices_cleaned",     "total_voices"),
        "animation": ("animations_cleaned", "total_animations"),
    }
    user_field, global_field = field_map.get(media_type, ("messages_cleaned", "total_messages"))
    conn = get_db_connection()
    try:
        conn.execute(f"""
            INSERT INTO statistics (telegram_id, {user_field}, total_processing_ms)
            VALUES (?, 1, ?)
            ON CONFLICT(telegram_id, stat_date) DO UPDATE SET
                {user_field} = {user_field} + 1,
                total_processing_ms = total_processing_ms + excluded.total_processing_ms
        """, (telegram_id, processing_ms))
        conn.execute(f"""
            UPDATE global_stats SET {global_field} = {global_field} + 1 WHERE id = 1
        """)
        conn.commit()
    finally:
        conn.close()


def db_record_error(telegram_id: int) -> None:
    conn = get_db_connection()
    try:
        conn.execute("""
            INSERT INTO statistics (telegram_id, errors)
            VALUES (?, 1)
            ON CONFLICT(telegram_id, stat_date) DO UPDATE SET
                errors = errors + 1
        """, (telegram_id,))
        conn.execute(
            "UPDATE global_stats SET total_errors = total_errors + 1 WHERE id = 1"
        )
        conn.commit()
    finally:
        conn.close()


def db_get_global_stats() -> dict[str, Any]:
    conn = get_db_connection()
    try:
        row = conn.execute("SELECT * FROM global_stats WHERE id = 1").fetchone()
        if row:
            return dict(row)
        return {}
    finally:
        conn.close()


def db_get_user_stats(telegram_id: int) -> dict[str, Any]:
    conn = get_db_connection()
    try:
        row = conn.execute("""
            SELECT
                COALESCE(SUM(messages_cleaned), 0)   AS messages_cleaned,
                COALESCE(SUM(photos_cleaned), 0)     AS photos_cleaned,
                COALESCE(SUM(videos_cleaned), 0)     AS videos_cleaned,
                COALESCE(SUM(documents_cleaned), 0)  AS documents_cleaned,
                COALESCE(SUM(audios_cleaned), 0)     AS audios_cleaned,
                COALESCE(SUM(voices_cleaned), 0)     AS voices_cleaned,
                COALESCE(SUM(animations_cleaned), 0) AS animations_cleaned,
                COALESCE(SUM(errors), 0)             AS errors
            FROM statistics WHERE telegram_id = ?
        """, (telegram_id,)).fetchone()
        return dict(row) if row else {}
    finally:
        conn.close()


def db_get_today_users() -> int:
    conn = get_db_connection()
    try:
        row = conn.execute("""
            SELECT COUNT(DISTINCT telegram_id) as cnt
            FROM statistics WHERE stat_date = date('now')
        """).fetchone()
        return row["cnt"] if row else 0
    finally:
        conn.close()


def db_get_all_user_ids() -> list[int]:
    conn = get_db_connection()
    try:
        rows = conn.execute(
            "SELECT telegram_id FROM users WHERE is_banned = 0"
        ).fetchall()
        return [r["telegram_id"] for r in rows]
    finally:
        conn.close()


def db_get_word_replacements(telegram_id: int) -> dict[str, str]:
    conn = get_db_connection()
    try:
        rows = conn.execute(
            "SELECT find_word, replace_word FROM word_replacements WHERE telegram_id = ?",
            (telegram_id,),
        ).fetchall()
        return {r["find_word"]: r["replace_word"] for r in rows}
    finally:
        conn.close()


def db_add_word_replacement(telegram_id: int, find_word: str, replace_word: str) -> None:
    conn = get_db_connection()
    try:
        conn.execute("""
            INSERT INTO word_replacements (telegram_id, find_word, replace_word)
            VALUES (?, ?, ?)
            ON CONFLICT(telegram_id, find_word) DO UPDATE SET replace_word = excluded.replace_word
        """, (telegram_id, find_word, replace_word))
        conn.commit()
    finally:
        conn.close()


def db_get_recent_users(limit: int = 20) -> list[dict[str, Any]]:
    conn = get_db_connection()
    try:
        rows = conn.execute("""
            SELECT telegram_id, username, first_name, last_name, join_date, last_seen
            FROM users ORDER BY last_seen DESC LIMIT ?
        """, (limit,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def db_is_banned(telegram_id: int) -> bool:
    conn = get_db_connection()
    try:
        row = conn.execute(
            "SELECT is_banned FROM users WHERE telegram_id = ?", (telegram_id,)
        ).fetchone()
        return bool(row and row["is_banned"])
    finally:
        conn.close()


# ============================================================
# URL REGEX ENGINE
# ============================================================

# Core TLD pattern (common ones + catch-all)
_TLD = (
    r"(?:com|net|org|edu|gov|mil|int|io|co|ai|app|dev|xyz|info|biz|"
    r"me|tv|cc|gg|ly|gl|tk|cf|ga|gq|ml|ru|uk|de|fr|jp|cn|in|br|au|"
    r"ca|es|it|nl|pl|se|no|fi|dk|ch|at|be|nz|za|sg|hk|my|ph|th|id|"
    r"vn|pk|bd|lk|np|eg|ng|ke|gh|tz|ug|rw|zm|[a-z]{2,})"
)

# Scheme-based URLs: http/https/ftp/ftps
_SCHEME_URL = (
    r"(?:https?|ftps?)"
    r"://"
    r"(?:[a-zA-Z0-9\-._~:/?#\[\]@!$&'()*+,;=%]+"
    r"|(?:[a-zA-Z0-9\-]+\.)+[a-zA-Z]{2,}"
    r"(?:/[^\s<>\"{}|\\^`\[\]]*)?)"
)

# www. URLs without scheme
_WWW_URL = (
    r"www\."
    r"[a-zA-Z0-9\-]+"
    r"(?:\.[a-zA-Z0-9\-]+)*"
    r"\." + _TLD +
    r"(?:/[^\s<>\"{}|\\^`\[\]()]*)?"
)

# Telegram links specifically
_TELEGRAM_URL = (
    r"(?:t\.me|telegram\.me|telegram\.dog)"
    r"(?:/[^\s<>\"{}|\\^`\[\]()]*)?"
)

# Short link domains
_SHORT_URL = (
    r"(?:bit\.ly|tinyurl\.com|goo\.gl|ow\.ly|is\.gd|buff\.ly|"
    r"dlvr\.it|fb\.me|t\.co|ht\.ly|lnkd\.in|youtu\.be|tiny\.cc|"
    r"rb\.gy|cutt\.ly|shorturl\.at|tiny\.one|bl\.ink)"
    r"(?:/[^\s<>\"{}|\\^`\[\]()]*)?"
)

# Cloud storage / social / dev links
_CLOUD_URL = (
    r"(?:drive\.google\.com|docs\.google\.com|dropbox\.com|mega\.nz|"
    r"mega\.io|discord\.gg|discord\.com/invite|youtube\.com|youtu\.be|"
    r"instagram\.com|facebook\.com|fb\.com|twitter\.com|x\.com|"
    r"linkedin\.com|github\.com|gitlab\.com|bitbucket\.org|"
    r"pastebin\.com|hastebin\.com|rentry\.co)"
    r"(?:/[^\s<>\"{}|\\^`\[\]()]*)?"
)

# Bare domain URLs (e.g. example.com/path)
_BARE_DOMAIN = (
    r"(?<![/@\w])"
    r"(?:[a-zA-Z0-9](?:[a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?\.)+"
    + _TLD +
    r"(?:/[^\s<>\"{}|\\^`\[\]()]*)?"
    r"(?!\w)"
)

# Combined master pattern
_URL_PATTERN = re.compile(
    r"(?:"
    + _SCHEME_URL
    + r"|"
    + _TELEGRAM_URL
    + r"|"
    + _SHORT_URL
    + r"|"
    + _CLOUD_URL
    + r"|"
    + _WWW_URL
    + r"|"
    + _BARE_DOMAIN
    + r")",
    re.IGNORECASE | re.UNICODE,
)

# Email pattern
_EMAIL_PATTERN = re.compile(
    r"\b[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}\b",
    re.UNICODE,
)

# Phone number pattern (international formats)
_PHONE_PATTERN = re.compile(
    r"(?<!\w)"
    r"(?:\+?(?:1|7|20|27|30|31|32|33|34|36|39|40|41|43|44|45|46|47|"
    r"48|49|51|52|53|54|55|56|57|58|60|61|62|63|64|65|66|81|82|84|86|"
    r"90|91|92|93|94|95|98|212|213|216|218|220|221|222|223|224|225|226|"
    r"227|228|229|230|231|232|233|234|235|236|237|238|239|240|241|242|"
    r"243|244|245|248|249|250|251|252|253|254|255|256|257|258|260|261|"
    r"262|263|264|265|266|267|268|269|290|291|297|298|299|350|351|352|"
    r"353|354|355|356|357|358|359|370|371|372|373|374|375|376|377|378|"
    r"380|381|382|385|386|387|389|420|421|423|500|501|502|503|504|505|"
    r"506|507|508|509|590|591|592|593|594|595|596|597|598|599|670|672|"
    r"673|674|675|676|677|678|679|680|681|682|683|685|686|687|688|689|"
    r"690|691|692|850|852|853|855|856|880|886|960|961|962|963|964|965|"
    r"966|967|968|970|971|972|973|974|975|976|977|992|993|994|995|996|998)"
    r"[\s\-\.]?)?"
    r"(?:\(?\d{2,4}\)?[\s\-\.]?)?"
    r"\d{3,4}[\s\-\.]?\d{3,4}"
    r"(?!\w)",
    re.UNICODE,
)

# Username pattern (@someone)
_USERNAME_PATTERN = re.compile(r"@[a-zA-Z][a-zA-Z0-9_]{4,31}", re.UNICODE)

# Hashtag pattern (#tag)
_HASHTAG_PATTERN = re.compile(r"#[^\s#@!$%^&*()\-=+\[\]{};:'\",./<>?\\|`~]+", re.UNICODE)


def remove_urls_from_text(text: str, settings: dict[str, Any]) -> str:
    """
    Remove URLs (and optionally usernames, hashtags, emails, phones)
    from plain text while preserving everything else.
    """
    if not text:
        return text

    result = text

    if settings.get("remove_links", 1):
        # Replace URLs with empty string, then clean up leftover blank lines
        result = _URL_PATTERN.sub("", result)

    if settings.get("remove_emails", 0):
        result = _EMAIL_PATTERN.sub("", result)

    if settings.get("remove_phones", 0):
        result = _PHONE_PATTERN.sub("", result)

    if settings.get("remove_usernames", 0):
        result = _USERNAME_PATTERN.sub("", result)

    if settings.get("remove_hashtags", 0):
        result = _HASHTAG_PATTERN.sub("", result)

    # Apply word replacements
    replacements: dict[str, str] = settings.get("_word_replacements", {})
    for find_word, replace_word in replacements.items():
        result = result.replace(find_word, replace_word)

    # Collapse multiple blank lines into at most one blank line
    result = re.sub(r"\n{3,}", "\n\n", result)

    # Strip trailing spaces from lines
    result = "\n".join(line.rstrip() for line in result.split("\n"))

    # Strip leading/trailing whitespace from the whole message
    result = result.strip()

    # Add header / footer if configured
    header: Optional[str] = settings.get("custom_header")
    footer: Optional[str] = settings.get("custom_footer")
    if header:
        result = f"{header}\n{result}" if result else header
    if footer:
        result = f"{result}\n{footer}" if result else footer

    return result


def clean_entities(
    text: str,
    entities: Optional[list[MessageEntity]],
    settings: dict[str, Any],
) -> tuple[str, Optional[list[MessageEntity]]]:
    """
    Clean URLs from text that has Telegram entities (formatting).
    We must adjust entity offsets after removing characters.

    Returns (cleaned_text, adjusted_entities).
    """
    if not text:
        return text, entities

    if not settings.get("remove_links", 1) and not settings.get("remove_emails", 0) \
            and not settings.get("remove_phones", 0) \
            and not settings.get("remove_usernames", 0) \
            and not settings.get("remove_hashtags", 0):
        return text, entities

    # Build list of (start, end) byte ranges of things to remove
    # We work with character indices since aiogram entities use UTF-16 code units
    # but let's work on str directly.

    ranges_to_remove: list[tuple[int, int]] = []

    if settings.get("remove_links", 1):
        for m in _URL_PATTERN.finditer(text):
            ranges_to_remove.append((m.start(), m.end()))

    if settings.get("remove_emails", 0):
        for m in _EMAIL_PATTERN.finditer(text):
            ranges_to_remove.append((m.start(), m.end()))

    if settings.get("remove_phones", 0):
        for m in _PHONE_PATTERN.finditer(text):
            ranges_to_remove.append((m.start(), m.end()))

    if settings.get("remove_usernames", 0):
        for m in _USERNAME_PATTERN.finditer(text):
            ranges_to_remove.append((m.start(), m.end()))

    if settings.get("remove_hashtags", 0):
        for m in _HASHTAG_PATTERN.finditer(text):
            ranges_to_remove.append((m.start(), m.end()))

    if not ranges_to_remove:
        return text, entities

    # Merge overlapping ranges
    ranges_to_remove.sort()
    merged: list[tuple[int, int]] = []
    for start, end in ranges_to_remove:
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))

    # Build cleaned text and a mapping old_char_idx -> new_char_idx
    new_text_parts: list[str] = []
    offset_map: list[int] = []  # index i -> new position of char at old index i
    new_pos = 0
    prev_end = 0

    for rm_start, rm_end in merged:
        # Keep text before this removal
        kept = text[prev_end:rm_start]
        for ch in kept:
            offset_map.append(new_pos)
            new_pos += 1
        new_text_parts.append(kept)
        # Skip removed range
        for _ in range(rm_end - rm_start):
            offset_map.append(-1)  # removed
        prev_end = rm_end

    # Keep tail
    kept = text[prev_end:]
    for ch in kept:
        offset_map.append(new_pos)
        new_pos += 1
    new_text_parts.append(kept)

    new_text = "".join(new_text_parts)

    # Apply word replacements to plain new_text (simple, no entity adjustment needed for these)
    replacements: dict[str, str] = settings.get("_word_replacements", {})
    for find_word, replace_word in replacements.items():
        new_text = new_text.replace(find_word, replace_word)

    # Collapse multiple blank lines
    new_text = re.sub(r"\n{3,}", "\n\n", new_text)
    new_text = "\n".join(line.rstrip() for line in new_text.split("\n"))
    new_text = new_text.strip()

    # Add header / footer
    header = settings.get("custom_header")
    footer = settings.get("custom_footer")
    if header:
        new_text = f"{header}\n{new_text}" if new_text else header
    if footer:
        new_text = f"{new_text}\n{footer}" if new_text else footer

    # Adjust entities
    new_entities: list[MessageEntity] = []
    if entities:
        for ent in entities:
            # Skip url entities if we're removing links
            if ent.type == "url" and settings.get("remove_links", 1):
                continue
            if ent.type == "text_link" and settings.get("remove_links", 1):
                continue

            old_start = ent.offset
            old_end = ent.offset + ent.length

            # Map start
            if old_start < len(offset_map):
                new_start = offset_map[old_start]
                if new_start == -1:
                    # Entity starts in removed range — skip it
                    continue
            else:
                continue

            # Map end (exclusive)
            new_end = new_start
            for i in range(old_start, min(old_end, len(offset_map))):
                mapped = offset_map[i]
                if mapped != -1:
                    new_end = mapped + 1

            new_length = new_end - new_start
            if new_length <= 0:
                continue

            new_entities.append(MessageEntity(
                type=ent.type,
                offset=new_start,
                length=new_length,
                url=ent.url,
                user=ent.user,
                language=ent.language,
                custom_emoji_id=getattr(ent, "custom_emoji_id", None),
            ))

    return new_text, new_entities if new_entities else None


# ============================================================
# RATE LIMITER
# ============================================================

class RateLimiter:
    """Simple in-memory token-bucket rate limiter per user."""

    def __init__(self, max_requests: int, window_seconds: int) -> None:
        self._max = max_requests
        self._window = window_seconds
        self._buckets: dict[int, list[float]] = {}

    def is_allowed(self, user_id: int) -> bool:
        now = time.monotonic()
        window_start = now - self._window
        bucket = self._buckets.get(user_id, [])
        bucket = [t for t in bucket if t > window_start]
        if len(bucket) >= self._max:
            self._buckets[user_id] = bucket
            return False
        bucket.append(now)
        self._buckets[user_id] = bucket
        return True

    def cleanup(self) -> None:
        """Remove expired entries (call periodically)."""
        now = time.monotonic()
        cutoff = now - self._window
        self._buckets = {
            uid: [t for t in times if t > cutoff]
            for uid, times in self._buckets.items()
            if any(t > cutoff for t in times)
        }


_rate_limiter = RateLimiter(RATE_LIMIT_REQUESTS, RATE_LIMIT_WINDOW)

# ============================================================
# KEYBOARDS
# ============================================================

def make_settings_keyboard(settings: dict[str, Any]) -> InlineKeyboardMarkup:
    """Build the settings inline keyboard with toggle buttons."""

    def toggle(val: int) -> str:
        return "✅" if val else "❌"

    rows = [
        [
            InlineKeyboardButton(
                text=f"{toggle(settings.get('remove_links', 1))} Remove Links",
                callback_data="toggle_remove_links",
            )
        ],
        [
            InlineKeyboardButton(
                text=f"{toggle(settings.get('remove_usernames', 0))} Remove @Usernames",
                callback_data="toggle_remove_usernames",
            )
        ],
        [
            InlineKeyboardButton(
                text=f"{toggle(settings.get('remove_hashtags', 0))} Remove #Hashtags",
                callback_data="toggle_remove_hashtags",
            )
        ],
        [
            InlineKeyboardButton(
                text=f"{toggle(settings.get('remove_emails', 0))} Remove Emails",
                callback_data="toggle_remove_emails",
            )
        ],
        [
            InlineKeyboardButton(
                text=f"{toggle(settings.get('remove_phones', 0))} Remove Phones",
                callback_data="toggle_remove_phones",
            )
        ],
        [
            InlineKeyboardButton(
                text=f"{toggle(settings.get('remember_enabled', 0))} Remember Settings",
                callback_data="toggle_remember_enabled",
            )
        ],
        [
            InlineKeyboardButton(text="📊 My Stats", callback_data="my_stats"),
            InlineKeyboardButton(text="🔄 Reset", callback_data="reset_settings"),
        ],
        [
            InlineKeyboardButton(text="❌ Close", callback_data="close_menu"),
        ],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def make_admin_keyboard() -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(text="👥 Users", callback_data="admin_users"),
            InlineKeyboardButton(text="📊 Stats", callback_data="admin_stats"),
        ],
        [
            InlineKeyboardButton(text="📢 Broadcast", callback_data="admin_broadcast_info"),
            InlineKeyboardButton(text="📋 Logs", callback_data="admin_logs"),
        ],
        [
            InlineKeyboardButton(text="❌ Close", callback_data="close_menu"),
        ],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ============================================================
# ROUTERS
# ============================================================

router = Router()

# ============================================================
# HELPER: ensure user exists in DB
# ============================================================

async def ensure_user(message: Message, loop: asyncio.AbstractEventLoop) -> None:
    user = message.from_user
    if not user:
        return
    await loop.run_in_executor(
        None,
        db_upsert_user,
        user.id,
        user.username,
        user.first_name,
        user.last_name,
    )


async def get_user_settings(user_id: int, loop: asyncio.AbstractEventLoop) -> dict[str, Any]:
    settings = await loop.run_in_executor(None, db_get_settings, user_id)
    replacements = await loop.run_in_executor(None, db_get_word_replacements, user_id)
    settings["_word_replacements"] = replacements
    return settings


# ============================================================
# COMMAND HANDLERS
# ============================================================

@router.message(CommandStart())
async def cmd_start(message: Message) -> None:
    loop = asyncio.get_event_loop()
    await ensure_user(message, loop)

    user = message.from_user
    first_name = user.first_name if user else "there"

    text = (
        f"👋 <b>Welcome to {BOT_NAME}, {first_name}!</b>\n\n"
        f"I remove links from forwarded messages while keeping everything else — "
        f"formatting, emoji, bold, italic — completely intact.\n\n"
        f"<b>How to use:</b>\n"
        f"Simply <b>forward any message</b> to me and I'll send it back clean! 🧹\n\n"
        f"<b>Commands:</b>\n"
        f"🔧 /settings — Configure what to remove\n"
        f"📊 /stats — Your cleaning stats\n"
        f"❓ /help — Full help guide\n"
        f"ℹ️ /about — About this bot\n\n"
        f"<i>Start by forwarding any message! ➡️</i>"
    )
    await message.answer(text, parse_mode=ParseMode.HTML)


@router.message(Command("help"))
async def cmd_help(message: Message) -> None:
    loop = asyncio.get_event_loop()
    await ensure_user(message, loop)

    text = (
        f"<b>📖 {BOT_NAME} — Help Guide</b>\n\n"
        f"<b>What I do:</b>\n"
        f"Forward any message to me. I remove all links and send the "
        f"cleaned version back to you.\n\n"
        f"<b>✅ Supported types:</b>\n"
        f"• Text messages\n"
        f"• Photos with captions\n"
        f"• Videos with captions\n"
        f"• Documents with captions\n"
        f"• Audio with captions\n"
        f"• Voice messages\n"
        f"• Animations (GIFs)\n"
        f"• Stickers\n"
        f"• Media Groups / Albums\n\n"
        f"<b>🔗 Links removed:</b>\n"
        f"http://, https://, www., t.me, telegram.me, bit.ly, "
        f"tinyurl, Google Drive, Dropbox, Mega, Discord, YouTube, "
        f"Instagram, Facebook, Twitter/X, LinkedIn, GitHub + all URLs\n\n"
        f"<b>🛡️ Preserved:</b>\n"
        f"Bold, Italic, Underline, Spoiler, Code, Blockquotes, "
        f"Emoji, Hashtags, Usernames (unless enabled in /settings)\n\n"
        f"<b>⚙️ Commands:</b>\n"
        f"/start — Start the bot\n"
        f"/help — This help message\n"
        f"/about — About & version info\n"
        f"/settings — Toggle removal options\n"
        f"/stats — Your statistics\n"
        f"/ping — Check bot latency\n"
        f"/remember on|off — Enable persistent settings\n"
        f"/reset — Reset all settings to default\n"
    )
    await message.answer(text, parse_mode=ParseMode.HTML)


@router.message(Command("about"))
async def cmd_about(message: Message) -> None:
    loop = asyncio.get_event_loop()
    await ensure_user(message, loop)

    text = (
        f"<b>ℹ️ About {BOT_NAME}</b>\n\n"
        f"<b>Version:</b> {BOT_VERSION}\n"
        f"<b>Framework:</b> aiogram 3.x\n"
        f"<b>Python:</b> 3.12+\n"
        f"<b>Database:</b> SQLite\n\n"
        f"<b>Features:</b>\n"
        f"✅ Removes all types of links\n"
        f"✅ Preserves all formatting\n"
        f"✅ Supports all media types\n"
        f"✅ Persistent user settings\n"
        f"✅ Media group / album support\n"
        f"✅ Rate limiting\n"
        f"✅ Professional logging\n"
        f"✅ Production-ready\n\n"
        f"<i>Built with ❤️ for clean messaging.</i>"
    )
    await message.answer(text, parse_mode=ParseMode.HTML)


@router.message(Command("settings"))
async def cmd_settings(message: Message) -> None:
    loop = asyncio.get_event_loop()
    await ensure_user(message, loop)

    user_id = message.from_user.id
    settings = await get_user_settings(user_id, loop)

    text = (
        f"<b>⚙️ Settings</b>\n\n"
        f"Customize what {BOT_NAME} removes from your messages.\n"
        f"Tap a button to toggle on/off."
    )
    await message.answer(
        text,
        reply_markup=make_settings_keyboard(settings),
        parse_mode=ParseMode.HTML,
    )


@router.message(Command("stats"))
async def cmd_stats(message: Message) -> None:
    loop = asyncio.get_event_loop()
    await ensure_user(message, loop)

    user_id = message.from_user.id
    user_stats = await loop.run_in_executor(None, db_get_user_stats, user_id)

    total = (
        user_stats.get("messages_cleaned", 0)
        + user_stats.get("photos_cleaned", 0)
        + user_stats.get("videos_cleaned", 0)
        + user_stats.get("documents_cleaned", 0)
        + user_stats.get("audios_cleaned", 0)
        + user_stats.get("voices_cleaned", 0)
        + user_stats.get("animations_cleaned", 0)
    )

    text = (
        f"<b>📊 Your Statistics</b>\n\n"
        f"💬 Text: {user_stats.get('messages_cleaned', 0)}\n"
        f"🖼️ Photos: {user_stats.get('photos_cleaned', 0)}\n"
        f"🎬 Videos: {user_stats.get('videos_cleaned', 0)}\n"
        f"📄 Documents: {user_stats.get('documents_cleaned', 0)}\n"
        f"🎵 Audios: {user_stats.get('audios_cleaned', 0)}\n"
        f"🎤 Voices: {user_stats.get('voices_cleaned', 0)}\n"
        f"🎞️ Animations: {user_stats.get('animations_cleaned', 0)}\n"
        f"─────────────────\n"
        f"<b>Total cleaned: {total}</b>\n"
        f"⚠️ Errors: {user_stats.get('errors', 0)}"
    )
    await message.answer(text, parse_mode=ParseMode.HTML)


@router.message(Command("ping"))
async def cmd_ping(message: Message) -> None:
    start = time.monotonic()
    sent = await message.answer("🏓 Pong!")
    latency_ms = int((time.monotonic() - start) * 1000)
    await sent.edit_text(f"🏓 Pong! <b>{latency_ms}ms</b>", parse_mode=ParseMode.HTML)


@router.message(Command("remember"))
async def cmd_remember(message: Message, command: CommandObject) -> None:
    loop = asyncio.get_event_loop()
    await ensure_user(message, loop)

    user_id = message.from_user.id
    arg = (command.args or "").strip().lower()

    if arg == "on":
        await loop.run_in_executor(None, db_update_setting, user_id, "remember_enabled", 1)
        await message.answer(
            "✅ <b>Remember enabled!</b>\nYour settings will be saved permanently.",
            parse_mode=ParseMode.HTML,
        )
    elif arg == "off":
        await loop.run_in_executor(None, db_update_setting, user_id, "remember_enabled", 0)
        await message.answer(
            "❌ <b>Remember disabled.</b>\nSettings reset each session.",
            parse_mode=ParseMode.HTML,
        )
    else:
        settings = await get_user_settings(user_id, loop)
        status = "ON ✅" if settings.get("remember_enabled") else "OFF ❌"
        await message.answer(
            f"<b>🧠 Remember System</b>\n\n"
            f"Current status: <b>{status}</b>\n\n"
            f"Usage:\n"
            f"/remember on — Enable persistent settings\n"
            f"/remember off — Disable persistent settings",
            parse_mode=ParseMode.HTML,
        )


@router.message(Command("reset"))
async def cmd_reset(message: Message) -> None:
    loop = asyncio.get_event_loop()
    await ensure_user(message, loop)

    user_id = message.from_user.id

    def reset_settings(uid: int) -> None:
        conn = get_db_connection()
        try:
            conn.execute("""
                UPDATE settings SET
                    remove_links = 1,
                    remove_usernames = 0,
                    remove_hashtags = 0,
                    remove_emails = 0,
                    remove_phones = 0,
                    custom_header = NULL,
                    custom_footer = NULL,
                    language = 'en',
                    remember_enabled = 0
                WHERE telegram_id = ?
            """, (uid,))
            conn.execute(
                "DELETE FROM word_replacements WHERE telegram_id = ?", (uid,)
            )
            conn.commit()
        finally:
            conn.close()

    await loop.run_in_executor(None, reset_settings, user_id)
    await message.answer(
        "🔄 <b>Settings reset to defaults.</b>\n\nAll preferences have been cleared.",
        parse_mode=ParseMode.HTML,
    )


# ============================================================
# ADMIN COMMANDS
# ============================================================

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


@router.message(Command("admin"))
async def cmd_admin(message: Message) -> None:
    if not message.from_user or not is_admin(message.from_user.id):
        await message.answer("⛔ Access denied.")
        return

    loop = asyncio.get_event_loop()
    g = await loop.run_in_executor(None, db_get_global_stats)
    today_users = await loop.run_in_executor(None, db_get_today_users)

    text = (
        f"<b>🛡️ Admin Panel</b>\n\n"
        f"👥 Total Users: {g.get('total_users', 0)}\n"
        f"📅 Today's Active: {today_users}\n"
        f"💬 Messages Cleaned: {g.get('total_messages', 0)}\n"
        f"🖼️ Photos: {g.get('total_photos', 0)}\n"
        f"🎬 Videos: {g.get('total_videos', 0)}\n"
        f"📄 Documents: {g.get('total_documents', 0)}\n"
        f"⚠️ Errors: {g.get('total_errors', 0)}\n"
        f"🚀 Bot started: {g.get('bot_start_time', 'N/A')}"
    )
    await message.answer(text, reply_markup=make_admin_keyboard(), parse_mode=ParseMode.HTML)


@router.message(Command("users"))
async def cmd_users(message: Message) -> None:
    if not message.from_user or not is_admin(message.from_user.id):
        await message.answer("⛔ Access denied.")
        return

    loop = asyncio.get_event_loop()
    users = await loop.run_in_executor(None, db_get_recent_users, 20)

    lines = [f"<b>👥 Recent 20 Users</b>\n"]
    for u in users:
        name = u.get("first_name") or "Unknown"
        username = f"@{u['username']}" if u.get("username") else "no username"
        lines.append(f"• <code>{u['telegram_id']}</code> — {name} ({username})")

    await message.answer("\n".join(lines), parse_mode=ParseMode.HTML)


@router.message(Command("broadcast"))
async def cmd_broadcast(message: Message, command: CommandObject) -> None:
    if not message.from_user or not is_admin(message.from_user.id):
        await message.answer("⛔ Access denied.")
        return

    text = (command.args or "").strip()
    if not text:
        await message.answer(
            "Usage: /broadcast Your message here\n\n"
            "This will send the message to ALL users."
        )
        return

    loop = asyncio.get_event_loop()
    user_ids = await loop.run_in_executor(None, db_get_all_user_ids)
    bot: Bot = message.bot  # type: ignore[assignment]

    status_msg = await message.answer(f"📢 Broadcasting to {len(user_ids)} users...")

    sent = 0
    failed = 0
    for uid in user_ids:
        try:
            await bot.send_message(uid, text)
            sent += 1
            await asyncio.sleep(0.05)  # respect rate limits
        except TelegramForbiddenError:
            failed += 1
        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after)
            try:
                await bot.send_message(uid, text)
                sent += 1
            except Exception:
                failed += 1
        except Exception:
            failed += 1

    # Log broadcast
    def log_broadcast(admin_id: int, msg: str, s: int, f: int) -> None:
        conn = get_db_connection()
        try:
            conn.execute(
                "INSERT INTO broadcast_log (admin_id, message, sent_count, fail_count) VALUES (?,?,?,?)",
                (admin_id, msg, s, f),
            )
            conn.commit()
        finally:
            conn.close()

    await loop.run_in_executor(
        None, log_broadcast, message.from_user.id, text, sent, failed
    )

    await status_msg.edit_text(
        f"✅ Broadcast complete!\n\n"
        f"📤 Sent: {sent}\n"
        f"❌ Failed: {failed}"
    )


@router.message(Command("logs"))
async def cmd_logs(message: Message) -> None:
    if not message.from_user or not is_admin(message.from_user.id):
        await message.answer("⛔ Access denied.")
        return

    log_file = "logs/bot.log"
    if not os.path.exists(log_file):
        await message.answer("📋 No log file found yet.")
        return

    try:
        with open(log_file, "r", encoding="utf-8") as f:
            lines = f.readlines()
        last_lines = "".join(lines[-50:])
        if len(last_lines) > 4000:
            last_lines = last_lines[-4000:]
        await message.answer(
            f"<b>📋 Last 50 log lines:</b>\n\n<pre>{last_lines}</pre>",
            parse_mode=ParseMode.HTML,
        )
    except Exception as exc:
        await message.answer(f"Error reading logs: {exc}")


@router.message(Command("restart"))
async def cmd_restart(message: Message) -> None:
    if not message.from_user or not is_admin(message.from_user.id):
        await message.answer("⛔ Access denied.")
        return
    await message.answer("🔄 Restarting... (use process manager to apply)")
    logger.info("Restart requested by admin %d", message.from_user.id)
    # On Render/Railway, the process manager will restart
    os.execv(__file__, ["python", __file__])


# ============================================================
# CALLBACK QUERY HANDLERS
# ============================================================

@router.callback_query(F.data.startswith("toggle_"))
async def callback_toggle(callback: CallbackQuery) -> None:
    if not callback.from_user or not callback.message:
        await callback.answer()
        return

    user_id = callback.from_user.id
    field = callback.data.replace("toggle_", "")  # type: ignore[union-attr]

    loop = asyncio.get_event_loop()
    settings = await loop.run_in_executor(None, db_get_settings, user_id)

    current_val = int(settings.get(field, 0))
    new_val = 1 - current_val  # toggle

    await loop.run_in_executor(None, db_update_setting, user_id, field, new_val)

    # Reload and rebuild keyboard
    settings = await get_user_settings(user_id, loop)

    try:
        await callback.message.edit_reply_markup(  # type: ignore[union-attr]
            reply_markup=make_settings_keyboard(settings)
        )
    except Exception:
        pass

    status = "✅ Enabled" if new_val else "❌ Disabled"
    friendly = field.replace("_", " ").title()
    await callback.answer(f"{friendly}: {status}")


@router.callback_query(F.data == "my_stats")
async def callback_my_stats(callback: CallbackQuery) -> None:
    if not callback.from_user:
        await callback.answer()
        return

    user_id = callback.from_user.id
    loop = asyncio.get_event_loop()
    user_stats = await loop.run_in_executor(None, db_get_user_stats, user_id)

    total = sum([
        user_stats.get("messages_cleaned", 0),
        user_stats.get("photos_cleaned", 0),
        user_stats.get("videos_cleaned", 0),
        user_stats.get("documents_cleaned", 0),
        user_stats.get("audios_cleaned", 0),
        user_stats.get("voices_cleaned", 0),
        user_stats.get("animations_cleaned", 0),
    ])

    await callback.answer(
        f"Total cleaned: {total} items | Errors: {user_stats.get('errors', 0)}",
        show_alert=True,
    )


@router.callback_query(F.data == "reset_settings")
async def callback_reset(callback: CallbackQuery) -> None:
    if not callback.from_user or not callback.message:
        await callback.answer()
        return

    user_id = callback.from_user.id
    loop = asyncio.get_event_loop()

    def do_reset(uid: int) -> None:
        conn = get_db_connection()
        try:
            conn.execute("""
                UPDATE settings SET
                    remove_links = 1, remove_usernames = 0, remove_hashtags = 0,
                    remove_emails = 0, remove_phones = 0,
                    custom_header = NULL, custom_footer = NULL,
                    language = 'en', remember_enabled = 0
                WHERE telegram_id = ?
            """, (uid,))
            conn.execute("DELETE FROM word_replacements WHERE telegram_id = ?", (uid,))
            conn.commit()
        finally:
            conn.close()

    await loop.run_in_executor(None, do_reset, user_id)
    settings = await get_user_settings(user_id, loop)

    try:
        await callback.message.edit_reply_markup(  # type: ignore[union-attr]
            reply_markup=make_settings_keyboard(settings)
        )
    except Exception:
        pass

    await callback.answer("🔄 Settings reset to defaults!", show_alert=True)


@router.callback_query(F.data == "close_menu")
async def callback_close(callback: CallbackQuery) -> None:
    if callback.message:
        try:
            await callback.message.delete()  # type: ignore[union-attr]
        except Exception:
            pass
    await callback.answer()


@router.callback_query(F.data == "admin_users")
async def callback_admin_users(callback: CallbackQuery) -> None:
    if not callback.from_user or not is_admin(callback.from_user.id):
        await callback.answer("⛔ Access denied.", show_alert=True)
        return

    loop = asyncio.get_event_loop()
    users = await loop.run_in_executor(None, db_get_recent_users, 10)
    lines = [f"👥 <b>Recent 10 Users</b>\n"]
    for u in users:
        name = u.get("first_name") or "Unknown"
        uname = f"@{u['username']}" if u.get("username") else "—"
        lines.append(f"• {name} ({uname}) — <code>{u['telegram_id']}</code>")

    await callback.answer()
    if callback.message:
        await callback.message.answer(  # type: ignore[union-attr]
            "\n".join(lines), parse_mode=ParseMode.HTML
        )


@router.callback_query(F.data == "admin_stats")
async def callback_admin_stats(callback: CallbackQuery) -> None:
    if not callback.from_user or not is_admin(callback.from_user.id):
        await callback.answer("⛔ Access denied.", show_alert=True)
        return

    loop = asyncio.get_event_loop()
    g = await loop.run_in_executor(None, db_get_global_stats)
    today = await loop.run_in_executor(None, db_get_today_users)

    text = (
        f"👥 Total Users: {g.get('total_users', 0)}\n"
        f"📅 Today Active: {today}\n"
        f"💬 Messages: {g.get('total_messages', 0)}\n"
        f"🖼️ Photos: {g.get('total_photos', 0)}\n"
        f"🎬 Videos: {g.get('total_videos', 0)}\n"
        f"⚠️ Errors: {g.get('total_errors', 0)}"
    )
    await callback.answer(text, show_alert=True)


@router.callback_query(F.data == "admin_broadcast_info")
async def callback_broadcast_info(callback: CallbackQuery) -> None:
    if not callback.from_user or not is_admin(callback.from_user.id):
        await callback.answer("⛔ Access denied.", show_alert=True)
        return
    await callback.answer(
        "Use /broadcast <message> to send to all users.", show_alert=True
    )


@router.callback_query(F.data == "admin_logs")
async def callback_admin_logs(callback: CallbackQuery) -> None:
    if not callback.from_user or not is_admin(callback.from_user.id):
        await callback.answer("⛔ Access denied.", show_alert=True)
        return
    await callback.answer("Use /logs to view recent log lines.", show_alert=True)


# ============================================================
# MESSAGE CLEANERS
# ============================================================

async def process_and_reply(
    message: Message,
    media_type: str,
    loop: asyncio.AbstractEventLoop,
) -> None:
    """Central processor for all message types."""
    user_id = message.from_user.id if message.from_user else 0

    # Rate limit check
    if not _rate_limiter.is_allowed(user_id):
        await message.reply(
            "⏳ <b>Slow down!</b> You're sending too many messages. Please wait a moment.",
            parse_mode=ParseMode.HTML,
        )
        return

    start_time = time.monotonic()
    settings = await get_user_settings(user_id, loop)

    try:
        await _dispatch_message(message, media_type, settings)
        processing_ms = int((time.monotonic() - start_time) * 1000)
        await loop.run_in_executor(
            None, db_record_stat, user_id, media_type, processing_ms
        )
    except TelegramRetryAfter as e:
        logger.warning("FloodWait: retry after %ds", e.retry_after)
        await asyncio.sleep(e.retry_after)
        try:
            await _dispatch_message(message, media_type, settings)
        except Exception as exc:
            logger.error("Retry failed: %s", exc)
            await loop.run_in_executor(None, db_record_error, user_id)
    except Exception as exc:
        logger.error("Error processing %s: %s\n%s", media_type, exc, traceback.format_exc())
        await loop.run_in_executor(None, db_record_error, user_id)
        await message.reply(
            "❌ <b>An error occurred.</b> Please try again.",
            parse_mode=ParseMode.HTML,
        )


async def _dispatch_message(
    message: Message,
    media_type: str,
    settings: dict[str, Any],
) -> None:
    """Route to the correct sender based on media type."""
    if media_type == "message":
        await _send_text(message, settings)
    elif media_type == "photo":
        await _send_photo(message, settings)
    elif media_type == "video":
        await _send_video(message, settings)
    elif media_type == "document":
        await _send_document(message, settings)
    elif media_type == "audio":
        await _send_audio(message, settings)
    elif media_type == "voice":
        await _send_voice(message, settings)
    elif media_type == "animation":
        await _send_animation(message, settings)
    elif media_type == "sticker":
        await _send_sticker(message, settings)


async def _send_text(message: Message, settings: dict[str, Any]) -> None:
    text = message.text or ""
    entities = message.entities

    if not text:
        return

    cleaned_text, cleaned_entities = clean_entities(text, entities, settings)

    if not cleaned_text:
        await message.reply("✅ <i>Message had only links — nothing left to send.</i>", parse_mode=ParseMode.HTML)
        return

    await message.reply(
        cleaned_text,
        entities=cleaned_entities,
        parse_mode=None,  # use entities directly
    )


async def _send_photo(message: Message, settings: dict[str, Any]) -> None:
    photo = message.photo[-1] if message.photo else None
    if not photo:
        return

    caption = message.caption or ""
    caption_entities = message.caption_entities

    cleaned_caption, cleaned_entities = clean_entities(caption, caption_entities, settings)

    await message.reply_photo(
        photo=photo.file_id,
        caption=cleaned_caption if cleaned_caption else None,
        caption_entities=cleaned_entities,
        parse_mode=None,
    )


async def _send_video(message: Message, settings: dict[str, Any]) -> None:
    video = message.video
    if not video:
        return

    caption = message.caption or ""
    caption_entities = message.caption_entities
    cleaned_caption, cleaned_entities = clean_entities(caption, caption_entities, settings)

    await message.reply_video(
        video=video.file_id,
        caption=cleaned_caption if cleaned_caption else None,
        caption_entities=cleaned_entities,
        parse_mode=None,
        duration=video.duration,
        width=video.width,
        height=video.height,
    )


async def _send_document(message: Message, settings: dict[str, Any]) -> None:
    document = message.document
    if not document:
        return

    caption = message.caption or ""
    caption_entities = message.caption_entities
    cleaned_caption, cleaned_entities = clean_entities(caption, caption_entities, settings)

    await message.reply_document(
        document=document.file_id,
        caption=cleaned_caption if cleaned_caption else None,
        caption_entities=cleaned_entities,
        parse_mode=None,
    )


async def _send_audio(message: Message, settings: dict[str, Any]) -> None:
    audio = message.audio
    if not audio:
        return

    caption = message.caption or ""
    caption_entities = message.caption_entities
    cleaned_caption, cleaned_entities = clean_entities(caption, caption_entities, settings)

    await message.reply_audio(
        audio=audio.file_id,
        caption=cleaned_caption if cleaned_caption else None,
        caption_entities=cleaned_entities,
        parse_mode=None,
        duration=audio.duration,
        performer=audio.performer,
        title=audio.title,
    )


async def _send_voice(message: Message, settings: dict[str, Any]) -> None:
    voice = message.voice
    if not voice:
        return
    # Voice messages don't have captions in standard Telegram
    await message.reply_voice(voice=voice.file_id, duration=voice.duration)


async def _send_animation(message: Message, settings: dict[str, Any]) -> None:
    animation = message.animation
    if not animation:
        return

    caption = message.caption or ""
    caption_entities = message.caption_entities
    cleaned_caption, cleaned_entities = clean_entities(caption, caption_entities, settings)

    await message.reply_animation(
        animation=animation.file_id,
        caption=cleaned_caption if cleaned_caption else None,
        caption_entities=cleaned_entities,
        parse_mode=None,
        duration=animation.duration,
        width=animation.width,
        height=animation.height,
    )


async def _send_sticker(message: Message, settings: dict[str, Any]) -> None:
    sticker = message.sticker
    if not sticker:
        return
    await message.reply_sticker(sticker=sticker.file_id)


# ============================================================
# MEDIA GROUP HANDLER
# ============================================================

# We buffer media groups for a short time, then process them together
_media_group_buffer: dict[str, list[Message]] = {}
_media_group_tasks: dict[str, asyncio.Task] = {}  # type: ignore[type-arg]


async def _process_media_group(media_group_id: str, bot: Bot) -> None:
    """Wait briefly to collect all messages in a group, then process."""
    await asyncio.sleep(0.8)  # collect window

    messages = _media_group_buffer.pop(media_group_id, [])
    _media_group_tasks.pop(media_group_id, None)

    if not messages:
        return

    messages.sort(key=lambda m: m.message_id)
    first = messages[0]
    user_id = first.from_user.id if first.from_user else 0
    loop = asyncio.get_event_loop()
    settings = await get_user_settings(user_id, loop)

    media_list = []

    for i, msg in enumerate(messages):
        caption = msg.caption or ""
        caption_entities = msg.caption_entities
        cleaned_caption, cleaned_entities = clean_entities(
            caption, caption_entities, settings
        )

        # Only first item gets caption in album (Telegram UI shows it for the group)
        show_caption = cleaned_caption if cleaned_caption else None
        show_entities = cleaned_entities if i == 0 else None
        cap_for_item = show_caption if i == 0 else None
        ent_for_item = show_entities if i == 0 else None

        if msg.photo:
            photo = msg.photo[-1]
            media_list.append(InputMediaPhoto(
                media=photo.file_id,
                caption=cap_for_item,
                caption_entities=ent_for_item,
                parse_mode=None,
            ))
        elif msg.video:
            media_list.append(InputMediaVideo(
                media=msg.video.file_id,
                caption=cap_for_item,
                caption_entities=ent_for_item,
                parse_mode=None,
                duration=msg.video.duration,
                width=msg.video.width,
                height=msg.video.height,
            ))
        elif msg.document:
            media_list.append(InputMediaDocument(
                media=msg.document.file_id,
                caption=cap_for_item,
                caption_entities=ent_for_item,
                parse_mode=None,
            ))
        elif msg.audio:
            media_list.append(InputMediaAudio(
                media=msg.audio.file_id,
                caption=cap_for_item,
                caption_entities=ent_for_item,
                parse_mode=None,
                duration=msg.audio.duration,
                performer=msg.audio.performer,
                title=msg.audio.title,
            ))

    if not media_list:
        return

    start_time = time.monotonic()
    try:
        await bot.send_media_group(
            chat_id=first.chat.id,
            media=media_list,
            reply_to_message_id=first.message_id,
        )
        processing_ms = int((time.monotonic() - start_time) * 1000)
        await loop.run_in_executor(
            None, db_record_stat, user_id, "photo", processing_ms
        )
    except TelegramRetryAfter as e:
        await asyncio.sleep(e.retry_after)
        try:
            await bot.send_media_group(
                chat_id=first.chat.id,
                media=media_list,
                reply_to_message_id=first.message_id,
            )
        except Exception as exc:
            logger.error("Media group retry failed: %s", exc)
    except Exception as exc:
        logger.error("Media group send failed: %s", exc)
        await loop.run_in_executor(None, db_record_error, user_id)


# ============================================================
# MESSAGE HANDLERS (ROUTING)
# ============================================================

@router.message(F.media_group_id)
async def handle_media_group(message: Message) -> None:
    """Buffer media group messages and process together."""
    await ensure_user(message, asyncio.get_event_loop())

    if not message.from_user:
        return

    user_id = message.from_user.id
    if not _rate_limiter.is_allowed(user_id):
        return  # silently skip extra messages in group if rate limited

    group_id = message.media_group_id  # type: ignore[assignment]

    if group_id not in _media_group_buffer:
        _media_group_buffer[group_id] = []

    _media_group_buffer[group_id].append(message)

    # Cancel existing task and restart (so we wait for all messages)
    if group_id in _media_group_tasks:
        _media_group_tasks[group_id].cancel()

    task = asyncio.create_task(
        _process_media_group(group_id, message.bot)  # type: ignore[arg-type]
    )
    _media_group_tasks[group_id] = task


@router.message(F.text & ~F.media_group_id)
async def handle_text(message: Message) -> None:
    if not message.from_user:
        return
    # Skip commands
    if message.text and message.text.startswith("/"):
        return
    await ensure_user(message, asyncio.get_event_loop())
    await process_and_reply(message, "message", asyncio.get_event_loop())


@router.message(F.photo & ~F.media_group_id)
async def handle_photo(message: Message) -> None:
    if not message.from_user:
        return
    await ensure_user(message, asyncio.get_event_loop())
    await process_and_reply(message, "photo", asyncio.get_event_loop())


@router.message(F.video & ~F.media_group_id)
async def handle_video(message: Message) -> None:
    if not message.from_user:
        return
    await ensure_user(message, asyncio.get_event_loop())
    await process_and_reply(message, "video", asyncio.get_event_loop())


@router.message(F.document & ~F.media_group_id)
async def handle_document(message: Message) -> None:
    if not message.from_user:
        return
    await ensure_user(message, asyncio.get_event_loop())
    await process_and_reply(message, "document", asyncio.get_event_loop())


@router.message(F.audio & ~F.media_group_id)
async def handle_audio(message: Message) -> None:
    if not message.from_user:
        return
    await ensure_user(message, asyncio.get_event_loop())
    await process_and_reply(message, "audio", asyncio.get_event_loop())


@router.message(F.voice)
async def handle_voice(message: Message) -> None:
    if not message.from_user:
        return
    await ensure_user(message, asyncio.get_event_loop())
    await process_and_reply(message, "voice", asyncio.get_event_loop())


@router.message(F.animation & ~F.media_group_id)
async def handle_animation(message: Message) -> None:
    if not message.from_user:
        return
    await ensure_user(message, asyncio.get_event_loop())
    await process_and_reply(message, "animation", asyncio.get_event_loop())


@router.message(F.sticker)
async def handle_sticker(message: Message) -> None:
    if not message.from_user:
        return
    await ensure_user(message, asyncio.get_event_loop())
    await process_and_reply(message, "sticker", asyncio.get_event_loop())


# Ignore polls, contacts, locations silently
@router.message(F.poll | F.contact | F.location)
async def handle_ignored(message: Message) -> None:
    pass


# ============================================================
# BOT STARTUP / SHUTDOWN
# ============================================================

async def set_bot_commands(bot: Bot) -> None:
    commands = [
        BotCommand(command="start",    description="Start the bot"),
        BotCommand(command="help",     description="Help guide"),
        BotCommand(command="about",    description="About this bot"),
        BotCommand(command="settings", description="Configure settings"),
        BotCommand(command="stats",    description="Your statistics"),
        BotCommand(command="ping",     description="Check bot latency"),
        BotCommand(command="remember", description="Enable persistent settings"),
        BotCommand(command="reset",    description="Reset all settings"),
    ]
    await bot.set_my_commands(commands, scope=BotCommandScopeDefault())


async def cleanup_task() -> None:
    """Periodic maintenance: clean rate limiter buckets."""
    while True:
        await asyncio.sleep(300)  # every 5 minutes
        _rate_limiter.cleanup()
        logger.debug("Rate limiter cleanup done.")


async def main() -> None:
    """Entry point."""
    if not BOT_TOKEN:
        logger.critical("BOT_TOKEN is not set! Please set it in .env or environment.")
        return

    logger.info("=" * 60)
    logger.info("%s v%s starting up", BOT_NAME, BOT_VERSION)
    logger.info("=" * 60)

    # Initialize database
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, init_db)

    # Create bot and dispatcher
    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=None),
    )
    dp = Dispatcher()
    dp.include_router(router)

    # Set commands
    try:
        await set_bot_commands(bot)
        logger.info("Bot commands registered.")
    except Exception as exc:
        logger.warning("Could not set bot commands: %s", exc)

    # Start maintenance task
    asyncio.create_task(cleanup_task())

    logger.info("Starting long polling...")
    try:
        await dp.start_polling(
            bot,
            allowed_updates=dp.resolve_used_update_types(),
            drop_pending_updates=True,
        )
    finally:
        await bot.session.close()
        logger.info("Bot stopped.")


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
    except Exception as exc:
        logger.critical("Fatal error: %s\n%s", exc, traceback.format_exc())
        raise
