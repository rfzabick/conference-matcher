import json
import time
import os
import threading
import logging
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.environ.get("DATABASE_URL", "")
logger = logging.getLogger(__name__)

_thread_local = threading.local()


def get_db():
    """Get a persistent per-thread connection, reused across requests."""
    conn = getattr(_thread_local, "conn", None)
    if conn is not None and not conn.closed:
        try:
            conn.rollback()
            return conn
        except Exception:
            pass
    conn = psycopg2.connect(DATABASE_URL, connect_timeout=5)
    _thread_local.conn = conn
    return conn


def put_db(conn):
    pass


# ── In-memory caches ──────────────────────────────────────────────
# All read-heavy data is loaded once from Postgres and served from RAM.
# Writes go to Postgres AND update the in-memory cache.

_cache_lock = threading.Lock()
_attendees = None        # list of dicts (no photo_data)
_attendee_count = None   # int
_match_cache = None      # dict: user_name -> {"matches_json": str, "attendee_count": int, "created_at": float}
_photo_cache = {}        # dict: slide_object_id -> (bytes, content_type)


def _load_attendees():
    """Load all attendees from DB into memory."""
    global _attendees, _attendee_count
    conn = get_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""SELECT id, slide_object_id, name, stuff_i_do, stuff_i_can_share,
                          stuff_i_need, linkedin_url, thumbnail_url, slide_content_hash,
                          created_at, updated_at
                   FROM attendees WHERE name != '' ORDER BY name""")
    _attendees = [dict(r) for r in cur.fetchall()]
    cur.execute("SELECT COUNT(*) as cnt FROM attendees")
    _attendee_count = cur.fetchone()["cnt"]
    logger.info(f"Loaded {len(_attendees)} attendees into memory")


def _load_match_cache():
    """Load all match cache entries from DB into memory."""
    global _match_cache
    conn = get_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT mc.user_name, mc.matches_json, mc.attendee_count, mc.created_at
        FROM match_cache mc
        INNER JOIN (
            SELECT user_name, MAX(created_at) as max_created
            FROM match_cache
            GROUP BY user_name
        ) latest ON mc.user_name = latest.user_name AND mc.created_at = latest.max_created
    """)
    _match_cache = {}
    for row in cur.fetchall():
        _match_cache[row["user_name"]] = {
            "matches_json": row["matches_json"],
            "attendee_count": row["attendee_count"],
            "created_at": row["created_at"],
        }
    logger.info(f"Loaded {len(_match_cache)} match cache entries into memory")


def _ensure_caches():
    """Lazy-load caches on first access, with retries."""
    global _attendees, _match_cache
    if _attendees is None or _match_cache is None:
        with _cache_lock:
            for attempt in range(3):
                try:
                    if _attendees is None:
                        _load_attendees()
                    if _match_cache is None:
                        _load_match_cache()
                    break
                except Exception as e:
                    logger.error(f"Cache load attempt {attempt + 1} failed: {e}")
                    # Reset thread-local connection on failure
                    _thread_local.conn = None
                    if attempt < 2:
                        time.sleep(1)
                    else:
                        raise


# ── Schema / init ─────────────────────────────────────────────────

def init_db():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS attendees (
            id SERIAL PRIMARY KEY,
            slide_object_id TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            stuff_i_do TEXT DEFAULT '',
            stuff_i_can_share TEXT DEFAULT '',
            stuff_i_need TEXT DEFAULT '',
            linkedin_url TEXT DEFAULT '',
            thumbnail_url TEXT DEFAULT '',
            slide_content_hash TEXT DEFAULT '',
            photo_data BYTEA,
            photo_content_type TEXT DEFAULT '',
            created_at DOUBLE PRECISION NOT NULL,
            updated_at DOUBLE PRECISION NOT NULL
        );

        CREATE TABLE IF NOT EXISTS match_cache (
            id SERIAL PRIMARY KEY,
            user_name TEXT NOT NULL,
            matches_json TEXT NOT NULL,
            attendee_count INTEGER NOT NULL,
            created_at DOUBLE PRECISION NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_attendees_name ON attendees(name);
        CREATE INDEX IF NOT EXISTS idx_attendees_slide_id ON attendees(slide_object_id);
        CREATE INDEX IF NOT EXISTS idx_match_cache_user ON match_cache(user_name);
    """)
    conn.commit()

    # Migration: add columns if missing
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'attendees'
    """)
    cols = {row[0] for row in cur.fetchall()}
    if "linkedin_url" not in cols:
        cur.execute("ALTER TABLE attendees ADD COLUMN linkedin_url TEXT DEFAULT ''")
    if "photo_data" not in cols:
        cur.execute("ALTER TABLE attendees ADD COLUMN photo_data BYTEA")
    if "photo_content_type" not in cols:
        cur.execute("ALTER TABLE attendees ADD COLUMN photo_content_type TEXT DEFAULT ''")
    conn.commit()

    # Migration: rename photo URLs from /photos/page_N to /photos/attendee_N
    # to bust browser caches after slide reorder
    cur.execute("""
        UPDATE attendees SET thumbnail_url = REPLACE(thumbnail_url, '/photos/page_', '/photos/attendee_')
        WHERE thumbnail_url LIKE '/photos/page_%'
    """)
    conn.commit()


# ── Attendee reads (from memory) ──────────────────────────────────

def get_all_attendees():
    _ensure_caches()
    return list(_attendees)


def get_attendee_names():
    _ensure_caches()
    return sorted(set(a["name"] for a in _attendees))


def get_attendee_by_name(name):
    _ensure_caches()
    lower = name.lower()
    for a in _attendees:
        if a["name"].lower() == lower:
            return dict(a)
    return None


def get_attendees_by_ids(ids):
    _ensure_caches()
    id_set = set(ids)
    return [dict(a) for a in _attendees if a["id"] in id_set]


def get_attendees_by_names(names):
    _ensure_caches()
    name_set = {n.lower() for n in names}
    return [dict(a) for a in _attendees if a["name"].lower() in name_set]


def search_attendees(query, name_only=False):
    _ensure_caches()
    pattern = query.lower()
    results = []
    for a in _attendees:
        if name_only:
            if pattern in a["name"].lower():
                results.append(dict(a))
        else:
            if (pattern in a["name"].lower() or
                pattern in (a.get("stuff_i_do") or "").lower() or
                pattern in (a.get("stuff_i_can_share") or "").lower() or
                pattern in (a.get("stuff_i_need") or "").lower()):
                results.append(dict(a))
    return results


def get_attendee_count():
    _ensure_caches()
    return _attendee_count


def get_known_slide_ids():
    """Used by slides.py during refresh — reads from DB directly."""
    conn = get_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT slide_object_id, slide_content_hash FROM attendees")
    return {r["slide_object_id"]: r["slide_content_hash"] for r in cur.fetchall()}


# ── Attendee writes (DB + cache invalidation) ────────────────────

def upsert_attendee(slide_object_id, name, stuff_i_do, stuff_i_can_share, stuff_i_need, thumbnail_url, content_hash, linkedin_url=""):
    conn = get_db()
    now = time.time()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO attendees (slide_object_id, name, stuff_i_do, stuff_i_can_share, stuff_i_need, linkedin_url, thumbnail_url, slide_content_hash, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT(slide_object_id) DO UPDATE SET
            name=EXCLUDED.name,
            stuff_i_do=EXCLUDED.stuff_i_do,
            stuff_i_can_share=EXCLUDED.stuff_i_can_share,
            stuff_i_need=EXCLUDED.stuff_i_need,
            linkedin_url=EXCLUDED.linkedin_url,
            thumbnail_url=CASE WHEN EXCLUDED.thumbnail_url = '' THEN attendees.thumbnail_url ELSE EXCLUDED.thumbnail_url END,
            slide_content_hash=EXCLUDED.slide_content_hash,
            updated_at=EXCLUDED.updated_at
    """, (slide_object_id, name, stuff_i_do, stuff_i_can_share, stuff_i_need, linkedin_url, thumbnail_url, content_hash, now, now))
    conn.commit()
    # Invalidate attendee cache so next read reloads
    _invalidate_attendees()


def update_attendee_thumbnail(slide_object_id, thumbnail_url):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "UPDATE attendees SET thumbnail_url = %s, updated_at = %s WHERE slide_object_id = %s",
        (thumbnail_url, time.time(), slide_object_id)
    )
    conn.commit()
    _invalidate_attendees()


def _invalidate_attendees():
    global _attendees, _attendee_count
    with _cache_lock:
        _attendees = None
        _attendee_count = None


# ── Photo reads/writes (in-memory cache) ─────────────────────────

def update_attendee_photo(slide_object_id, photo_bytes, content_type):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "UPDATE attendees SET photo_data = %s, photo_content_type = %s, updated_at = %s WHERE slide_object_id = %s",
        (psycopg2.Binary(photo_bytes), content_type, time.time(), slide_object_id)
    )
    conn.commit()
    _photo_cache[slide_object_id] = (photo_bytes, content_type)


def get_attendee_photo(slide_object_id):
    if slide_object_id in _photo_cache:
        return _photo_cache[slide_object_id]

    conn = get_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        "SELECT photo_data, photo_content_type FROM attendees WHERE slide_object_id = %s",
        (slide_object_id,)
    )
    row = cur.fetchone()
    if row and row["photo_data"]:
        result = (bytes(row["photo_data"]), row["photo_content_type"])
    else:
        result = (None, None)
    _photo_cache[slide_object_id] = result
    return result


def clear_attendee_photo(slide_object_id):
    """Clear photo data for a specific attendee so it gets re-extracted."""
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "UPDATE attendees SET photo_data = NULL, photo_content_type = '', thumbnail_url = '', updated_at = %s WHERE slide_object_id = %s",
        (time.time(), slide_object_id)
    )
    conn.commit()
    _photo_cache.pop(slide_object_id, None)
    _invalidate_attendees()


def clear_photo_cache():
    _photo_cache.clear()


# ── Match cache reads (from memory) ──────────────────────────────

def get_cached_matches(user_name, ttl=86400):
    _ensure_caches()
    entry = _match_cache.get(user_name)
    if not entry:
        return None
    if time.time() - entry["created_at"] > ttl:
        return None
    if _attendee_count != entry["attendee_count"]:
        return None
    return json.loads(entry["matches_json"])


def get_all_cached_matches():
    _ensure_caches()
    results = []
    for user_name, entry in _match_cache.items():
        results.append({
            "user_name": user_name,
            "matches": json.loads(entry["matches_json"])
        })
    return results


# ── Match cache writes (DB + memory) ─────────────────────────────

def set_cached_matches(user_name, matches):
    _ensure_caches()
    conn = get_db()
    now = time.time()
    count = _attendee_count
    cur = conn.cursor()
    cur.execute("DELETE FROM match_cache WHERE user_name = %s", (user_name,))
    cur.execute("""
        INSERT INTO match_cache (user_name, matches_json, attendee_count, created_at)
        VALUES (%s, %s, %s, %s)
    """, (user_name, json.dumps(matches), count, now))
    conn.commit()
    # Update in-memory cache
    with _cache_lock:
        _match_cache[user_name] = {
            "matches_json": json.dumps(matches),
            "attendee_count": count,
            "created_at": now,
        }


def clear_match_cache():
    global _match_cache
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM match_cache")
    conn.commit()
    with _cache_lock:
        _match_cache = {}
