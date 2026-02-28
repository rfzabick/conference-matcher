import sqlite3
import json
import time
import os

DB_PATH = os.environ.get("DB_PATH", os.path.join(os.path.dirname(__file__), "conference_matcher.db"))


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS attendees (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            slide_object_id TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            stuff_i_do TEXT DEFAULT '',
            stuff_i_can_share TEXT DEFAULT '',
            stuff_i_need TEXT DEFAULT '',
            linkedin_url TEXT DEFAULT '',
            thumbnail_url TEXT DEFAULT '',
            slide_content_hash TEXT DEFAULT '',
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS match_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_name TEXT NOT NULL,
            matches_json TEXT NOT NULL,
            attendee_count INTEGER NOT NULL,
            created_at REAL NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_attendees_name ON attendees(name);
        CREATE INDEX IF NOT EXISTS idx_attendees_slide_id ON attendees(slide_object_id);
        CREATE INDEX IF NOT EXISTS idx_match_cache_user ON match_cache(user_name);
    """)
    # Migration: add linkedin_url column if missing
    cols = [row[1] for row in conn.execute("PRAGMA table_info(attendees)").fetchall()]
    if "linkedin_url" not in cols:
        conn.execute("ALTER TABLE attendees ADD COLUMN linkedin_url TEXT DEFAULT ''")
    conn.commit()
    conn.close()


def upsert_attendee(slide_object_id, name, stuff_i_do, stuff_i_can_share, stuff_i_need, thumbnail_url, content_hash, linkedin_url=""):
    conn = get_db()
    now = time.time()
    conn.execute("""
        INSERT INTO attendees (slide_object_id, name, stuff_i_do, stuff_i_can_share, stuff_i_need, linkedin_url, thumbnail_url, slide_content_hash, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(slide_object_id) DO UPDATE SET
            name=excluded.name,
            stuff_i_do=excluded.stuff_i_do,
            stuff_i_can_share=excluded.stuff_i_can_share,
            stuff_i_need=excluded.stuff_i_need,
            linkedin_url=excluded.linkedin_url,
            thumbnail_url=excluded.thumbnail_url,
            slide_content_hash=excluded.slide_content_hash,
            updated_at=excluded.updated_at
    """, (slide_object_id, name, stuff_i_do, stuff_i_can_share, stuff_i_need, linkedin_url, thumbnail_url, content_hash, now, now))
    conn.commit()
    conn.close()


def update_attendee_thumbnail(slide_object_id, thumbnail_url):
    conn = get_db()
    conn.execute(
        "UPDATE attendees SET thumbnail_url = ?, updated_at = ? WHERE slide_object_id = ?",
        (thumbnail_url, time.time(), slide_object_id)
    )
    conn.commit()
    conn.close()


def get_all_attendees():
    conn = get_db()
    rows = conn.execute("SELECT * FROM attendees WHERE name != '' ORDER BY name").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_attendee_names():
    conn = get_db()
    rows = conn.execute("SELECT DISTINCT name FROM attendees WHERE name != '' ORDER BY name").fetchall()
    conn.close()
    return [r["name"] for r in rows]


def get_attendee_by_name(name):
    conn = get_db()
    row = conn.execute("SELECT * FROM attendees WHERE LOWER(name) = LOWER(?)", (name,)).fetchone()
    conn.close()
    return dict(row) if row else None


def get_attendees_by_ids(ids):
    if not ids:
        return []
    conn = get_db()
    placeholders = ",".join("?" * len(ids))
    rows = conn.execute(f"SELECT * FROM attendees WHERE id IN ({placeholders})", ids).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def search_attendees(query, name_only=False):
    conn = get_db()
    pattern = f"%{query}%"
    if name_only:
        rows = conn.execute("""
            SELECT * FROM attendees
            WHERE name != '' AND name LIKE ?
            ORDER BY name
        """, (pattern,)).fetchall()
    else:
        rows = conn.execute("""
            SELECT * FROM attendees
            WHERE name LIKE ? OR stuff_i_do LIKE ? OR stuff_i_can_share LIKE ? OR stuff_i_need LIKE ?
            ORDER BY name
        """, (pattern, pattern, pattern, pattern)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_known_slide_ids():
    conn = get_db()
    rows = conn.execute("SELECT slide_object_id, slide_content_hash FROM attendees").fetchall()
    conn.close()
    return {r["slide_object_id"]: r["slide_content_hash"] for r in rows}


def get_attendee_count():
    conn = get_db()
    row = conn.execute("SELECT COUNT(*) as cnt FROM attendees").fetchone()
    conn.close()
    return row["cnt"]


def get_cached_matches(user_name, ttl=3600):
    conn = get_db()
    row = conn.execute("""
        SELECT matches_json, attendee_count, created_at FROM match_cache
        WHERE user_name = ?
        ORDER BY created_at DESC LIMIT 1
    """, (user_name,)).fetchone()
    conn.close()

    if not row:
        return None

    # Check TTL
    if time.time() - row["created_at"] > ttl:
        return None

    # Invalidate if attendee count changed (new people added)
    current_count = get_attendee_count()
    if current_count != row["attendee_count"]:
        return None

    return json.loads(row["matches_json"])


def get_all_cached_matches():
    """Return all match cache entries (latest per user) for building the social graph."""
    conn = get_db()
    rows = conn.execute("""
        SELECT mc.user_name, mc.matches_json
        FROM match_cache mc
        INNER JOIN (
            SELECT user_name, MAX(created_at) as max_created
            FROM match_cache
            GROUP BY user_name
        ) latest ON mc.user_name = latest.user_name AND mc.created_at = latest.max_created
    """).fetchall()
    conn.close()
    results = []
    for row in rows:
        results.append({
            "user_name": row["user_name"],
            "matches": json.loads(row["matches_json"])
        })
    return results


def clear_match_cache():
    conn = get_db()
    conn.execute("DELETE FROM match_cache")
    conn.commit()
    conn.close()


def set_cached_matches(user_name, matches):
    conn = get_db()
    now = time.time()
    count = get_attendee_count()
    # Remove old cache for this user
    conn.execute("DELETE FROM match_cache WHERE user_name = ?", (user_name,))
    conn.execute("""
        INSERT INTO match_cache (user_name, matches_json, attendee_count, created_at)
        VALUES (?, ?, ?, ?)
    """, (user_name, json.dumps(matches), count, now))
    conn.commit()
    conn.close()
