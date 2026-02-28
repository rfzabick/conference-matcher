import json
import time
import os
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.environ.get("DATABASE_URL", "")


def get_db():
    return psycopg2.connect(DATABASE_URL, connect_timeout=5)


def put_db(conn):
    try:
        conn.close()
    except Exception:
        pass


def init_db():
    conn = get_db()
    try:
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
    finally:
        put_db(conn)


def upsert_attendee(slide_object_id, name, stuff_i_do, stuff_i_can_share, stuff_i_need, thumbnail_url, content_hash, linkedin_url=""):
    conn = get_db()
    try:
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
                thumbnail_url=EXCLUDED.thumbnail_url,
                slide_content_hash=EXCLUDED.slide_content_hash,
                updated_at=EXCLUDED.updated_at
        """, (slide_object_id, name, stuff_i_do, stuff_i_can_share, stuff_i_need, linkedin_url, thumbnail_url, content_hash, now, now))
        conn.commit()
    finally:
        put_db(conn)


def update_attendee_thumbnail(slide_object_id, thumbnail_url):
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE attendees SET thumbnail_url = %s, updated_at = %s WHERE slide_object_id = %s",
            (thumbnail_url, time.time(), slide_object_id)
        )
        conn.commit()
    finally:
        put_db(conn)


def update_attendee_photo(slide_object_id, photo_bytes, content_type):
    """Store photo binary data in the database."""
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE attendees SET photo_data = %s, photo_content_type = %s, updated_at = %s WHERE slide_object_id = %s",
            (psycopg2.Binary(photo_bytes), content_type, time.time(), slide_object_id)
        )
        conn.commit()
    finally:
        put_db(conn)


_photo_cache = {}

def get_attendee_photo(slide_object_id):
    """Retrieve photo binary data, cached in memory after first load."""
    if slide_object_id in _photo_cache:
        return _photo_cache[slide_object_id]

    conn = get_db()
    try:
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
    finally:
        put_db(conn)


def clear_photo_cache():
    """Clear the in-memory photo cache (call after re-fetching photos)."""
    _photo_cache.clear()


def get_all_attendees():
    conn = get_db()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""SELECT id, slide_object_id, name, stuff_i_do, stuff_i_can_share,
                              stuff_i_need, linkedin_url, thumbnail_url, slide_content_hash,
                              created_at, updated_at
                       FROM attendees WHERE name != '' ORDER BY name""")
        return [dict(r) for r in cur.fetchall()]
    finally:
        put_db(conn)


def get_attendee_names():
    conn = get_db()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT DISTINCT name FROM attendees WHERE name != '' ORDER BY name")
        return [r["name"] for r in cur.fetchall()]
    finally:
        put_db(conn)


def get_attendee_by_name(name):
    conn = get_db()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT id, slide_object_id, name, stuff_i_do, stuff_i_can_share, stuff_i_need, linkedin_url, thumbnail_url, slide_content_hash, created_at, updated_at FROM attendees WHERE LOWER(name) = LOWER(%s)", (name,))
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        put_db(conn)


def get_attendees_by_ids(ids):
    if not ids:
        return []
    conn = get_db()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        placeholders = ",".join(["%s"] * len(ids))
        cur.execute(f"SELECT id, slide_object_id, name, stuff_i_do, stuff_i_can_share, stuff_i_need, linkedin_url, thumbnail_url, slide_content_hash, created_at, updated_at FROM attendees WHERE id IN ({placeholders})", ids)
        return [dict(r) for r in cur.fetchall()]
    finally:
        put_db(conn)


def search_attendees(query, name_only=False):
    conn = get_db()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        pattern = f"%{query}%"
        if name_only:
            cur.execute("""
                SELECT id, slide_object_id, name, stuff_i_do, stuff_i_can_share, stuff_i_need,
                       linkedin_url, thumbnail_url, slide_content_hash, created_at, updated_at
                FROM attendees
                WHERE name != '' AND name ILIKE %s
                ORDER BY name
            """, (pattern,))
        else:
            cur.execute("""
                SELECT id, slide_object_id, name, stuff_i_do, stuff_i_can_share, stuff_i_need,
                       linkedin_url, thumbnail_url, slide_content_hash, created_at, updated_at
                FROM attendees
                WHERE name ILIKE %s OR stuff_i_do ILIKE %s OR stuff_i_can_share ILIKE %s OR stuff_i_need ILIKE %s
                ORDER BY name
            """, (pattern, pattern, pattern, pattern))
        return [dict(r) for r in cur.fetchall()]
    finally:
        put_db(conn)


def get_known_slide_ids():
    conn = get_db()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT slide_object_id, slide_content_hash FROM attendees")
        return {r["slide_object_id"]: r["slide_content_hash"] for r in cur.fetchall()}
    finally:
        put_db(conn)


def get_attendee_count():
    conn = get_db()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT COUNT(*) as cnt FROM attendees")
        row = cur.fetchone()
        return row["cnt"]
    finally:
        put_db(conn)


def get_cached_matches(user_name, ttl=86400):
    conn = get_db()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT matches_json, attendee_count, created_at FROM match_cache
            WHERE user_name = %s
            ORDER BY created_at DESC LIMIT 1
        """, (user_name,))
        row = cur.fetchone()
    finally:
        put_db(conn)

    if not row:
        return None

    if time.time() - row["created_at"] > ttl:
        return None

    current_count = get_attendee_count()
    if current_count != row["attendee_count"]:
        return None

    return json.loads(row["matches_json"])


def get_all_cached_matches():
    """Return all match cache entries (latest per user) for building the social graph."""
    conn = get_db()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT mc.user_name, mc.matches_json
            FROM match_cache mc
            INNER JOIN (
                SELECT user_name, MAX(created_at) as max_created
                FROM match_cache
                GROUP BY user_name
            ) latest ON mc.user_name = latest.user_name AND mc.created_at = latest.max_created
        """)
        results = []
        for row in cur.fetchall():
            results.append({
                "user_name": row["user_name"],
                "matches": json.loads(row["matches_json"])
            })
        return results
    finally:
        put_db(conn)


def clear_match_cache():
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM match_cache")
        conn.commit()
    finally:
        put_db(conn)


def set_cached_matches(user_name, matches):
    conn = get_db()
    try:
        now = time.time()
        count = get_attendee_count()
        cur = conn.cursor()
        cur.execute("DELETE FROM match_cache WHERE user_name = %s", (user_name,))
        cur.execute("""
            INSERT INTO match_cache (user_name, matches_json, attendee_count, created_at)
            VALUES (%s, %s, %s, %s)
        """, (user_name, json.dumps(matches), count, now))
        conn.commit()
    finally:
        put_db(conn)
