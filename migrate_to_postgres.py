#!/usr/bin/env python3
"""One-time migration from SQLite + photo files to Fly Postgres.

Usage (local, with DATABASE_URL set):
    DATABASE_URL=postgres://... python migrate_to_postgres.py

Usage (on Fly, where DATABASE_URL is already a secret):
    fly ssh console -C "python migrate_to_postgres.py"
"""
import os
import sqlite3
import json
import psycopg2
from psycopg2.extras import execute_values

SQLITE_PATH = os.environ.get("SQLITE_PATH", os.path.join(os.path.dirname(__file__), "conference_matcher.db"))
PHOTOS_DIR = os.environ.get("PHOTOS_DIR", os.path.join(os.path.dirname(__file__), "photos"))
DATABASE_URL = os.environ.get("DATABASE_URL", "")


def migrate():
    if not DATABASE_URL:
        print("ERROR: DATABASE_URL not set")
        return

    if not os.path.exists(SQLITE_PATH):
        print(f"ERROR: SQLite database not found at {SQLITE_PATH}")
        return

    # Connect to SQLite
    sqlite_conn = sqlite3.connect(SQLITE_PATH)
    sqlite_conn.row_factory = sqlite3.Row

    # Connect to Postgres
    pg_conn = psycopg2.connect(DATABASE_URL)
    pg_cur = pg_conn.cursor()

    # Create tables in Postgres
    pg_cur.execute("""
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
    pg_conn.commit()

    # Migrate attendees
    attendees = sqlite_conn.execute("SELECT * FROM attendees").fetchall()
    print(f"Migrating {len(attendees)} attendees...")

    ext_to_mime = {
        "png": "image/png",
        "jpeg": "image/jpeg",
        "jpg": "image/jpeg",
        "jfif": "image/jpeg",
        "webp": "image/webp",
        "gif": "image/gif",
    }

    migrated = 0
    photos_loaded = 0
    for row in attendees:
        row = dict(row)
        slide_id = row["slide_object_id"]

        # Try to load photo from disk
        photo_data = None
        photo_content_type = ""
        if os.path.isdir(PHOTOS_DIR):
            for fname in os.listdir(PHOTOS_DIR):
                if fname.startswith(f"{slide_id}."):
                    photo_path = os.path.join(PHOTOS_DIR, fname)
                    ext = fname.rsplit(".", 1)[-1].lower()
                    photo_content_type = ext_to_mime.get(ext, f"image/{ext}")
                    with open(photo_path, "rb") as f:
                        photo_data = f.read()
                    photos_loaded += 1
                    break

        pg_cur.execute("""
            INSERT INTO attendees (slide_object_id, name, stuff_i_do, stuff_i_can_share, stuff_i_need,
                                   linkedin_url, thumbnail_url, slide_content_hash,
                                   photo_data, photo_content_type, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (slide_object_id) DO UPDATE SET
                name = EXCLUDED.name,
                stuff_i_do = EXCLUDED.stuff_i_do,
                stuff_i_can_share = EXCLUDED.stuff_i_can_share,
                stuff_i_need = EXCLUDED.stuff_i_need,
                linkedin_url = EXCLUDED.linkedin_url,
                thumbnail_url = EXCLUDED.thumbnail_url,
                slide_content_hash = EXCLUDED.slide_content_hash,
                photo_data = EXCLUDED.photo_data,
                photo_content_type = EXCLUDED.photo_content_type,
                updated_at = EXCLUDED.updated_at
        """, (
            slide_id,
            row.get("name", ""),
            row.get("stuff_i_do", ""),
            row.get("stuff_i_can_share", ""),
            row.get("stuff_i_need", ""),
            row.get("linkedin_url", ""),
            row.get("thumbnail_url", ""),
            row.get("slide_content_hash", ""),
            psycopg2.Binary(photo_data) if photo_data else None,
            photo_content_type,
            row["created_at"],
            row["updated_at"],
        ))
        migrated += 1

    pg_conn.commit()
    print(f"Migrated {migrated} attendees, loaded {photos_loaded} photos")

    # Migrate match_cache
    matches = sqlite_conn.execute("SELECT * FROM match_cache").fetchall()
    print(f"Migrating {len(matches)} match cache entries...")

    for row in matches:
        row = dict(row)
        pg_cur.execute("""
            INSERT INTO match_cache (user_name, matches_json, attendee_count, created_at)
            VALUES (%s, %s, %s, %s)
        """, (
            row["user_name"],
            row["matches_json"],
            row["attendee_count"],
            row["created_at"],
        ))

    pg_conn.commit()
    print(f"Migrated {len(matches)} match cache entries")

    # Reset the serial sequence to be after the max id
    pg_cur.execute("SELECT MAX(id) FROM attendees")
    max_id = pg_cur.fetchone()[0]
    if max_id:
        pg_cur.execute(f"SELECT setval('attendees_id_seq', {max_id})")

    pg_cur.execute("SELECT MAX(id) FROM match_cache")
    max_id = pg_cur.fetchone()[0]
    if max_id:
        pg_cur.execute(f"SELECT setval('match_cache_id_seq', {max_id})")

    pg_conn.commit()

    sqlite_conn.close()
    pg_cur.close()
    pg_conn.close()
    print("Migration complete!")


if __name__ == "__main__":
    migrate()
