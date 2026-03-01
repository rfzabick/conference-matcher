"""One-time script to clear all photos so they get re-extracted on next refresh."""
from database import init_db, get_db

init_db()
conn = get_db()
cur = conn.cursor()
cur.execute("UPDATE attendees SET photo_data = NULL, photo_content_type = '', thumbnail_url = ''")
conn.commit()
print("Cleared all photos")
