"""Verify that DB names match the actual PDF page content.
Run on Fly: fly ssh console -C "python3 verify_mapping.py"
"""
import os
import io
import fitz
import httpx
from database import init_db, get_db
from psycopg2.extras import RealDictCursor

PRESENTATION_ID = os.environ.get("PRESENTATION_ID", "")
PDF_EXPORT_URL = f"https://docs.google.com/presentation/d/{PRESENTATION_ID}/export/pdf"

init_db()

# Download fresh PDF
print("Downloading PDF...")
resp = httpx.get(PDF_EXPORT_URL, follow_redirects=True, timeout=120)
pdf_bytes = resp.content
print(f"PDF size: {len(pdf_bytes)} bytes")

doc = fitz.open(stream=pdf_bytes, filetype="pdf")
print(f"Total pages: {len(doc)}")

# Get all attendees from DB
conn = get_db()
cur = conn.cursor(cursor_factory=RealDictCursor)
cur.execute("SELECT slide_object_id, name, thumbnail_url FROM attendees WHERE name != '' ORDER BY slide_object_id")
rows = cur.fetchall()
print(f"DB attendees with names: {len(rows)}")

# Compare
mismatches = 0
for row in rows:
    slide_id = row["slide_object_id"]
    db_name = row["name"]
    page_num = int(slide_id.split("_")[1])

    if page_num >= len(doc):
        print(f"  {slide_id}: DB name='{db_name}' — PAGE OUT OF RANGE (only {len(doc)} pages)")
        mismatches += 1
        continue

    page = doc[page_num]
    page_text = page.get_text()[:500].replace('\n', ' ').strip()

    # Check if the DB name appears in the page text
    name_parts = db_name.lower().split()
    found = all(part in page_text.lower() for part in name_parts if len(part) > 2)

    if not found:
        # Try first + last name only
        if len(name_parts) >= 2:
            found = name_parts[0] in page_text.lower() and name_parts[-1] in page_text.lower()

    status = "OK" if found else "MISMATCH"
    if not found:
        mismatches += 1
        print(f"  {slide_id}: DB name='{db_name}' — {status}")
        print(f"    Page text: {page_text[:200]}")

doc.close()
print(f"\nTotal mismatches: {mismatches} / {len(rows)}")
if mismatches == 0:
    print("All DB names match current PDF pages!")
else:
    print(f"WARNING: {mismatches} attendees have names that don't match the current PDF!")
    print("This means the PDF slides have been reordered since the last name extraction.")
    print("Fix: trigger a full re-extract by clearing hashes and refreshing.")
