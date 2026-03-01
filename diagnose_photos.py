"""Diagnostic script to check photo-to-name mapping and image extraction.
Run on Fly: fly ssh console -C "python3 diagnose_photos.py"
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

# Check DB names
conn = get_db()
cur = conn.cursor(cursor_factory=RealDictCursor)

print("\n=== DB Data (pages 43-49) ===")
for i in range(43, min(50, len(doc))):
    slide_id = f"page_{i}"
    cur.execute(
        "SELECT name, thumbnail_url FROM attendees WHERE slide_object_id = %s",
        (slide_id,),
    )
    row = cur.fetchone()
    if row:
        print(f"  {slide_id} (slide {i+1}): name='{row['name']}', thumb='{row['thumbnail_url']}'")
    else:
        print(f"  {slide_id} (slide {i+1}): NOT IN DB")

# Check for shared image xrefs across pages
print("\n=== Checking for shared image xrefs (all pages) ===")
xref_pages = {}  # xref -> list of page indices
for i in range(len(doc)):
    page = doc[i]
    for img_info in page.get_images(full=True):
        xref = img_info[0]
        xref_pages.setdefault(xref, []).append(i)

shared = {x: pages for x, pages in xref_pages.items() if len(pages) > 1}
if shared:
    print(f"  {len(shared)} xrefs are shared across multiple pages!")
    # Show a few examples
    for xref, pages in list(shared.items())[:5]:
        print(f"    xref={xref} appears on pages: {pages[:10]}{'...' if len(pages) > 10 else ''}")
    # Check how many unique xrefs per page for pages 43-49
    for i in range(43, min(50, len(doc))):
        page = doc[i]
        imgs = page.get_images(full=True)
        unique_xrefs = [x[0] for x in imgs]
        shared_on_page = [x for x in unique_xrefs if x in shared]
        unique_on_page = [x for x in unique_xrefs if x not in shared]
        print(f"  Page {i}: {len(imgs)} images, {len(shared_on_page)} shared, {len(unique_on_page)} unique")
else:
    print("  No shared xrefs found (each image belongs to exactly one page)")

# Image analysis for pages around Daniel
print("\n=== Image Analysis (pages 43-49) ===")
for i in range(43, min(50, len(doc))):
    page = doc[i]
    page_area = page.rect.width * page.rect.height
    images = page.get_images(full=True)

    print(f"\nPage {i} / slide {i+1} ({len(images)} images via get_images):")

    candidates = []
    for img_info in images:
        xref = img_info[0]
        rects = page.get_image_rects(img_info)
        if not rects:
            print(f"  xref={xref}: no rects (not rendered here)")
            continue
        rect = rects[0]
        coverage = (rect.width * rect.height) / max(page_area, 1)
        aspect = max(rect.width, rect.height) / max(min(rect.width, rect.height), 1)

        img_data = doc.extract_image(xref)
        if not img_data:
            print(f"  xref={xref}: extract_image returned None")
            continue

        w = img_data.get("width", 0)
        h = img_data.get("height", 0)
        file_size = len(img_data.get("image", b""))
        ext = img_data.get("ext", "?")
        is_shared = xref in shared

        print(
            f"  xref={xref}: rendered={rect.width:.0f}x{rect.height:.0f}, "
            f"native={w}x{h}, coverage={coverage:.3f}, aspect={aspect:.2f}, "
            f"size={file_size}, ext={ext}"
            f"{' [SHARED]' if is_shared else ''}"
        )

        skip_reason = None
        if coverage > 0.9:
            skip_reason = "coverage>0.9"
        elif rect.width < 30 or rect.height < 30:
            skip_reason = "too small"
        elif aspect > 4:
            skip_reason = "aspect>4"
        elif file_size < 1000 and w * h > 10000:
            skip_reason = "low bpp"

        if skip_reason:
            print(f"    -> FILTERED: {skip_reason}")
        else:
            candidates.append((rect.width, rect.height, aspect, file_size, xref, is_shared))
            print(f"    -> CANDIDATE")

    if candidates:
        candidates.sort(key=lambda c: (c[2], -c[3]))
        winner = candidates[0]
        print(f"  WINNER: xref={winner[4]}, aspect={winner[2]:.2f}, size={winner[3]}"
              f"{' [SHARED - THIS IS THE BUG]' if winner[5] else ''}")
    else:
        print(f"  NO CANDIDATES")

    # Also check get_text("dict") for image blocks
    blocks = page.get_text("dict")["blocks"]
    img_blocks = [b for b in blocks if b["type"] == 1]
    print(f"  get_text dict: {len(img_blocks)} image blocks")
    for b in img_blocks:
        bbox = b["bbox"]
        bw = bbox[2] - bbox[0]
        bh = bbox[3] - bbox[1]
        print(f"    dict-img: {bw:.0f}x{bh:.0f} at ({bbox[0]:.0f},{bbox[1]:.0f}), size={len(b.get('image', b''))}")

doc.close()
print("\nDone.")
