import os
import io
import json
import hashlib
import base64
import logging
import httpx
import anthropic
import fitz  # PyMuPDF
from pypdf import PdfReader, PdfWriter
from database import get_known_slide_ids, upsert_attendee, clear_match_cache, update_attendee_thumbnail, update_attendee_photo

logger = logging.getLogger(__name__)

PRESENTATION_ID = os.environ.get("PRESENTATION_ID", "")

PDF_EXPORT_URL = f"https://docs.google.com/presentation/d/{PRESENTATION_ID}/export/pdf"

DATA_DIR = os.environ.get("DATA_DIR", os.path.dirname(__file__))


def download_presentation_pdf():
    """Download the public presentation as a PDF."""
    response = httpx.get(PDF_EXPORT_URL, follow_redirects=True, timeout=60)
    response.raise_for_status()
    return response.content


def split_pdf_pages(pdf_bytes):
    """Split a PDF into individual page bytes. Returns list of (page_num, page_bytes, content_hash)."""
    reader = PdfReader(io.BytesIO(pdf_bytes))
    pages = []
    for i, page in enumerate(reader.pages):
        writer = PdfWriter()
        writer.add_page(page)
        buf = io.BytesIO()
        writer.write(buf)
        page_bytes = buf.getvalue()
        content_hash = hashlib.md5(page_bytes).hexdigest()
        pages.append((i, page_bytes, content_hash))
    return pages


def extract_attendee_data_from_pdf_page(page_bytes):
    """Use Claude to extract structured attendee data from a single-page PDF."""
    client = anthropic.Anthropic()

    page_b64 = base64.standard_b64encode(page_bytes).decode("utf-8")

    prompt = """Look at this conference slide and extract the following information about the attendee.
The slide is from a shared deck where each person fills out one slide about themselves.
Each slide has a name and three sections: "Stuff I do", "Stuff I can share/help with", and "Stuff I need".

Extract:
- name: The person's full name
- stuff_i_do: What they do (their work, roles, projects, hobbies)
- stuff_i_can_share: What they can share or help others with (skills, knowledge, resources)
- stuff_i_need: What they need or are looking for (help, connections, resources, advice)
- linkedin_url: Their LinkedIn profile URL if visible on the slide (look for linkedin.com links or LinkedIn icons with URLs)

Respond ONLY with valid JSON in this exact format:
{"name": "...", "stuff_i_do": "...", "stuff_i_can_share": "...", "stuff_i_need": "...", "linkedin_url": "..."}

If this slide does not appear to be about a specific person (e.g. it's a title slide, agenda, or instructions), respond with:
{"name": "", "stuff_i_do": "", "stuff_i_can_share": "", "stuff_i_need": "", "linkedin_url": ""}

If you cannot determine a field, use an empty string."""

    response = client.messages.create(
        model="claude-haiku-4-5",
        max_tokens=1024,
        messages=[{
            "role": "user",
            "content": [
                {
                    "type": "document",
                    "source": {
                        "type": "base64",
                        "media_type": "application/pdf",
                        "data": page_b64
                    }
                },
                {"type": "text", "text": prompt}
            ]
        }]
    )

    text = response.content[0].text.strip()
    try:
        if "```" in text:
            text = text.split("```json")[-1].split("```")[0].strip()
            if not text:
                text = response.content[0].text.split("```")[1].split("```")[0].strip()
        return json.loads(text)
    except (json.JSONDecodeError, IndexError):
        logger.warning(f"Failed to parse Claude response: {text[:200]}")
        return None


def debug_slide_images(page_num):
    """Return info about all images on a specific slide."""
    resp = httpx.get(PDF_EXPORT_URL, follow_redirects=True, timeout=120)
    resp.raise_for_status()
    doc = fitz.open(stream=resp.content, filetype="pdf")
    page = doc[page_num]
    page_area = page.rect.width * page.rect.height
    images = page.get_images(full=True)
    result = []
    for img_info in images:
        xref = img_info[0]
        try:
            img_data = doc.extract_image(xref)
            if not img_data:
                continue
            w = img_data.get("width", 0)
            h = img_data.get("height", 0)
            ext = img_data.get("ext", "?")
            size = len(img_data.get("image", b""))
            bpp = size / max(w * h, 1)

            # Get rendered dimensions
            rects = page.get_image_rects(img_info)
            rendered = None
            coverage = 0
            if rects:
                rect = rects[0]
                rendered = {"width": round(rect.width, 1), "height": round(rect.height, 1),
                            "aspect": round(max(rect.width, rect.height) / max(min(rect.width, rect.height), 1), 2)}
                coverage = round((rect.width * rect.height) / max(page_area, 1), 3)

            result.append({
                "xref": xref,
                "native_width": w,
                "native_height": h,
                "rendered": rendered,
                "coverage": coverage,
                "ext": ext,
                "file_size": size,
                "bytes_per_pixel": round(bpp, 3),
            })
        except Exception as e:
            result.append({"xref": xref, "error": str(e)})
    page_size = {"width": round(page.rect.width, 1), "height": round(page.rect.height, 1)}
    doc.close()
    return {"page": page_num, "page_size": page_size,
            "image_count": len(result), "images": result}


def fetch_profile_photos(pdf_bytes=None):
    """Extract profile photos from PDF pages and store in database."""
    if not PRESENTATION_ID:
        logger.warning("Missing PRESENTATION_ID. Skipping photo fetch.")
        return {"status": "skipped", "reason": "missing PRESENTATION_ID"}

    # Download PDF if not provided
    if pdf_bytes is None:
        logger.info("Downloading PDF for photo extraction...")
        try:
            resp = httpx.get(PDF_EXPORT_URL, follow_redirects=True, timeout=120)
            resp.raise_for_status()
            pdf_bytes = resp.content
            logger.info(f"Downloaded PDF: {len(pdf_bytes)} bytes")
        except Exception as e:
            logger.error(f"Failed to download PDF: {e}")
            return {"status": "error", "reason": str(e)}

    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    except Exception as e:
        logger.error(f"Failed to open PDF: {e}")
        return {"status": "error", "reason": str(e)}

    # Check which slides already have photos in the DB
    from database import get_db, put_db
    from psycopg2.extras import RealDictCursor
    conn = get_db()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT slide_object_id FROM attendees WHERE photo_data IS NOT NULL")
        has_photo = {r["slide_object_id"] for r in cur.fetchall()}
    finally:
        put_db(conn)

    logger.info(f"Extracting profile photos from {len(doc)} pages...")
    fetched = 0
    skipped = 0
    errors = 0

    ext_to_mime = {
        "png": "image/png",
        "jpeg": "image/jpeg",
        "jpg": "image/jpeg",
        "jfif": "image/jpeg",
        "jp2": "image/jp2",
        "webp": "image/webp",
        "gif": "image/gif",
        "bmp": "image/bmp",
        "tiff": "image/tiff",
    }

    for i in range(len(doc)):
        slide_id = f"page_{i}"
        if slide_id in has_photo:
            skipped += 1
            continue

        try:
            page = doc[i]
            images = page.get_images(full=True)
            if not images:
                skipped += 1
                continue

            page_area = page.rect.width * page.rect.height

            candidates = []
            for img_info in images:
                xref = img_info[0]
                try:
                    rects = page.get_image_rects(img_info)
                    if not rects:
                        continue
                    rect = rects[0]
                    rendered_w = rect.width
                    rendered_h = rect.height
                    rendered_area = rendered_w * rendered_h
                    coverage = rendered_area / max(page_area, 1)

                    if coverage > 0.9:
                        continue
                    if rendered_w < 30 or rendered_h < 30:
                        continue

                    aspect = max(rendered_w, rendered_h) / max(min(rendered_w, rendered_h), 1)
                    if aspect > 4:
                        continue

                    img_data = doc.extract_image(xref)
                    if not img_data:
                        continue

                    w = img_data.get("width", 0)
                    h = img_data.get("height", 0)
                    raw = img_data.get("image", b"")
                    if len(raw) < 1000 and w * h > 10000:
                        continue

                    try:
                        pix = fitz.Pixmap(raw)
                        if pix.n >= 3:
                            samples = pix.samples
                            step = max(1, len(samples) // 3000) * pix.n
                            total = 0
                            count = 0
                            for j in range(0, len(samples) - pix.n + 1, step):
                                total += samples[j] + samples[j+1] + samples[j+2]
                                count += 3
                            avg_brightness = total / max(count, 1)
                            if avg_brightness < 15:
                                continue
                        pix = None
                    except Exception:
                        pass

                    file_size = len(raw)
                    candidates.append((rendered_w, rendered_h, aspect, img_data, coverage, file_size))
                except Exception:
                    continue

            if not candidates:
                skipped += 1
                continue

            candidates.sort(key=lambda c: (c[2], -c[5]))
            img_data = candidates[0][3]
            ext = img_data.get("ext", "png")
            photo_filename = f"{slide_id}.{ext}"
            content_type = ext_to_mime.get(ext.lower(), f"image/{ext}")

            # Store photo in database
            update_attendee_photo(slide_id, img_data["image"], content_type)
            update_attendee_thumbnail(slide_id, f"/photos/{photo_filename}")
            fetched += 1
            logger.info(f"Extracted photo for slide {i}: {photo_filename} ({img_data.get('width')}x{img_data.get('height')})")
        except Exception as e:
            logger.error(f"Error extracting photo from slide {i}: {e}")
            errors += 1

    doc.close()

    result = {"status": "completed", "fetched": fetched, "skipped": skipped, "errors": errors}
    logger.info(f"Photo fetch complete: {result}")
    return result


def refresh_slides():
    """Download presentation PDF, diff against DB, and process only new/changed slides."""
    if not PRESENTATION_ID:
        logger.warning("Presentation ID not set. Skipping refresh.")
        return {"status": "skipped", "reason": "missing PRESENTATION_ID"}

    logger.info("Starting slide refresh...")

    try:
        pdf_bytes = download_presentation_pdf()
        logger.info(f"Downloaded PDF: {len(pdf_bytes)} bytes")
    except Exception as e:
        logger.error(f"Failed to download presentation PDF: {e}")
        return {"status": "error", "reason": str(e)}

    try:
        pages = split_pdf_pages(pdf_bytes)
        logger.info(f"Split into {len(pages)} pages")
    except Exception as e:
        logger.error(f"Failed to split PDF: {e}")
        return {"status": "error", "reason": str(e)}

    known = get_known_slide_ids()
    new_count = 0
    updated_count = 0
    skipped_count = 0
    error_count = 0

    for page_num, page_bytes, content_hash in pages:
        slide_id = f"page_{page_num}"

        # Skip if we already have this exact version
        if slide_id in known and known[slide_id] == content_hash:
            skipped_count += 1
            continue

        is_new = slide_id not in known

        try:
            data = extract_attendee_data_from_pdf_page(page_bytes)
            if not data or not data.get("name"):
                logger.info(f"Slide {page_num}: no attendee data (likely title/info slide)")
                skipped_count += 1
                # Still record the hash so we don't re-process it
                upsert_attendee(
                    slide_object_id=slide_id,
                    name="",
                    stuff_i_do="",
                    stuff_i_can_share="",
                    stuff_i_need="",
                    thumbnail_url="",
                    content_hash=content_hash
                )
                continue

            upsert_attendee(
                slide_object_id=slide_id,
                name=data.get("name", ""),
                stuff_i_do=data.get("stuff_i_do", ""),
                stuff_i_can_share=data.get("stuff_i_can_share", ""),
                stuff_i_need=data.get("stuff_i_need", ""),
                thumbnail_url="",
                content_hash=content_hash,
                linkedin_url=data.get("linkedin_url", "")
            )

            if is_new:
                new_count += 1
                logger.info(f"Added new attendee: {data.get('name')}")
            else:
                updated_count += 1
                logger.info(f"Updated attendee: {data.get('name')}")

        except Exception as e:
            logger.error(f"Error processing slide {page_num}: {e}")
            error_count += 1
            continue

    result = {
        "status": "completed",
        "total_slides": len(pages),
        "new": new_count,
        "updated": updated_count,
        "skipped": skipped_count,
        "errors": error_count
    }
    logger.info(f"Slide refresh complete: {result}")

    # Render slide thumbnails from the PDF we already downloaded
    fetch_profile_photos(pdf_bytes)

    # Pre-compute matches if any slides were new or updated
    if new_count > 0 or updated_count > 0:
        logger.info("New/updated slides found — clearing match cache and pre-computing matches...")
        clear_match_cache()
        from matcher import precompute_all_matches
        precompute_all_matches()

    return result
