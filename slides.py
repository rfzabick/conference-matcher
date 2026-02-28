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
from database import get_known_slide_ids, upsert_attendee, clear_match_cache, update_attendee_thumbnail

logger = logging.getLogger(__name__)

PRESENTATION_ID = os.environ.get("PRESENTATION_ID", "")

PDF_EXPORT_URL = f"https://docs.google.com/presentation/d/{PRESENTATION_ID}/export/pdf"

DATA_DIR = os.environ.get("DATA_DIR", os.environ.get("DB_PATH", "").rsplit("/", 1)[0] or os.path.dirname(__file__))
PHOTOS_DIR = os.path.join(DATA_DIR, "photos")


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

Respond ONLY with valid JSON in this exact format:
{"name": "...", "stuff_i_do": "...", "stuff_i_can_share": "...", "stuff_i_need": "..."}

If this slide does not appear to be about a specific person (e.g. it's a title slide, agenda, or instructions), respond with:
{"name": "", "stuff_i_do": "", "stuff_i_can_share": "", "stuff_i_need": ""}

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


def fetch_profile_photos(pdf_bytes=None):
    """Render each PDF page as a thumbnail image and save locally."""
    if not PRESENTATION_ID:
        logger.warning("Missing PRESENTATION_ID. Skipping photo fetch.")
        return {"status": "skipped", "reason": "missing PRESENTATION_ID"}

    os.makedirs(PHOTOS_DIR, exist_ok=True)

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

    logger.info(f"Extracting profile photos from {len(doc)} pages...")
    fetched = 0
    skipped = 0
    errors = 0

    for i in range(len(doc)):
        slide_id = f"page_{i}"
        # Skip if we already have a photo for this slide
        existing = [f for f in os.listdir(PHOTOS_DIR) if f.startswith(f"{slide_id}.")]
        if existing:
            skipped += 1
            continue

        try:
            page = doc[i]
            images = page.get_images(full=True)
            if not images:
                skipped += 1
                continue

            # Extract all images and find the best candidate for a profile photo
            candidates = []
            for img_info in images:
                xref = img_info[0]
                try:
                    img_data = doc.extract_image(xref)
                    if not img_data:
                        continue
                    w = img_data.get("width", 0)
                    h = img_data.get("height", 0)
                    if w < 50 or h < 50:
                        continue  # too small (icons, decorations)
                    aspect = max(w, h) / max(min(w, h), 1)
                    if aspect > 4:
                        continue  # too elongated (banners, borders)
                    # Check if image is mostly a single color (backgrounds)
                    raw = img_data.get("image", b"")
                    if len(raw) < 1000 and w * h > 10000:
                        continue  # very small file but large dimensions = solid color
                    candidates.append((w, h, aspect, img_data))
                except Exception:
                    continue

            if not candidates:
                skipped += 1
                continue

            # Prefer photos: not the largest (likely background), not the smallest
            # Sort by area descending, skip the largest if there are multiple
            candidates.sort(key=lambda c: c[0] * c[1], reverse=True)
            if len(candidates) > 1:
                # Skip the largest (likely background), take the next biggest
                img_data = candidates[1][3]
            else:
                img_data = candidates[0][3]
            ext = img_data.get("ext", "png")
            photo_filename = f"{slide_id}.{ext}"
            photo_path = os.path.join(PHOTOS_DIR, photo_filename)

            with open(photo_path, "wb") as f:
                f.write(img_data["image"])

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
                content_hash=content_hash
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
