import os
import io
import json
import hashlib
import base64
import logging
import zipfile
import xml.etree.ElementTree as ET
import httpx
import anthropic
from pypdf import PdfReader, PdfWriter
from database import get_known_slide_ids, upsert_attendee, clear_match_cache, update_attendee_thumbnail

logger = logging.getLogger(__name__)

PRESENTATION_ID = os.environ.get("PRESENTATION_ID", "")

PDF_EXPORT_URL = f"https://docs.google.com/presentation/d/{PRESENTATION_ID}/export/pdf"
PPTX_EXPORT_URL = f"https://docs.google.com/presentation/d/{PRESENTATION_ID}/export/pptx"

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


def fetch_profile_photos():
    """Download PPTX, extract profile photos from each slide, and save locally."""
    if not PRESENTATION_ID:
        logger.warning("Missing PRESENTATION_ID. Skipping photo fetch.")
        return {"status": "skipped", "reason": "missing PRESENTATION_ID"}

    os.makedirs(PHOTOS_DIR, exist_ok=True)

    logger.info("Downloading presentation as PPTX...")
    try:
        resp = httpx.get(PPTX_EXPORT_URL, follow_redirects=True, timeout=120)
        resp.raise_for_status()
        pptx_bytes = resp.content
        logger.info(f"Downloaded PPTX: {len(pptx_bytes)} bytes")
    except Exception as e:
        logger.error(f"Failed to download PPTX: {e}")
        return {"status": "error", "reason": str(e)}

    try:
        zf = zipfile.ZipFile(io.BytesIO(pptx_bytes))
    except Exception as e:
        logger.error(f"Failed to open PPTX as ZIP: {e}")
        return {"status": "error", "reason": str(e)}

    # Find all slide files (slide1.xml, slide2.xml, ...) and sort by number
    slide_files = sorted(
        [n for n in zf.namelist() if n.startswith("ppt/slides/slide") and n.endswith(".xml")],
        key=lambda n: int(''.join(filter(str.isdigit, n.split("/")[-1])))
    )
    logger.info(f"Found {len(slide_files)} slides in PPTX")

    ns = {
        "a": "http://schemas.openxmlformats.org/drawingml/2006/main",
        "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
        "p": "http://schemas.openxmlformats.org/presentationml/2006/main",
        "rel": "http://schemas.openxmlformats.org/package/2006/relationships",
    }

    fetched = 0
    skipped = 0
    errors = 0

    for slide_idx, slide_path in enumerate(slide_files):
        slide_id = f"page_{slide_idx}"
        # Check if we already have a photo for this slide
        existing = [f for f in os.listdir(PHOTOS_DIR) if f.startswith(f"{slide_id}.")]
        if existing:
            skipped += 1
            continue

        try:
            # Parse the slide XML to find image references
            slide_xml = zf.read(slide_path)
            slide_tree = ET.fromstring(slide_xml)

            # Parse the relationships file for this slide
            slide_num = slide_path.split("/")[-1]  # e.g. "slide1.xml"
            rels_path = f"ppt/slides/_rels/{slide_num}.rels"
            if rels_path not in zf.namelist():
                skipped += 1
                continue

            rels_xml = zf.read(rels_path)
            rels_tree = ET.fromstring(rels_xml)

            # Build a map of relationship ID -> target file
            rel_map = {}
            for rel in rels_tree.findall("rel:Relationship", ns):
                rid = rel.get("Id")
                target = rel.get("Target")
                if target and rid:
                    rel_map[rid] = target

            # Find all image references in the slide (blipFill elements with r:embed)
            images = []
            for blip in slide_tree.iter(f"{{{ns['a']}}}blip"):
                embed_id = blip.get(f"{{{ns['r']}}}embed")
                if embed_id and embed_id in rel_map:
                    media_path = "ppt/slides/" + rel_map[embed_id]
                    # Normalize path (resolve ../media/image1.png)
                    media_path = os.path.normpath(media_path).replace("\\", "/")
                    if media_path in zf.namelist():
                        file_size = zf.getinfo(media_path).file_size
                        images.append((file_size, media_path))

            if not images:
                skipped += 1
                continue

            # Pick the largest image (most likely the profile photo)
            images.sort(reverse=True)
            best_image_path = images[0][1]
            ext = os.path.splitext(best_image_path)[1] or ".png"
            photo_filename = f"{slide_id}{ext}"
            photo_path = os.path.join(PHOTOS_DIR, photo_filename)

            # Extract and save the image
            img_data = zf.read(best_image_path)
            with open(photo_path, "wb") as f:
                f.write(img_data)

            # Update the database
            update_attendee_thumbnail(slide_id, f"/photos/{photo_filename}")
            fetched += 1
            logger.info(f"Saved photo for slide {slide_idx}: {photo_filename} ({len(img_data)} bytes)")

        except Exception as e:
            logger.error(f"Error extracting photo from slide {slide_idx}: {e}")
            errors += 1

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

    # Fetch profile photos
    fetch_profile_photos()

    # Pre-compute matches if any slides were new or updated
    if new_count > 0 or updated_count > 0:
        logger.info("New/updated slides found — clearing match cache and pre-computing matches...")
        clear_match_cache()
        from matcher import precompute_all_matches
        precompute_all_matches()

    return result
