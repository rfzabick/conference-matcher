import os
import io
import hashlib
import logging
import httpx
import fitz  # PyMuPDF
from pypdf import PdfReader, PdfWriter
from database import get_known_slide_ids, upsert_attendee, clear_match_cache, update_attendee_thumbnail, update_attendee_photo, clear_photo_cache, clear_attendee_photo

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


def extract_linkedin_urls(pdf_bytes):
    """Extract LinkedIn profile URLs from PDF page hyperlinks. Returns dict of page_num -> url."""
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    result = {}
    for i in range(len(doc)):
        for link in doc[i].get_links():
            uri = link.get("uri", "")
            if "linkedin.com/in/" in uri:
                result[i] = uri
                break
    doc.close()
    return result


def extract_attendee_data_from_pdf_page_text(page):
    """Extract structured attendee data by parsing PDF page text directly.

    Slides follow a consistent format with sections "Stuff I do",
    "Stuff I can share/help with", and "Stuff I need", each containing
    bullet points marked with ●. The attendee's name appears in a large
    font near the top of the slide.

    Args:
        page: A fitz (PyMuPDF) page object.

    Returns:
        dict with keys name, stuff_i_do, stuff_i_can_share, stuff_i_need,
        or None if this page is not an attendee slide.
    """
    import re
    text = page.get_text()

    # --- Extract name from large-font text blocks ---
    skip_texts = {"Stuff I do", "Stuff I can share/help with", "Stuff I can share / help with",
                  "Stuff I need", "Example", "Full Name", "Website", "Linkedin",
                  "Title and Company", "Please upload", "a profile picture here"}
    blocks = page.get_text("dict")
    name_candidates = []
    for block in blocks["blocks"]:
        if "lines" not in block:
            continue
        for line in block["lines"]:
            for span in line["spans"]:
                tc = span["text"].strip()
                fs = span["size"]
                if (fs >= 14 and tc and tc not in skip_texts
                        and "●" not in tc and "@" not in tc
                        and "linkedin" not in tc.lower()
                        and "http" not in tc.lower()
                        and len(tc) > 2 and len(tc) < 50):
                    name_candidates.append((fs, line["bbox"][1], tc))

    # Largest font first, then topmost
    name_candidates.sort(key=lambda x: (-x[0], x[1]))
    name = name_candidates[0][2].strip() if name_candidates else ""

    # If no "Stuff I do" section, this is either a non-attendee page or
    # an image-heavy page with just a name. Return name-only if we found a
    # name AND the page has personal indicators (LinkedIn/Website text or links).
    if "Stuff I do" not in text:
        has_personal = ("linkedin" in text.lower() or "website" in text.lower()
                        or any("linkedin.com" in l.get("uri", "") for l in page.get_links()))
        if name and has_personal:
            return {
                "name": name,
                "stuff_i_do": "",
                "stuff_i_can_share": "",
                "stuff_i_need": "",
            }
        return None

    # --- Parse sections ---
    do_match = re.search(r"Stuff I do[\s/\w]*\n", text)
    share_match = re.search(r"Stuff I can share/?[\s]?help with\s*\n", text)
    need_match = re.search(r"Stuff I need\s*\n", text)

    if not do_match:
        return None

    do_start = do_match.end()
    do_end = share_match.start() if share_match else (need_match.start() if need_match else len(text))
    do_text = text[do_start:do_end]

    share_text = ""
    if share_match:
        share_start = share_match.end()
        share_end = need_match.start() if need_match else len(text)
        share_text = text[share_start:share_end]

    need_text = ""
    if need_match:
        need_text = text[need_match.end():]
        # Trim at contact info (email, linkedin, or name/title block after bullets)
        lines = need_text.split("\n")
        clean_lines = []
        for line in lines:
            stripped = line.strip()
            if stripped == "●":
                clean_lines.append(line)
                continue
            if not stripped:
                continue
            if "@" in stripped or "linkedin" in stripped.lower():
                break
            clean_lines.append(line)
        need_text = "\n".join(clean_lines)

    # --- Parse bullet points, preserving original bullet structure ---
    def parse_bullets(section_text):
        parts = re.split(r"●\s*\n?", section_text)
        items = []
        for p in parts:
            cleaned = " ".join(p.split("\n")).strip()
            cleaned = re.sub(r"\s+", " ", cleaned).strip()
            if cleaned:
                items.append(cleaned)
        if len(items) <= 1:
            return items[0] if items else ""
        return "\n".join("• " + item for item in items)

    stuff_i_do = parse_bullets(do_text)
    stuff_i_can_share = parse_bullets(share_text)
    stuff_i_need = parse_bullets(need_text)

    # Skip template slides with placeholder content
    if "add 2-4 bullet points" in stuff_i_do.lower():
        return None

    return {
        "name": name,
        "stuff_i_do": stuff_i_do,
        "stuff_i_can_share": stuff_i_can_share,
        "stuff_i_need": stuff_i_need,
    }


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
    clear_photo_cache()
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

    # Check which slides already have photos AND a valid thumbnail_url in the DB.
    # Slides with photo_data but empty thumbnail_url need repair (thumbnail got wiped by upsert).
    from database import get_db, put_db
    from psycopg2.extras import RealDictCursor
    conn = get_db()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT slide_object_id, thumbnail_url, photo_content_type FROM attendees WHERE photo_data IS NOT NULL")
        rows = cur.fetchall()
        has_photo = set()
        for r in rows:
            if r["thumbnail_url"]:
                has_photo.add(r["slide_object_id"])
            else:
                # Repair: photo_data exists but thumbnail_url is empty — re-set the thumbnail URL
                ext = (r["photo_content_type"] or "image/png").split("/")[-1]
                if ext == "jpeg":
                    ext = "jpg"
                url_id = r['slide_object_id'].replace("page_", "attendee_", 1)
                repair_url = f"/photos/{url_id}.{ext}"
                update_attendee_thumbnail(r["slide_object_id"], repair_url)
                logger.info(f"Repaired missing thumbnail_url for {r['slide_object_id']}: {repair_url}")
                has_photo.add(r["slide_object_id"])
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
                    if rendered_w < 50 or rendered_h < 50:
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
                    # Detect upscaled images (native smaller than rendered) —
                    # real profile photos are high-res and downscaled, not upscaled
                    is_upscaled = (rendered_w > w * 1.1) or (rendered_h > h * 1.1)
                    candidates.append((rendered_w, rendered_h, aspect, img_data, coverage, file_size, is_upscaled))
                except Exception:
                    continue

            if not candidates:
                skipped += 1
                continue

            # Prefer non-upscaled images, then squarer aspect ratio, then larger file size
            candidates.sort(key=lambda c: (c[6], c[2], -c[5]))
            img_data = candidates[0][3]
            ext = img_data.get("ext", "png")
            url_id = slide_id.replace("page_", "attendee_", 1)
            photo_filename = f"{url_id}.{ext}"
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


def refresh_slides(force=False):
    """Download presentation PDF, diff against DB, and process only new/changed slides.
    If force=True, re-extract all slides regardless of content hash."""
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

    # Extract LinkedIn URLs from PDF hyperlinks (reliable, not AI-guessed)
    linkedin_urls = extract_linkedin_urls(pdf_bytes)
    logger.info(f"Found {len(linkedin_urls)} LinkedIn hyperlinks in PDF")

    # Open PDF with fitz for programmatic text extraction
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")

    known = get_known_slide_ids()
    new_count = 0
    updated_count = 0
    skipped_count = 0
    error_count = 0

    for page_num, page_bytes, content_hash in pages:
        slide_id = f"page_{page_num}"

        # Skip if we already have this exact version (unless force refresh)
        if not force and slide_id in known and known[slide_id] == content_hash:
            skipped_count += 1
            continue

        is_new = slide_id not in known

        # Content changed — clear the old photo so it gets re-extracted
        if not is_new:
            clear_attendee_photo(slide_id)
            logger.info(f"Cleared stale photo for {slide_id} (content changed)")

        try:
            fitz_page = doc[page_num]
            data = extract_attendee_data_from_pdf_page_text(fitz_page)
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
                name=data.get("name", "").strip(),
                stuff_i_do=data.get("stuff_i_do", ""),
                stuff_i_can_share=data.get("stuff_i_can_share", ""),
                stuff_i_need=data.get("stuff_i_need", ""),
                thumbnail_url="",
                content_hash=content_hash,
                linkedin_url=linkedin_urls.get(page_num, "")
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

    doc.close()

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
