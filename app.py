import os
import logging
import threading
from flask import Flask, render_template, jsonify, request, send_from_directory, Response
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv

load_dotenv()

import httpx
from database import init_db, get_db, get_attendee_names, search_attendees, get_all_attendees, get_attendees_by_ids, get_attendees_by_names, get_cached_matches, get_attendee_by_name, get_all_cached_matches, get_attendee_photo
from slides import refresh_slides, fetch_profile_photos
from matcher import get_matches_for_user

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Initialize database in background so worker boots fast and /healthz responds immediately
def _init_db_background():
    for _attempt in range(3):
        try:
            init_db()
            logger.info("Database initialized successfully")
            return
        except Exception as e:
            logger.warning(f"init_db attempt {_attempt + 1} failed: {e}")
            import time as _time
            _time.sleep(2)
    logger.error("Failed to initialize database after 3 attempts")

threading.Thread(target=_init_db_background, daemon=True).start()


# --- Routes ---

@app.route("/healthz")
def healthz():
    return "ok", 200


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/names")
def api_names():
    names = get_attendee_names()
    return jsonify(names)


@app.route("/api/search")
def api_search():
    q = request.args.get("q", "").strip()
    name_only = request.args.get("name_only", "").lower() in ("1", "true", "yes")
    if not q:
        attendees = get_all_attendees()
    else:
        attendees = search_attendees(q, name_only=name_only)
    # Strip internal fields
    for a in attendees:
        a.pop("slide_content_hash", None)
        a.pop("slide_object_id", None)
        a.pop("photo_data", None)
        a.pop("photo_content_type", None)
    return jsonify(attendees)


@app.route("/api/match")
def api_match():
    user_name = request.args.get("name", "").strip()
    if not user_name:
        return jsonify({"error": "Missing 'name' parameter"}), 400
    result = get_matches_for_user(user_name)

    # Limit to top 10 for the user-facing match page (full list still used by graph)
    if "matches" in result:
        result = dict(result)
        result["matches"] = result["matches"][:10]

    # Detect mutual matches: check if each matched person also matched the current user
    user = get_attendee_by_name(user_name)
    if user and "matches" in result:
        user_id = user["id"]
        for match in result["matches"]:
            match["mutual"] = False
            match_name = match.get("name", "")
            their_cached = get_cached_matches(match_name)
            if their_cached and "matches" in their_cached:
                their_match_ids = {m.get("attendee_id") for m in their_cached["matches"]}
                if user_id in their_match_ids:
                    match["mutual"] = True

    return jsonify(result)


@app.route("/api/star", methods=["POST"])
def api_star():
    data = request.get_json()
    if not data:
        return jsonify({"error": "Missing JSON body"}), 400
    # Stars are managed client-side via cookies; this endpoint just validates the attendee exists
    attendee_id = data.get("id")
    action = data.get("action", "star")  # "star" or "unstar"
    return jsonify({"status": "ok", "id": attendee_id, "action": action})


@app.route("/api/stars")
def api_stars():
    # Name-based lookup (used by starred panel)
    names_param = request.args.get("names", "")
    if names_param:
        names = [n.strip() for n in names_param.split("|") if n.strip()]
        attendees = get_attendees_by_names(names)
    else:
        # ID-based lookup (used by match page for attendee_id references)
        ids_param = request.args.get("ids", "")
        if not ids_param:
            return jsonify([])
        try:
            ids = [int(x) for x in ids_param.split(",") if x.strip()]
        except ValueError:
            return jsonify([])
        attendees = get_attendees_by_ids(ids)
    for a in attendees:
        a.pop("slide_content_hash", None)
        a.pop("slide_object_id", None)
        a.pop("photo_data", None)
        a.pop("photo_content_type", None)
    return jsonify(attendees)


@app.route("/graph")
def graph():
    return render_template("graph.html")


@app.route("/api/graph")
def api_graph():
    attendees = get_all_attendees()
    nodes = [{"id": a["id"], "name": a["name"], "thumbnail_url": a.get("thumbnail_url", "")} for a in attendees]
    node_id_by_name = {}
    for a in attendees:
        node_id_by_name[a["name"].lower()] = a["id"]

    all_matches = get_all_cached_matches()

    # Build directed edge set: source -> set of targets
    directed = {}
    for entry in all_matches:
        source_name = entry["user_name"]
        source_id = node_id_by_name.get(source_name.lower())
        if source_id is None:
            continue
        match_data = entry["matches"]
        if isinstance(match_data, dict):
            match_list = match_data.get("matches", [])
        elif isinstance(match_data, list):
            match_list = match_data
        else:
            continue
        for m in match_list:
            target_id = m.get("attendee_id")
            if target_id is not None:
                directed.setdefault(source_id, set()).add(target_id)

    # Build edges with mutual detection
    edges = []
    seen = set()
    for source_id, targets in directed.items():
        for target_id in targets:
            edge_key = (min(source_id, target_id), max(source_id, target_id))
            if edge_key in seen:
                continue
            seen.add(edge_key)
            reverse = target_id in directed and source_id in directed.get(target_id, set())
            edges.append({"source": source_id, "target": target_id, "mutual": reverse})

    return jsonify({"nodes": nodes, "edges": edges})


@app.route("/photos/<path:filename>")
def serve_photo(filename):
    # Parse slide_object_id from filename (e.g., "attendee_14.png" -> "page_14")
    base = filename.rsplit(".", 1)[0] if "." in filename else filename
    slide_object_id = base.replace("attendee_", "page_", 1) if base.startswith("attendee_") else base
    photo_data, content_type = get_attendee_photo(slide_object_id)
    if photo_data is None:
        return "Not found", 404
    # Use ETag based on content hash for cache validation instead of long max-age,
    # so browsers always revalidate and never show stale photos after re-extraction
    import hashlib
    etag = hashlib.md5(photo_data).hexdigest()
    if_none_match = request.headers.get("If-None-Match")
    if if_none_match and if_none_match.strip('"') == etag:
        return "", 304
    response = Response(photo_data, mimetype=content_type or "image/png")
    response.headers["ETag"] = f'"{etag}"'
    response.cache_control.no_cache = True
    response.cache_control.public = True
    return response


@app.route("/download/<path:filename>")
def download_file(filename):
    return send_from_directory(os.path.join(app.root_path, "static"), filename)


_refresh_status = {"running": False, "result": None, "error": None}
_refresh_lock = threading.Lock()

def _run_refresh_in_background(force=False):
    global _refresh_status
    try:
        result = refresh_slides(force=force)
        with _refresh_lock:
            _refresh_status = {"running": False, "result": result, "error": None}
        logger.info(f"Background refresh complete: {result}")
    except Exception as e:
        with _refresh_lock:
            _refresh_status = {"running": False, "result": None, "error": str(e)}
        logger.error(f"Background refresh failed: {e}")

@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    global _refresh_status
    with _refresh_lock:
        if _refresh_status["running"]:
            return jsonify({"status": "already_running"})
        _refresh_status = {"running": True, "result": None, "error": None}
    force = request.get_json(silent=True) or {}
    thread = threading.Thread(target=_run_refresh_in_background, kwargs={"force": force.get("force", False)}, daemon=True)
    thread.start()
    return jsonify({"status": "started"})

@app.route("/api/refresh-status")
def api_refresh_status():
    with _refresh_lock:
        return jsonify(_refresh_status)


_precompute_status = {"running": False, "result": None, "error": None}
_precompute_lock = threading.Lock()

def _run_precompute_in_background():
    global _precompute_status
    try:
        from matcher import precompute_all_matches
        result = precompute_all_matches()
        with _precompute_lock:
            _precompute_status = {"running": False, "result": result, "error": None}
        logger.info(f"Background precompute complete: {result}")
    except Exception as e:
        with _precompute_lock:
            _precompute_status = {"running": False, "result": None, "error": str(e)}
        logger.error(f"Background precompute failed: {e}")

@app.route("/api/precompute-matches", methods=["POST"])
def api_precompute_matches():
    global _precompute_status
    with _precompute_lock:
        if _precompute_status["running"]:
            return jsonify({"status": "already_running"})
        _precompute_status = {"running": True, "result": None, "error": None}
    thread = threading.Thread(target=_run_precompute_in_background, daemon=True)
    thread.start()
    return jsonify({"status": "started"})

@app.route("/api/precompute-status")
def api_precompute_status():
    with _precompute_lock:
        return jsonify(_precompute_status)


_fix_linkedin_status = {"running": False, "result": None, "error": None}
_fix_linkedin_lock = threading.Lock()

def _run_fix_linkedin_in_background():
    global _fix_linkedin_status
    try:
        from slides import extract_linkedin_urls, download_presentation_pdf
        pdf_bytes = download_presentation_pdf()
        linkedin_urls = extract_linkedin_urls(pdf_bytes)
        conn = get_db()
        cur = conn.cursor()
        cur.execute("UPDATE attendees SET linkedin_url = ''")
        updated = 0
        for page_num, url in linkedin_urls.items():
            slide_id = f"page_{page_num}"
            cur.execute("UPDATE attendees SET linkedin_url = %s WHERE slide_object_id = %s AND name != ''",
                        (url, slide_id))
            updated += cur.rowcount
        conn.commit()
        from database import _invalidate_attendees
        _invalidate_attendees()
        result = {"linkedin_urls_found": len(linkedin_urls), "attendees_updated": updated}
        with _fix_linkedin_lock:
            _fix_linkedin_status = {"running": False, "result": result, "error": None}
        logger.info(f"LinkedIn fix complete: {result}")
    except Exception as e:
        with _fix_linkedin_lock:
            _fix_linkedin_status = {"running": False, "result": None, "error": str(e)}
        logger.error(f"LinkedIn fix failed: {e}")

@app.route("/api/fix-linkedin", methods=["POST"])
def api_fix_linkedin():
    global _fix_linkedin_status
    with _fix_linkedin_lock:
        if _fix_linkedin_status["running"]:
            return jsonify({"status": "already_running"})
        _fix_linkedin_status = {"running": True, "result": None, "error": None}
    thread = threading.Thread(target=_run_fix_linkedin_in_background, daemon=True)
    thread.start()
    return jsonify({"status": "started"})

@app.route("/api/fix-linkedin-status")
def api_fix_linkedin_status():
    with _fix_linkedin_lock:
        return jsonify(_fix_linkedin_status)


@app.route("/api/fetch-photos", methods=["POST"])
def api_fetch_photos():
    result = fetch_profile_photos()
    return jsonify(result)


@app.route("/api/verify-mapping")
def api_verify_mapping():
    """Compare DB names against current PDF page text to detect stale data."""
    import fitz
    from slides import PDF_EXPORT_URL
    try:
        resp = httpx.get(PDF_EXPORT_URL, follow_redirects=True, timeout=120)
        resp.raise_for_status()
    except Exception as e:
        return jsonify({"error": f"Failed to download PDF: {e}"})
    doc = fitz.open(stream=resp.content, filetype="pdf")
    conn = get_db()
    from psycopg2.extras import RealDictCursor
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT slide_object_id, name FROM attendees WHERE name != ''")
    rows = cur.fetchall()
    results = []
    for row in rows:
        page_num = int(row["slide_object_id"].split("_")[1])
        if page_num >= len(doc):
            results.append({"page": page_num, "db_name": row["name"], "status": "OUT_OF_RANGE"})
            continue
        page_text = doc[page_num].get_text()[:300]
        name_parts = row["name"].lower().split()
        found = all(part in page_text.lower() for part in name_parts if len(part) > 2)
        if not found and len(name_parts) >= 2:
            found = name_parts[0] in page_text.lower() and name_parts[-1] in page_text.lower()
        if not found:
            results.append({"page": page_num, "db_name": row["name"], "pdf_text": page_text[:150], "status": "MISMATCH"})
    doc.close()
    return jsonify({"total_checked": len(rows), "mismatches": len(results), "details": results})


@app.route("/api/debug-images")
def api_debug_images():
    """Show all images extracted from a specific slide."""
    from slides import debug_slide_images
    page = request.args.get("page", "14")
    return jsonify(debug_slide_images(int(page)))


# --- Background Scheduler ---

def scheduled_refresh():
    logger.info("Running scheduled slide refresh...")
    try:
        refresh_slides()
    except Exception as e:
        logger.error(f"Scheduled refresh failed: {e}")


# Guard against duplicate schedulers when running with multiple threads/workers
_scheduler_started = False
if not _scheduler_started:
    scheduler = BackgroundScheduler()
    scheduler.add_job(scheduled_refresh, "interval", hours=1, id="slide_refresh")
    scheduler.start()
    _scheduler_started = True


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)
