import os
import logging
from flask import Flask, render_template, jsonify, request, send_from_directory
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv

load_dotenv()

from database import init_db, get_attendee_names, search_attendees, get_all_attendees, get_attendees_by_ids, get_cached_matches, get_attendee_by_name
from slides import refresh_slides, fetch_profile_photos, PHOTOS_DIR
from matcher import get_matches_for_user

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Initialize database
init_db()


# --- Routes ---

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
    return jsonify(attendees)


@app.route("/api/match")
def api_match():
    user_name = request.args.get("name", "").strip()
    if not user_name:
        return jsonify({"error": "Missing 'name' parameter"}), 400
    result = get_matches_for_user(user_name)

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
    return jsonify(attendees)


@app.route("/photos/<path:filename>")
def serve_photo(filename):
    return send_from_directory(PHOTOS_DIR, filename)


@app.route("/download/<path:filename>")
def download_file(filename):
    return send_from_directory(os.path.join(app.root_path, "static"), filename)


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    result = refresh_slides()
    return jsonify(result)


@app.route("/api/fetch-photos", methods=["POST"])
def api_fetch_photos():
    result = fetch_profile_photos()
    return jsonify(result)


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


scheduler = BackgroundScheduler()
scheduler.add_job(scheduled_refresh, "interval", hours=1, id="slide_refresh")
scheduler.start()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)
