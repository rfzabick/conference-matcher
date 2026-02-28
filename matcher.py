import json
import logging
import threading
import anthropic
from database import get_all_attendees, get_attendee_by_name, get_cached_matches, set_cached_matches

logger = logging.getLogger(__name__)

_match_locks = {}
_match_locks_lock = threading.Lock()


def get_matches_for_user(user_name):
    """Get AI-suggested matches for the identified user."""
    # Check cache first
    cached = get_cached_matches(user_name)
    if cached is not None:
        logger.info(f"Returning cached matches for {user_name}")
        return cached

    # Acquire per-user lock to prevent duplicate Claude API calls
    with _match_locks_lock:
        if user_name not in _match_locks:
            _match_locks[user_name] = threading.Lock()
        lock = _match_locks[user_name]

    with lock:
        # Double-check cache after acquiring lock
        cached = get_cached_matches(user_name)
        if cached is not None:
            logger.info(f"Returning cached matches for {user_name} (after lock)")
            return cached

        # Look up the user
        user = get_attendee_by_name(user_name)
        if not user:
            return {"error": f"User '{user_name}' not found in attendee list"}

        # Get all other attendees
        all_attendees = get_all_attendees()
        others = [a for a in all_attendees if a["id"] != user["id"]]

        if not others:
            return {"matches": [], "message": "No other attendees found yet"}

        # Build profiles summary for the prompt
        user_profile = _format_profile(user)
        other_profiles = "\n\n".join(
            f"[ID:{a['id']}] {_format_profile(a)}" for a in others
        )

        client = anthropic.Anthropic()

        prompt = f"""You are a conference networking assistant. Given an attendee's profile and a list of other attendees, suggest the 3-10 best people for them to meet.

Each attendee has three sections:
- "Stuff I do" - their work, projects, and activities
- "Stuff I can share/help with" - skills, knowledge, and resources they offer
- "Stuff I need" - what they're looking for: help, connections, resources, advice

PRIORITY MATCHING RULES:
1. MOST IMPORTANT: Match what the current user NEEDS with what others CAN SHARE, and vice versa. If someone can help with what this person needs, or needs what this person can share, that's the strongest match.
2. ALSO VALUABLE: People doing similar or complementary work ("Stuff I do" overlap).
3. Explain each match in terms of the specific give/get dynamic.

YOUR PROFILE:
{user_profile}

OTHER ATTENDEES:
{other_profiles}

Respond with ONLY valid JSON in this format:
{{
  "matches": [
    {{
      "attendee_id": <integer ID>,
      "name": "their name",
      "reason": "A short, friendly 1-2 sentence explanation focusing on the specific give/get: what they can help you with or what you can help them with"
    }}
  ]
}}

Order matches from strongest to weakest. Include 3-10 matches depending on how many good connections exist."""

        try:
            response = client.messages.create(
                model="claude-haiku-4-5",
                max_tokens=2048,
                messages=[{"role": "user", "content": prompt}]
            )

            text = response.content[0].text.strip()
            # Handle markdown code blocks
            if "```" in text:
                text = text.split("```json")[-1].split("```")[0].strip()
                if not text:
                    text = response.content[0].text.split("```")[1].split("```")[0].strip()

            result = json.loads(text)

            # Validate attendee IDs exist
            valid_ids = {a["id"] for a in others}
            result["matches"] = [
                m for m in result.get("matches", [])
                if m.get("attendee_id") in valid_ids
            ]

            # Cache the result
            set_cached_matches(user_name, result)

            return result

        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"Failed to parse matching response: {e}")
            return {"error": "Failed to generate matches. Please try again."}
        except Exception as e:
            logger.error(f"Error generating matches: {e}")
            return {"error": str(e)}


def precompute_all_matches():
    """Pre-compute matches for every attendee. Call after reingestion."""
    all_attendees = get_all_attendees()
    if len(all_attendees) < 2:
        logger.info("Not enough attendees to pre-compute matches")
        return

    logger.info(f"Pre-computing matches for {len(all_attendees)} attendees...")

    # Build the other-profiles block once (we'll swap out the "you" each time)
    all_profiles = {a["id"]: _format_profile(a) for a in all_attendees}
    client = anthropic.Anthropic()
    computed = 0
    errors = 0

    for user in all_attendees:
        # Skip if already cached and valid
        cached = get_cached_matches(user["name"])
        if cached is not None:
            continue

        others = [a for a in all_attendees if a["id"] != user["id"]]
        user_profile = all_profiles[user["id"]]
        other_profiles_text = "\n\n".join(
            f"[ID:{a['id']}] {all_profiles[a['id']]}" for a in others
        )

        prompt = f"""You are a conference networking assistant. Given an attendee's profile and a list of other attendees, suggest the 3-10 best people for them to meet.

Each attendee has three sections:
- "Stuff I do" - their work, projects, and activities
- "Stuff I can share/help with" - skills, knowledge, and resources they offer
- "Stuff I need" - what they're looking for: help, connections, resources, advice

PRIORITY MATCHING RULES:
1. MOST IMPORTANT: Match what the current user NEEDS with what others CAN SHARE, and vice versa. If someone can help with what this person needs, or needs what this person can share, that's the strongest match.
2. ALSO VALUABLE: People doing similar or complementary work ("Stuff I do" overlap).
3. Explain each match in terms of the specific give/get dynamic.

YOUR PROFILE:
{user_profile}

OTHER ATTENDEES:
{other_profiles_text}

Respond with ONLY valid JSON in this format:
{{
  "matches": [
    {{
      "attendee_id": <integer ID>,
      "name": "their name",
      "reason": "A short, friendly 1-2 sentence explanation focusing on the specific give/get: what they can help you with or what you can help them with"
    }}
  ]
}}

Order matches from strongest to weakest. Include 3-10 matches depending on how many good connections exist."""

        try:
            response = client.messages.create(
                model="claude-haiku-4-5",
                max_tokens=2048,
                messages=[{"role": "user", "content": prompt}]
            )

            text = response.content[0].text.strip()
            if "```" in text:
                text = text.split("```json")[-1].split("```")[0].strip()
                if not text:
                    text = response.content[0].text.split("```")[1].split("```")[0].strip()

            result = json.loads(text)
            valid_ids = {a["id"] for a in others}
            result["matches"] = [
                m for m in result.get("matches", [])
                if m.get("attendee_id") in valid_ids
            ]

            set_cached_matches(user["name"], result)
            computed += 1
            logger.info(f"Pre-computed matches for {user['name']} ({computed}/{len(all_attendees)})")

        except Exception as e:
            logger.error(f"Failed to pre-compute matches for {user['name']}: {e}")
            errors += 1

    logger.info(f"Pre-computation complete: {computed} computed, {errors} errors")


def _format_profile(attendee):
    parts = [f"Name: {attendee['name']}"]
    if attendee.get("stuff_i_do"):
        parts.append(f"Stuff I do: {attendee['stuff_i_do']}")
    if attendee.get("stuff_i_can_share"):
        parts.append(f"Stuff I can share/help with: {attendee['stuff_i_can_share']}")
    if attendee.get("stuff_i_need"):
        parts.append(f"Stuff I need: {attendee['stuff_i_need']}")
    return "\n".join(parts)
