# den_social/main/events_config.py
import os

# -----------------------------
# AREA SETTINGS
# -----------------------------
NEIGHBORHOOD = os.getenv("EVENTS_NEIGHBORHOOD", "Pacific Beach")
CITY = os.getenv("EVENTS_CITY", "San Diego")
STATE = os.getenv("EVENTS_STATE", "California")
COUNTRY = os.getenv("EVENTS_COUNTRY", "United States")
TIMEZONE = os.getenv("EVENTS_TIMEZONE", "America/Los_Angeles")

# Optional geofence (helps filter to the neighborhood)
# Pacific Beach approx center; tweak as needed.
CENTER_LAT = float(os.getenv("EVENTS_CENTER_LAT", "32.7976"))
CENTER_LON = float(os.getenv("EVENTS_CENTER_LON", "-117.2526"))
RADIUS_MILES = float(os.getenv("EVENTS_RADIUS_MILES", "5"))

# -----------------------------
# CRAWL / PARSE KNOBS
# -----------------------------
REQUEST_TIMEOUT_S = int(os.getenv("EVENTS_REQUEST_TIMEOUT_S", "20"))
MAX_SEED_PAGES = int(os.getenv("EVENTS_MAX_SEED_PAGES", "20"))        # how many seed URLs to attempt
MAX_FOLLOW_PER_SEED = int(os.getenv("EVENTS_MAX_FOLLOW_PER_SEED", "12"))  # detail pages to follow per seed
MAX_EVENTS = int(os.getenv("EVENTS_MAX_EVENTS", "400"))
FUTURE_DAYS_LIMIT = int(os.getenv("EVENTS_FUTURE_DAYS_LIMIT", "60"))  # keep events within N days ahead

# -----------------------------
# OUTPUT
# -----------------------------
OUTPUT_TXT_PATH = os.getenv("EVENTS_OUTPUT_TXT", "events_output.txt")
OUTPUT_RAW_JSONL_PATH = os.getenv("EVENTS_OUTPUT_JSONL", "events_raw.jsonl")
USE_OPENAI_FORMATTING = os.getenv("EVENTS_USE_OPENAI", "1").lower() in ("1", "true", "yes")

# -----------------------------
# SEED PAGES (edit freely)
# Add neighborhood calendars and venues you care about.
# -----------------------------
SOURCE_PAGES = [
    "https://www.sandiegoreader.com/events/pacific-beach/",
    "https://www.pacificbeach.org/events/",
    "https://www.eventbrite.com/d/ca--san-diego/pacific-beach--events/",
    "https://www.meetup.com/find/?source=EVENTS&keywords=Pacific%20Beach&location=us--CA--San+Diego",
    "https://www.belmontpark.com/events/",
    "https://www.pbshoreclub.com/events",
]

# -----------------------------
# OPENAI (formatting/cleanup only)
# Matches your import/use pattern exactly.
# -----------------------------
OPENAI_MODEL = os.getenv("EVENTS_OPENAI_MODEL", "gpt-4o-mini")
MAX_TOTAL_TOKENS = int(os.getenv("EVENTS_MAX_TOTAL_TOKENS", "120000"))
DESIRED_OUTPUT_TOKENS = int(os.getenv("EVENTS_DESIRED_OUTPUT_TOKENS", "1500"))

# Prompt passed to OpenAI to clean/normalize/dedupe the scraped events
# Use .format(...) on the 'user' block to inject the area + the raw JSON.
events_prompt = [
    {
        "role": "system",
        "content": (
            "You are a precise local events curator. Given a JSON array of raw events scraped "
            "from the web, deduplicate near-duplicates, normalize dates/times to the area's "
            "timezone, correct obvious parse issues, and output a clean human-readable text list. "
            "Only keep events that are likely in or near the target area and within the next N days. "
            "Do not hallucinate missing details; leave fields blank if not present."
        ),
    },
    {
        "role": "user",
        "content": (
            "Target Area:\n"
            "Neighborhood: {neighborhood}\n"
            "City: {city}\n"
            "State: {state}\n"
            "Timezone: {timezone}\n"
            "Future window: next {future_days} days\n\n"
            "Requirements:\n"
            "1) Deduplicate events with same title and date/time (±90 minutes).\n"
            "2) Normalize formats:\n"
            "   - Date: Mon, Sep 9, 2025\n"
            "   - Time: 7:30 PM (12‑hour, omit if unknown)\n"
            "   - Price: 'Free' or '$<amount>' if parsable\n"
            "3) Output format per event (one blank line between events):\n"
            "   Title\n"
            "   Date — Time\n"
            "   Venue · Address\n"
            "   Price: <value>\n"
            "   URL: <url>\n"
            "   Source: <domain>\n"
            "4) Sort by start date/time ascending.\n"
            "5) Exclude ads, tours to other cities, and non-events.\n"
            "\nRAW_EVENTS_JSON:\n{raw_json}"
        ),
    },
]
