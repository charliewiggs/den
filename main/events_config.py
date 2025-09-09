# den_social/main/events_config.py
# Lean config for 3-model event discovery + extraction

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# ----------------------------- AREA / WINDOW --------------------------------
AREA = {
    "neighborhood": "Pacific Beach",
    "city": "San Diego",
    "state": "California",
    "timezone": "America/Los_Angeles",
}
TIME_WINDOW_DAYS = 14  # default; can override via env in fetcher

def build_date_window(tz_name: str, days_ahead: int):
    tz = ZoneInfo(tz_name)
    today_local = datetime.now(tz).date()
    start_dt = datetime.combine(today_local, datetime.min.time(), tz)
    end_dt = start_dt + timedelta(days=days_ahead)
    return start_dt, end_dt

# ----------------------------- MODELS / BUDGETS ------------------------------
MODEL_DISCOVERY = "gpt-4o-mini"       # Model 1 & 2 (URL discovery)
MODEL_EXTRACTION = "gpt-4o-mini"      # Model 3 (event extraction)

MAX_DISCOVERY_TOKENS = 1000
MAX_EXTRACTION_TOKENS = 1800

SITES_PER_MODEL = 10                  # Model 1 returns 10, Model 2 returns 10 new
SITES_PER_GROUP = 3                   # Extractor processes 3 pages at a time
MAX_SOURCE_TEXT_CHARS = 60000         # Truncate page text passed to extractor

# ----------------------------- PROMPTS ---------------------------------------
# Model 1: find 10 event websites in the area
MODEL1_SYSTEM = (
    "You are a meticulous local event scout. "
    "Return ONLY a JSON array of URLs to websites that list public, time-bound events "
    "for the specified neighborhood/city/state. Include major aggregators (Eventbrite, Meetup) "
    "AND hyper-local venues: bars, restaurants, community centers, parks, clubs, theaters, "
    "neighborhood associations, libraries, schools, surf/beach orgs, etc. "
    "Exclude social profiles with no event listings. No duplicates. JSON array only."
)
MODEL1_USER_TEMPLATE = """Area:
Neighborhood: {neighborhood}
City: {city}
State: {state}
Timezone: {timezone}
Days ahead: {days_ahead}

Return exactly {sites_per_model} URLs as a JSON array. Favor pages likely to have upcoming events for this area.
"""
def build_model1_user_prompt(area: dict, days_ahead: int) -> str:
    return MODEL1_USER_TEMPLATE.format(
        neighborhood=area["neighborhood"],
        city=area["city"],
        state=area["state"],
        timezone=area["timezone"],
        days_ahead=days_ahead,
        sites_per_model=SITES_PER_MODEL,
    )

# Model 2: find 10 NEW websites (no overlap with Model 1)
MODEL2_SYSTEM = (
    "You are a diligent second-pass scout. Given an existing list of websites, "
    "return ONLY a JSON array of NEW URLs (no overlap) that list public, time-bound events "
    "in the same area. Include hyper-local venues and orgs. JSON array only."
)
MODEL2_USER_TEMPLATE = """Area:
Neighborhood: {neighborhood}
City: {city}
State: {state}
Timezone: {timezone}
Days ahead: {days_ahead}

Existing URLs (do NOT include these again):
{existing_urls_json}

Return exactly {sites_per_model} NEW URLs as a JSON array. Avoid duplicates and off-topic pages.
"""
def build_model2_user_prompt(area: dict, days_ahead: int, existing_urls: list[str]) -> str:
    import json as _json
    return MODEL2_USER_TEMPLATE.format(
        neighborhood=area["neighborhood"],
        city=area["city"],
        state=area["state"],
        timezone=area["timezone"],
        days_ahead=days_ahead,
        existing_urls_json=_json.dumps(existing_urls or [], ensure_ascii=False, indent=2),
        sites_per_model=SITES_PER_MODEL,
    )

# Model 3: extract events from up to 3 pages at a time
MODEL3_SYSTEM = (
    "You extract structured events from raw webpage text. "
    "Only include events INSIDE the specified neighborhood, within the date window. "
    "If uncertain about neighborhood inclusion, omit. Return ONLY a JSON array of events."
)
MODEL3_USER_TEMPLATE = """Extract events strictly inside:
Neighborhood: {neighborhood}
City: {city}
State: {state}
Timezone: {timezone}
Date window: {start_iso} to {end_iso}

Schema (array of objects):
{{
  "name": "string",
  "start_local_iso": "YYYY-MM-DDTHH:MM",
  "end_local_iso": "YYYY-MM-DDTHH:MM or null",
  "venue_name": "string or null",
  "address": "string or null",
  "neighborhood": "{neighborhood}",
  "city": "{city}",
  "state": "{state}",
  "category": "string",
  "price": "string or null",
  "description": "1–3 sentences",
  "source_name": "string",
  "source_url": "https://...",
  "tags": ["optional", "keywords"]
}}

Below are up to 3 pages. If a page has no relevant events in the window, ignore it.

{pages_block}

Return a JSON array only.
"""
def build_pages_block(pages: list[dict]) -> str:
    lines = []
    for idx, p in enumerate(pages, start=1):
        lines.append(f"### PAGE {idx}\nURL: {p['url']}\nTEXT (truncated):\n---\n{p['text']}\n---")
    return "\n\n".join(lines)
