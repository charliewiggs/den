# den_social/main/fetch_events.py
import os
import json
import math
import time
import re
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from dotenv import load_dotenv
load_dotenv()

# ---- OpenAI client (exactly your pattern) ----
from openai import OpenAI  # Do not modify how the client is imported or used

openai_api_key = os.environ.get("OPENAI_API_KEY")
if openai_api_key:
    os.environ["OPENAI_API_KEY"] = openai_api_key
client = OpenAI(api_key=openai_api_key) if openai_api_key else None

# ---- Local config ----
try:
    from den_social.main.events_config import (
        NEIGHBORHOOD, CITY, STATE, COUNTRY, TIMEZONE,
        CENTER_LAT, CENTER_LON, RADIUS_MILES,
        REQUEST_TIMEOUT_S, MAX_SEED_PAGES, MAX_FOLLOW_PER_SEED, MAX_EVENTS,
        FUTURE_DAYS_LIMIT, OUTPUT_TXT_PATH, OUTPUT_RAW_JSONL_PATH,
        USE_OPENAI_FORMATTING, SOURCE_PAGES,
        OPENAI_MODEL, MAX_TOTAL_TOKENS, DESIRED_OUTPUT_TOKENS,
        events_prompt,
    )
except Exception as e:
    raise RuntimeError(f"Failed to import events_config: {e}")

# ---- Optional zone handling ----
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
    _tz = ZoneInfo(TIMEZONE)
except Exception:
    # Fallback to naive local times if zoneinfo not present
    _tz = None

UA = os.getenv(
    "EVENTS_USER_AGENT",
    "macos:den_social.events:1.0.0 (+https://example.com) (by /u/yourusername)"
)

@dataclass
class Event:
    title: str
    start_dt: Optional[str]     # ISO 8601
    end_dt: Optional[str]       # ISO 8601
    venue: Optional[str]
    address: Optional[str]
    price: Optional[str]
    url: Optional[str]
    source: Optional[str]
    lat: Optional[float] = None
    lon: Optional[float] = None
    raw_source_url: Optional[str] = None

# ---------------------------
# HTTP helpers
# ---------------------------
def fetch(url: str, timeout: int = REQUEST_TIMEOUT_S) -> Optional[str]:
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": UA, "Accept": "text/html,application/xhtml+xml"},
            timeout=timeout,
        )
        if resp.status_code >= 400:
            return None
        return resp.text
    except Exception:
        return None

# ---------------------------
# JSON-LD extraction
# ---------------------------
def _json_loads(text: str) -> Optional[Any]:
    try:
        return json.loads(text)
    except Exception:
        # some sites wrap multiple JSON-LD objects back-to-back; try array salvage
        text2 = text.strip()
        if text2 and not text2.startswith("[") and not text2.endswith("]"):
            try:
                return json.loads(f"[{text2}]")
            except Exception:
                return None
        return None

def extract_jsonld_objects(html: str) -> List[Any]:
    soup = BeautifulSoup(html, "html.parser")
    out: List[Any] = []
    for tag in soup.find_all("script", {"type": "application/ld+json"}):
        if not tag.string:
            # sometimes contents are within text, not .string
            content = tag.get_text() or ""
        else:
            content = tag.string
        data = _json_loads(content)
        if data is None:
            continue
        if isinstance(data, list):
            out.extend(data)
        else:
            out.append(data)
    return out

def _iter_nodes(obj: Any):
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _iter_nodes(v)
    elif isinstance(obj, list):
        for it in obj:
            yield from _iter_nodes(it)

def _type_contains(node: Dict[str, Any], target: str) -> bool:
    t = node.get("@type")
    if not t:
        return False
    if isinstance(t, str):
        return t.lower() == target.lower()
    if isinstance(t, list):
        return any(isinstance(x, str) and x.lower() == target.lower() for x in t)
    return False

def discover_event_links_from_jsonld(objs: List[Any], base_url: str, limit: int) -> List[str]:
    """
    Find links to event detail pages from ItemList or SearchResultsPage JSON-LD.
    """
    links: List[str] = []
    for obj in objs:
        for node in _iter_nodes(obj):
            if _type_contains(node, "ItemList") or _type_contains(node, "SearchResultsPage"):
                items = node.get("itemListElement") or []
                for it in items:
                    url = None
                    if isinstance(it, dict):
                        # ItemList can have url directly or nested under 'item'
                        url = it.get("url")
                        if not url and isinstance(it.get("item"), dict):
                            url = it["item"].get("url") or it["item"].get("@id")
                    if url:
                        links.append(urljoin(base_url, url))
    # de-dup in order
    seen = set()
    ordered = []
    for u in links:
        if u not in seen:
            ordered.append(u)
            seen.add(u)
        if len(ordered) >= limit:
            break
    return ordered

def _take_first(*vals) -> Optional[str]:
    for v in vals:
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None

def parse_events_from_jsonld(objs: List[Any], raw_source_url: str) -> List[Event]:
    events: List[Event] = []
    for obj in objs:
        for node in _iter_nodes(obj):
            if not _type_contains(node, "Event"):
                continue
            # Fields per https://schema.org/Event
            name = node.get("name")
            start = node.get("startDate") or node.get("startTime")
            end = node.get("endDate") or node.get("endTime")
            url = node.get("url") or node.get("@id")
            source_domain = urlparse(url or raw_source_url).netloc or urlparse(raw_source_url).netloc

            # location may be object or text
            loc = node.get("location")
            venue = None
            address_text = None
            lat = lon = None

            if isinstance(loc, dict):
                venue = _take_first(loc.get("name"), (loc.get("address") or {}).get("name"))
                address = loc.get("address")
                if isinstance(address, dict):
                    address_text = ", ".join([
                        x for x in [
                            address.get("streetAddress"),
                            address.get("addressLocality"),
                            address.get("addressRegion"),
                            address.get("postalCode"),
                        ] if x
                    ]) or None
                elif isinstance(address, str):
                    address_text = address

                # geo may be nested
                geo = loc.get("geo") or {}
                try:
                    lat = float(geo.get("latitude")) if geo.get("latitude") is not None else None
                    lon = float(geo.get("longitude")) if geo.get("longitude") is not None else None
                except Exception:
                    lat = lon = None
            elif isinstance(loc, str):
                venue = None
                address_text = loc

            # price
            offers = node.get("offers")
            price = None
            if isinstance(offers, dict):
                price = _take_first(
                    offers.get("priceCurrency") and f"{offers.get('priceCurrency')} {offers.get('price')}",
                    offers.get("price"),
                    offers.get("description"),
                )
            elif isinstance(offers, list):
                # take first reasonable
                for off in offers:
                    if isinstance(off, dict):
                        price = _take_first(
                            off.get("priceCurrency") and f"{off.get('priceCurrency')} {off.get('price')}",
                            off.get("price"),
                            off.get("description"),
                        )
                        if price:
                            break

            ev = Event(
                title=name or "",
                start_dt=str(start) if start else None,
                end_dt=str(end) if end else None,
                venue=venue,
                address=address_text,
                price=str(price) if price is not None else None,
                url=url if url else raw_source_url,
                source=source_domain or None,
                lat=lat,
                lon=lon,
                raw_source_url=raw_source_url,
            )
            if ev.title:
                events.append(ev)
    return events

# ---------------------------
# Filters & utilities
# ---------------------------
def haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 3958.8  # miles
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = phi2 - phi1
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dl/2)**2
    return 2 * R * math.asin(math.sqrt(a))

def in_geofence(ev: Event) -> bool:
    if ev.lat is not None and ev.lon is not None:
        try:
            return haversine_miles(CENTER_LAT, CENTER_LON, ev.lat, ev.lon) <= RADIUS_MILES
        except Exception:
            return True  # if coordinates malformed, don't toss it here
    # Fallback heuristic on text (looser)
    text = " ".join(filter(None, [ev.venue or "", ev.address or ""])).lower()
    neigh_ok = NEIGHBORHOOD.lower() in text
    city_ok = CITY.lower() in text
    # If we can’t confirm neighborhood, keep San Diego events; OpenAI can trim later.
    return neigh_ok or city_ok

_dt_re = re.compile(r"^\d{4}-\d{2}-\d{2}")

def parse_iso_to_local(iso_val: Optional[str]) -> Optional[datetime]:
    if not iso_val:
        return None
    try:
        # common cases: full ISO or date-only
        # try fromisoformat first
        dt = None
        try:
            dt = datetime.fromisoformat(iso_val.replace("Z", "+00:00"))
        except Exception:
            pass
        if dt is None and _dt_re.match(iso_val):
            # date-only "YYYY-MM-DD"
            dt = datetime.fromisoformat(iso_val + "T00:00:00")
        if dt is None:
            # last resort: lenient parse
            from dateutil.parser import parse
            dt = parse(iso_val)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        if _tz:
            dt = dt.astimezone(_tz)
        return dt
    except Exception:
        return None

def within_future_window(ev: Event) -> bool:
    start = parse_iso_to_local(ev.start_dt)
    if not start:
        return True  # keep unknown; OpenAI can drop later if needed
    now = datetime.now(_tz) if _tz else datetime.now()
    return start >= (now - timedelta(days=1)) and start <= now + timedelta(days=FUTURE_DAYS_LIMIT)

def dedupe_events(events: List[Event]) -> List[Event]:
    def key(ev: Event) -> Tuple[str, str]:
        # coarse key: title (lower) + date (YYYY-MM-DD)
        start = parse_iso_to_local(ev.start_dt)
        datepart = start.strftime("%Y-%m-%d") if start else ""
        return (ev.title.strip().lower(), datepart)

    seen = {}
    for ev in events:
        k = key(ev)
        if k not in seen:
            seen[k] = ev
            continue
        # prefer the one with more fields filled
        def score(e: Event) -> int:
            return sum(1 for x in [e.venue, e.address, e.price, e.url, e.source, e.lat, e.lon, e.end_dt] if x)
        if score(ev) > score(seen[k]):
            seen[k] = ev
    return list(seen.values())

def sort_events(events: List[Event]) -> List[Event]:
    def sort_key(ev: Event):
        dt = parse_iso_to_local(ev.start_dt)
        return (dt or datetime.max, ev.title.lower())
    return sorted(events, key=sort_key)

# ---------------------------
# Output helpers
# ---------------------------
def write_raw_jsonl(events: List[Event], path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for ev in events:
            f.write(json.dumps(asdict(ev), ensure_ascii=False) + "\n")

def to_human_line(ev: Event) -> str:
    start = parse_iso_to_local(ev.start_dt)
    end = parse_iso_to_local(ev.end_dt)
    def fmt(dt: Optional[datetime]) -> str:
        if not dt:
            return ""
        # Mon, Sep 9, 2025 — 7:30 PM
        return dt.strftime("%a, %b %-d, %Y") if os.name != "nt" else dt.strftime("%a, %b %#d, %Y")
    def fmt_time(dt: Optional[datetime]) -> str:
        if not dt:
            return ""
        return dt.strftime("%-I:%M %p") if os.name != "nt" else dt.strftime("%#I:%M %p")

    date_str = fmt(start)
    time_str = fmt_time(start)
    venue_line = " · ".join([x for x in [ev.venue, ev.address] if x])
    price_str = ev.price or ""
    lines = [
        ev.title or "",
        " — ".join([x for x in [date_str, time_str] if x]),
        venue_line,
        f"Price: {price_str}".strip(),
        f"URL: {ev.url or ''}",
        f"Source: {ev.source or ''}",
    ]
    return "\n".join([ln for ln in lines if ln]).strip()

def write_txt(events: List[Event], path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for i, ev in enumerate(events):
            if i:
                f.write("\n\n")
            f.write(to_human_line(ev))

# ---------------------------
# OpenAI post-processing
# ---------------------------
def openai_format_events(events: List[Event]) -> str:
    if not client:
        # If no API key present, fall back to local formatting
        return "\n\n".join(to_human_line(ev) for ev in events)

    raw_json = json.dumps([asdict(e) for e in events], ensure_ascii=False)
    prompt_msgs = []
    for msg in events_prompt:
        if msg["role"] == "user":
            prompt_msgs.append({
                "role": "user",
                "content": msg["content"].format(
                    neighborhood=NEIGHBORHOOD,
                    city=CITY,
                    state=STATE,
                    timezone=TIMEZONE,
                    future_days=FUTURE_DAYS_LIMIT,
                    raw_json=raw_json,
                )
            })
        else:
            prompt_msgs.append(msg)

    # crude token budgeting akin to your pattern
    # (we're not counting input tokens precisely here; the message size is modest)
    resp = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=prompt_msgs,
        temperature=0.2,
        max_tokens=DESIRED_OUTPUT_TOKENS,
    )
    content = resp.choices[0].message.content.strip()
    content = content.replace("**", "").replace("\"", "").replace("*", "")
    return content

# ---------------------------
# Main crawl routine
# ---------------------------
def crawl_events() -> List[Event]:
    seeds = SOURCE_PAGES[:MAX_SEED_PAGES]
    all_events: List[Event] = []

    for seed in seeds:
        html = fetch(seed)
        if not html:
            continue

        objs = extract_jsonld_objects(html)
        # parse events directly on the seed page
        direct = parse_events_from_jsonld(objs, raw_source_url=seed)
        all_events.extend(direct)

        # If it looks like a listing page, discover detail links and follow some
        links = discover_event_links_from_jsonld(objs, base_url=seed, limit=MAX_FOLLOW_PER_SEED)
        for link in links:
            html2 = fetch(link)
            if not html2:
                continue
            objs2 = extract_jsonld_objects(html2)
            evs = parse_events_from_jsonld(objs2, raw_source_url=seed)
            all_events.extend(evs)

        # be polite
        time.sleep(0.25)

        if len(all_events) >= MAX_EVENTS:
            break

    # Basic sanitization & filtering
    # 1) keep plausible in-area events
    filtered = [ev for ev in all_events if in_geofence(ev)]
    # 2) within future window
    filtered = [ev for ev in filtered if within_future_window(ev)]
    # 3) dedupe + sort
    filtered = dedupe_events(filtered)
    filtered = sort_events(filtered)
    # cap
    if len(filtered) > MAX_EVENTS:
        filtered = filtered[:MAX_EVENTS]
    return filtered

def main():
    events = crawl_events()

    # Always save raw JSONL (debug/trace)
    try:
        write_raw_jsonl(events, OUTPUT_RAW_JSONL_PATH)
        print(f"[ok] Wrote raw JSONL: {OUTPUT_RAW_JSONL_PATH} ({len(events)} events)")
    except Exception as e:
        print(f"[warn] Could not write JSONL: {e}")

    # Format
    if USE_OPENAI_FORMATTING:
        try:
            formatted = openai_format_events(events)
            with open(OUTPUT_TXT_PATH, "w", encoding="utf-8") as f:
                f.write(formatted)
            print(f"[ok] Wrote formatted TXT via OpenAI: {OUTPUT_TXT_PATH}")
            return
        except Exception as e:
            print(f"[warn] OpenAI formatting failed; falling back to local format: {e}")

    # Local formatting fallback
    write_txt(events, OUTPUT_TXT_PATH)
    print(f"[ok] Wrote TXT: {OUTPUT_TXT_PATH}")

if __name__ == "__main__":
    main()
