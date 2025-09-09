# den_social/main/fetch_events.py
# Lean 3-model pipeline: discover sites (10 + 10 new) → extract events in batches of 3

import os, json, re
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from openai import OpenAI  # do not modify import or usage

from den_social.main.events_config import (
    AREA, TIME_WINDOW_DAYS,
    MODEL_DISCOVERY, MODEL_EXTRACTION,
    MAX_DISCOVERY_TOKENS, MAX_EXTRACTION_TOKENS,
    SITES_PER_MODEL, SITES_PER_GROUP, MAX_SOURCE_TEXT_CHARS,
    MODEL1_SYSTEM, MODEL2_SYSTEM, MODEL3_SYSTEM,
    build_model1_user_prompt, build_model2_user_prompt,
    MODEL3_USER_TEMPLATE, build_pages_block, build_date_window,
)

# ----------------------------- ENV / CLIENT ----------------------------------
load_dotenv()
openai_api_key = os.environ.get("OPENAI_API_KEY")
if not openai_api_key:
    raise ValueError("Missing OPENAI_API_KEY in environment.")
os.environ["OPENAI_API_KEY"] = openai_api_key
client = OpenAI(api_key=openai_api_key)

# ----------------------------- HELPERS ---------------------------------------
def slugify(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", (text or "").lower()).strip("-")

def extract_json(payload: str):
    s = (payload or "").strip()
    if s.startswith("```"):
        s = re.sub(r"^```(?:json)?\s*", "", s)
        s = re.sub(r"\s*```$", "", s)
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        pass
    m = re.search(r"(\{.*\}|\[.*\])", s, flags=re.DOTALL)
    return json.loads(m.group(1)) if m else (_raise_json())

def _raise_json():
    raise ValueError("Model did not return valid JSON.")

def override_area_from_env(area: dict) -> dict:
    return {
        "neighborhood": os.environ.get("NEIGHBORHOOD", area["neighborhood"]),
        "city": os.environ.get("CITY", area["city"]),
        "state": os.environ.get("STATE", area["state"]),
        "timezone": os.environ.get("TIMEZONE", area["timezone"]),
    }

def normalize_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    if u.startswith("http://") or u.startswith("https://"):
        return u
    if u.startswith("www."):
        return "https://" + u
    return u

def http_get(url: str, timeout=12) -> str:
    headers = {"User-Agent": "Mozilla/5.0 (compatible; den-social-events/1.0)"}
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.text

def html_to_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for t in soup(["script", "style", "noscript", "header", "footer", "nav"]):
        t.decompose()
    text = soup.get_text("\n", strip=True)
    return re.sub(r"\n{3,}", "\n\n", text)[:MAX_SOURCE_TEXT_CHARS]

def dedupe_events(events: list[dict]) -> list[dict]:
    seen, out = set(), []
    for e in events:
        key = (
            (e.get("name") or "").strip().lower(),
            (e.get("start_local_iso") or "").strip().lower(),
            (e.get("venue_name") or "").strip().lower(),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(e)
    return out

# ----------------------------- MODEL CALLS -----------------------------------
def run_model1_discovery(area: dict, days_ahead: int) -> list[str]:
    resp = client.chat.completions.create(
        model=MODEL_DISCOVERY,
        messages=[
            {"role": "system", "content": MODEL1_SYSTEM},
            {"role": "user", "content": build_model1_user_prompt(area, days_ahead)},
        ],
        temperature=0.2,
        max_tokens=MAX_DISCOVERY_TOKENS,
    )
    urls = extract_json(resp.choices[0].message.content)
    if not isinstance(urls, list):
        return []
    norm, seen = [], set()
    for u in urls:
        if not isinstance(u, str): continue
        nu = normalize_url(u)
        if not nu: continue
        low = nu.lower()
        if low in seen: continue
        seen.add(low)
        norm.append(nu)
        if len(norm) >= SITES_PER_MODEL: break
    return norm

def run_model2_discovery(area: dict, days_ahead: int, existing_urls: list[str]) -> list[str]:
    resp = client.chat.completions.create(
        model=MODEL_DISCOVERY,
        messages=[
            {"role": "system", "content": MODEL2_SYSTEM},
            {"role": "user", "content": build_model2_user_prompt(area, days_ahead, existing_urls)},
        ],
        temperature=0.2,
        max_tokens=MAX_DISCOVERY_TOKENS,
    )
    urls = extract_json(resp.choices[0].message.content)
    if not isinstance(urls, list):
        return []
    exist = {u.lower() for u in (existing_urls or [])}
    norm, seen = [], set()
    for u in urls:
        if not isinstance(u, str): continue
        nu = normalize_url(u)
        if not nu: continue
        low = nu.lower()
        if low in exist or low in seen: continue
        seen.add(low)
        norm.append(nu)
        if len(norm) >= SITES_PER_MODEL: break
    return norm

def run_model3_extract_batch(area: dict, start_iso: str, end_iso: str, pages: list[dict]) -> list[dict]:
    user = MODEL3_USER_TEMPLATE.format(
        neighborhood=area["neighborhood"],
        city=area["city"],
        state=area["state"],
        timezone=area["timezone"],
        start_iso=start_iso,
        end_iso=end_iso,
        pages_block=build_pages_block(pages),
    )
    resp = client.chat.completions.create(
        model=MODEL_EXTRACTION,
        messages=[
            {"role": "system", "content": MODEL3_SYSTEM},
            {"role": "user", "content": user},
        ],
        temperature=0.1,
        max_tokens=MAX_EXTRACTION_TOKENS,
    )
    arr = extract_json(resp.choices[0].message.content)
    if not isinstance(arr, list):
        return []
    out = []
    for ev in arr:
        if not isinstance(ev, dict): continue
        ev.setdefault("neighborhood", area["neighborhood"])
        ev.setdefault("city", area["city"])
        ev.setdefault("state", area["state"])
        src_url = ev.get("source_url") or ""
        host = (urlparse(src_url).hostname or "").strip() or "source"
        ev.setdefault("source_name", host)
        out.append(ev)
    return out

# ----------------------------- MAIN -----------------------------------------
def main():
    area = override_area_from_env(AREA)
    # optional TIME_WINDOW_DAYS override via env
    days_ahead = int(os.environ.get("TIME_WINDOW_DAYS", TIME_WINDOW_DAYS))

    start_dt, end_dt = build_date_window(area["timezone"], days_ahead)
    start_iso = start_dt.isoformat(timespec="minutes")
    end_iso = end_dt.isoformat(timespec="minutes")

    # Model 1: 10 sites
    sites1 = run_model1_discovery(area, days_ahead)
    # Model 2: 10 NEW sites
    sites2 = run_model2_discovery(area, days_ahead, existing_urls=sites1)

    # Merge + dedupe, preserve order
    seen, all_sites = set(), []
    for u in sites1 + sites2:
        low = (u or "").lower()
        if low and low not in seen:
            seen.add(low)
            all_sites.append(u)

    # Batch crawl in groups of 3 and extract
    events_all = []
    for i in range(0, len(all_sites), SITES_PER_GROUP):
        batch = all_sites[i:i + SITES_PER_GROUP]
        pages = []
        for url in batch:
            try:
                html = http_get(url)
                text = html_to_text(html)
                if len(text) < 800:  # skip likely empty/JS-only pages
                    continue
                pages.append({"url": url, "text": text})
            except Exception:
                continue
        if not pages:
            continue
        extracted = run_model3_extract_batch(area, start_iso, end_iso, pages)
        if extracted:
            events_all.extend(extracted)

    events = dedupe_events(events_all)

    # Output JSON
    out_dir = Path("data/events")
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M")
    out_path = out_dir / f"events_{slugify(area['neighborhood'])}-{slugify(area['city'])}-{slugify(area['state'])}_{stamp}.json"

    final_json = {
        "area": {
            "neighborhood": area["neighborhood"],
            "city": area["city"],
            "state": area["state"],
            "timezone": area["timezone"],
        },
        "date_window": {"start_local_iso": start_iso, "end_local_iso": end_iso},
        "sources": all_sites,
        "events": events,
    }

    with out_path.open("w", encoding="utf-8") as f:
        json.dump(final_json, f, ensure_ascii=False, indent=2)

    print(f"\nSaved {len(events)} events → {out_path}\n")
    for i, e in enumerate(events[:40], start=1):
        name = e.get("name"); when = e.get("start_local_iso")
        venue = e.get("venue_name") or e.get("address") or "TBD"
        print(f"{i:>2}. {name} | {when} | {venue}")

if __name__ == "__main__":
    main()
