import os
import json
import time
from pathlib import Path
from datetime import datetime, timezone

import requests

# ---- Helpers ---------------------------------------------------------------

def ensure_outdir(path: str):
    d = Path(path).parent
    d.mkdir(parents=True, exist_ok=True)

def _row_from_listing_item(d: dict) -> dict:
    """Map reddit listing item -> our compact row schema."""
    if d.get("is_self"):
        ptype = "text"
    elif str(d.get("post_hint")) == "image":
        ptype = "image"
    else:
        ptype = "link"

    created_dt = datetime.fromtimestamp(d["created_utc"], tz=timezone.utc)
    return {
        "id": d["id"],
        "created_utc": int(d["created_utc"]),
        "created_iso": created_dt.isoformat(),
        "subreddit": d.get("subreddit", ""),
        "author": d.get("author") or "[deleted]",
        "title": d.get("title", ""),
        "permalink": f"https://www.reddit.com{d.get('permalink','')}",
        "url": d.get("url"),
        "post_type": ptype,
        "score": d.get("score", 0),
        "num_comments": d.get("num_comments", 0),
        "selftext": d.get("selftext", "") if ptype == "text" else "",
    }

def _fetch_new_public(subreddit: str, limit: int, ua: str) -> list[dict]:
    """Fetch up to `limit` newest posts via public JSON, with pagination."""
    url = f"https://www.reddit.com/r/{subreddit}/new.json"
    headers = {"User-Agent": ua}
    out, after = [], None

    while len(out) < limit:
        remaining = limit - len(out)
        params = {
            "limit": min(100, remaining),
            "raw_json": 1,
        }
        if after:
            params["after"] = after

        r = requests.get(url, headers=headers, params=params, timeout=20)
        r.raise_for_status()
        j = r.json()
        children = j.get("data", {}).get("children", [])
        if not children:
            break

        for c in children:
            out.append(_row_from_listing_item(c["data"]))
            if len(out) >= limit:
                break

        after = j.get("data", {}).get("after")
        if not after:
            break

        # Be polite to public endpoints.
        time.sleep(0.5)

    # Sort newest -> oldest to be clearly chronological in output
    out.sort(key=lambda x: x["created_utc"], reverse=True)
    return out

# ---- Public API ------------------------------------------------------------

def fetch_posts_from_config():
    """
    Read subreddits + count from reddit_config and write a single JSON array
    to OUT_JSON with posts from all subreddits (newest -> oldest within each).
    """
    from den_social.main.reddit_config import (
        SUBREDDITS,
        POSTS_PER_SUBREDDIT,
        OUT_JSON,
    )

    ua = os.environ.get(
        "REDDIT_USER_AGENT",
        "macos:den_social.sdsu_scraper:1.0.0 (by /u/yourusername)",
    )

    all_rows = []
    for sr in SUBREDDITS:
        rows = _fetch_new_public(sr, POSTS_PER_SUBREDDIT, ua)
        all_rows.extend(rows)

    # Optionally sort across subs too (keep output globally chronological)
    all_rows.sort(key=lambda x: x["created_utc"], reverse=True)

    ensure_outdir(OUT_JSON)
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(all_rows, f, ensure_ascii=False, indent=2)

    print(f"[public] Wrote {len(all_rows)} posts to {OUT_JSON}")

def latest_post_via_public_json(subreddit: str = "SDSU", out_json: str = "out/latest.json"):
    """Quick QA helper: write only the latest post from one subreddit."""
    ua = os.environ.get(
        "REDDIT_USER_AGENT",
        "macos:den_social.sdsu_scraper:1.0.0 (by /u/yourusername)",
    )
    rows = _fetch_new_public(subreddit, 1, ua)
    if not rows:
        print(f"No posts found in r/{subreddit}")
        return

    ensure_outdir(out_json)
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(rows[0], f, ensure_ascii=False, indent=2)
    print(f"[public] Wrote latest post from r/{subreddit} to {out_json}")

# ---- CLI ------------------------------------------------------------------

if __name__ == "__main__":
    # `python -m den_social.main.fetch_reddit_post`
    fetch_posts_from_config()
