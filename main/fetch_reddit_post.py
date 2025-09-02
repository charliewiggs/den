# den_social/main/fetch_reddit_post.py
import os
import csv
import time
from datetime import datetime, timezone
from typing import Optional
import praw
from pathlib import Path
from dotenv import load_dotenv

# Always load ../.env relative to this file (den_social/.env)
ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=ENV_PATH)  # loads .env into os.environ

def _print_auth_debug():
    # Mask sensitive bits but show what's being used
    def mask(s, keep=4):
        if not s:
            return "<missing>"
        return s[:keep] + "…" if len(s) > keep else "…"
    print("[auth-debug] client_id:", mask(os.getenv("REDDIT_CLIENT_ID")))
    print("[auth-debug] user_agent:", os.getenv("REDDIT_USER_AGENT"))
    print("[auth-debug] username :", os.getenv("REDDIT_USERNAME"))
    print("[auth-debug] has password?", bool(os.getenv("REDDIT_PASSWORD")))

def debug_token_request():
    """
    Direct POST to Reddit OAuth with password grant.
    Prints safe fingerprints (lengths & masked) to catch stray whitespace/typos.
    """
    import requests, requests.auth

    def mask(s, keep=4):
        if s is None: return "<None>"
        s = s.replace("\n","\\n").replace("\r","\\r")
        return (s[:keep] + "…") if len(s) > keep else s

    cid = (os.environ.get("REDDIT_CLIENT_ID") or "").strip()
    sec = (os.environ.get("REDDIT_CLIENT_SECRET") or "").strip()
    ua  = (os.environ.get("REDDIT_USER_AGENT") or "den_social/0.1").strip()
    usr = (os.environ.get("REDDIT_USERNAME") or "").strip()
    pwd = (os.environ.get("REDDIT_PASSWORD") or "").strip()

    print("[token-debug] cid len:", len(cid), "cid:", mask(cid))
    print("[token-debug] sec len:", len(sec), "sec:", mask(sec))
    print("[token-debug] ua     :", ua)
    print("[token-debug] user   :", usr)
    print("[token-debug] pwd len:", len(pwd))

    r = requests.post(
        "https://www.reddit.com/api/v1/access_token",
        auth=requests.auth.HTTPBasicAuth(cid, sec),
        data={"grant_type": "password", "username": usr, "password": pwd, "scope": "identity"},
        headers={"User-Agent": ua},
        timeout=20,
    )
    print("[token-debug] status:", r.status_code)
    print("[token-debug] body  :", r.text)


# ------------ Configuration ------------
SUBREDDITS = [
    "SDSU", "SanDiego", "CollegeBasketball", "CFB", "MountainWest"
]
QUERY = '("SDSU" OR "San Diego State" OR Aztecs)'
TIME_FILTER = "week"      # 'hour' | 'day' | 'week' | 'month' | 'year' | 'all'
LIMIT_PER_SUB = 300       # adjust if you hit rate limits
OUT_CSV = "out/posts.csv"

# ------------ Auth (read from env) ------------
def load_reddit():
    """
    Auth with OAuth2 password grant (requires a 'personal use script' app).
    Enforces Reddit's User-Agent policy and fails fast on misconfig.

    Required env:
      REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET,
      REDDIT_USER_AGENT, REDDIT_USERNAME, REDDIT_PASSWORD
    """
    import praw, prawcore, os

    needed = [
        "REDDIT_CLIENT_ID",
        "REDDIT_CLIENT_SECRET",
        "REDDIT_USER_AGENT",
        "REDDIT_USERNAME",
        "REDDIT_PASSWORD",
    ]
    missing = [k for k in needed if not os.getenv(k)]
    if missing:
        raise RuntimeError("Missing env vars: " + ", ".join(missing))

    ua = os.environ["REDDIT_USER_AGENT"]
    if "(by /u/" not in ua:
        raise RuntimeError(
            "REDDIT_USER_AGENT must follow Reddit's format, e.g. "
            "'macos:den_social.sdsu_scraper:1.0.0 (by /u/FlashyFudge5434)'"
        )

    reddit = praw.Reddit(
        client_id=os.environ["REDDIT_CLIENT_ID"],
        client_secret=os.environ["REDDIT_CLIENT_SECRET"],
        user_agent=ua,
        username=os.environ["REDDIT_USERNAME"],
        password=os.environ["REDDIT_PASSWORD"],
        ratelimit_seconds=10,
    )

    # Touch an auth-required endpoint to fail fast if creds/app type are wrong
    try:
        _ = reddit.user.me()
    except prawcore.exceptions.ResponseException as e:
        raise RuntimeError(
            "OAuth failed (401). Ensure your app is type 'script' and creds are exact."
        ) from e

    reddit.read_only = True  # we only fetch
    return reddit

# ------------ Helpers ------------
def is_image_submission(sub) -> bool:
    """
    Return True for single-image posts.
    Skips galleries, videos, polls, and crossposts to avoid mislabeling.
    """
    if getattr(sub, "crosspost_parent_list", None):
        return False
    if getattr(sub, "is_gallery", False):
        return False
    if getattr(sub, "is_video", False):
        return False
    if getattr(sub, "poll_data", None):
        return False

    # Primary hint from Reddit JSON
    if getattr(sub, "post_hint", None) == "image":
        return True

    # Fallback on URL extension (covers edge cases)
    url = (sub.url or "").lower()
    return url.endswith((".jpg", ".jpeg", ".png", ".webp", ".gif"))

def classify_type(sub) -> Optional[str]:
    """
    Return 'text' | 'image' | 'link', or None if we should skip.
    """
    if getattr(sub, "is_video", False):
        return None
    if getattr(sub, "is_gallery", False):
        return None
    if getattr(sub, "poll_data", None):
        return None
    if getattr(sub, "crosspost_parent_list", None):
        return None

    if getattr(sub, "is_self", False):
        return "text"
    if is_image_submission(sub):
        return "image"
    return "link"

def ensure_outdir(path: str):
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

# ------------ Main ------------
def latest_post_via_public_json(subreddit: str = "SDSU", out_json: str = "out/latest.json"):
    """
    Fetch the most recent post via Reddit's public JSON endpoint (no OAuth).
    Uses a proper User-Agent (required) and outputs the same JSON schema
    as latest_post_to_json for easy QA.

    NOTE: This is read-only and rate-limited more aggressively than OAuth.
    Keep requests minimal and include a descriptive User-Agent.
    """
    import json
    import requests

    ensure_outdir(out_json)

    ua = os.environ.get(
        "REDDIT_USER_AGENT",
        "macos:den_social.sdsu_scraper:1.0.0 (by /u/yourusername)"
    )
    headers = {"User-Agent": ua}

    url = f"https://www.reddit.com/r/{subreddit}/new.json"
    params = {"limit": 1, "raw_json": 1}  # raw_json=1 avoids HTML entity escaping
    resp = requests.get(url, headers=headers, params=params, timeout=20)
    resp.raise_for_status()  # fail fast with a clean traceback if blocked or throttled

    data = resp.json()
    if not data.get("data", {}).get("children"):
        print(f"No posts found in r/{subreddit}")
        return

    d = data["data"]["children"][0]["data"]

    # Match your existing schema
    if d.get("is_self"):
        ptype = "text"
    elif str(d.get("post_hint")) == "image":
        ptype = "image"
    else:
        ptype = "link"

    created_dt = datetime.fromtimestamp(d["created_utc"], tz=timezone.utc)
    row = {
        "id": d["id"],
        "created_utc": int(d["created_utc"]),
        "created_iso": created_dt.isoformat(),
        "subreddit": d.get("subreddit", subreddit),
        "author": d.get("author") or "[deleted]",
        "title": d.get("title", ""),
        "permalink": f"https://www.reddit.com{d.get('permalink','')}",
        "url": d.get("url"),
        "post_type": ptype,
        "score": d.get("score", 0),
        "num_comments": d.get("num_comments", 0),
        "selftext": d.get("selftext", "") if ptype == "text" else "",
    }

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(row, f, ensure_ascii=False, indent=2)

    print(f"[public] Wrote latest post from r/{subreddit} to {out_json}")

def main():
    reddit = load_reddit()
    ensure_outdir(OUT_CSV)

    fieldnames = [
        "id", "created_utc", "created_iso", "subreddit", "author",
        "title", "permalink", "url", "post_type", "score", "num_comments",
        "selftext"
    ]

    seen_ids = set()
    count_written = 0

    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for sr in SUBREDDITS:
            print(f"Searching r/{sr} …", flush=True)
            results = reddit.subreddit(sr).search(
                QUERY, sort="new", time_filter=TIME_FILTER, limit=LIMIT_PER_SUB
            )
            for sub in results:
                try:
                    if sub.id in seen_ids:
                        continue
                    ptype = classify_type(sub)
                    if ptype is None:
                        continue

                    created_dt = datetime.fromtimestamp(sub.created_utc, tz=timezone.utc)
                    row = {
                        "id": sub.id,
                        "created_utc": int(sub.created_utc),
                        "created_iso": created_dt.isoformat(),
                        "subreddit": str(sub.subreddit),
                        "author": str(sub.author) if sub.author else "[deleted]",
                        "title": sub.title,
                        "permalink": f"https://www.reddit.com{sub.permalink}",
                        "url": sub.url,
                        "post_type": ptype,
                        "score": sub.score,
                        "num_comments": sub.num_comments,
                        "selftext": sub.selftext if ptype == "text" else "",
                    }
                    writer.writerow(row)
                    seen_ids.add(sub.id)
                    count_written += 1
                except Exception as e:
                    print(f"Skipping {getattr(sub, 'id', '?')}: {e}", flush=True)
                    time.sleep(0.25)

    print(f"Done. Wrote {count_written} rows to {OUT_CSV}")

if __name__ == "__main__":
    main()
