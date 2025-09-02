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

# ------------ Configuration ------------
SUBREDDITS = [
    "SDSU", "SanDiego", "CollegeBasketball", "CFB", "MountainWest"
]
QUERY = '("SDSU" OR "San Diego State" OR Aztecs)'
TIME_FILTER = "week"      # 'hour' | 'day' | 'week' | 'month' | 'year' | 'all'
LIMIT_PER_SUB = 300       # adjust if you hit rate limits
OUT_CSV = "out/posts.csv"

# ------------ Auth (read from env) ------------
# ------------ Auth (read from env) ------------
def load_reddit():
    """
    Prefer script/password grant when username/password are provided (works for Script apps).
    Otherwise, fall back to client credentials (read-only) for supported app types.
    Requires: REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT.
    """
    import prawcore
    required = ["REDDIT_CLIENT_ID", "REDDIT_CLIENT_SECRET", "REDDIT_USER_AGENT"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise RuntimeError(
            "Missing env vars: " + ", ".join(missing) +
            "\nSet them before running."
        )

    common_kwargs = dict(
        client_id=os.environ["REDDIT_CLIENT_ID"],
        client_secret=os.environ["REDDIT_CLIENT_SECRET"],
        user_agent=os.environ["REDDIT_USER_AGENT"],
        ratelimit_seconds=10,
    )

    username = os.getenv("REDDIT_USERNAME")
    password = os.getenv("REDDIT_PASSWORD")

    # Script/password grant (DO NOT set read_only here)
    if username and password:
        reddit = praw.Reddit(
            username=username,
            password=password,
            **common_kwargs,
        )
        # leave reddit.read_only as-is (script authorizer)
        return reddit

    # Fallback: client-credentials (read-only)
    try:
        reddit = praw.Reddit(**common_kwargs)
        reddit.read_only = True  # only here
        # Force token fetch now (fail fast if 401)
        _ = reddit.user.me()
        return reddit
    except prawcore.exceptions.ResponseException as e:
        raise RuntimeError(
            "Reddit auth failed with client-credentials (likely 401). "
            "Provide REDDIT_USERNAME and REDDIT_PASSWORD for a Script app, "
            "or ensure your app type supports client credentials."
        ) from e

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
def latest_post_to_json(subreddit: str = "SDSU", out_json: str = "out/latest.json"):
    """
    Fetch the most recent eligible post from a subreddit and write a JSON package
    matching the existing CSV row schema. Keeps your current typing/filters.
    """
    import json

    reddit = load_reddit()
    ensure_outdir(out_json)

    # Grab newest posts and return the first one that passes your classifier
    for sub in reddit.subreddit(subreddit).new(limit=10):
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

        with open(out_json, "w", encoding="utf-8") as f:
            json.dump(row, f, ensure_ascii=False, indent=2)

        print(f"Wrote latest post from r/{subreddit} to {out_json}")
        return

    print(f"No eligible posts found in r/{subreddit}")

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
