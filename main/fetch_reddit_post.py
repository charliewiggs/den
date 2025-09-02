# den_social/main/fetch_reddit_post.py
import os
from datetime import datetime, timezone
from typing import Dict, Any, List

import requests
import pymysql

# --------- Config (import your simple knobs) ----------
try:
    from den_social.main.reddit_config import SUBREDDITS, POST_LIMIT
except Exception:
    SUBREDDITS = ["SDSU"]
    POST_LIMIT = 25

# --------- Helpers ----------
def _ua() -> str:
    """Reddit requires a descriptive User-Agent."""
    return os.environ.get(
        "REDDIT_USER_AGENT",
        "macos:den_social.scraper:1.0.0 (by /u/yourusername)"
    )

def _extract_images_from_gallery(d: Dict[str, Any]) -> List[str]:
    """Collect image URLs for gallery posts."""
    images: List[str] = []
    gallery = d.get("gallery_data") or {}
    media = d.get("media_metadata") or {}
    for it in gallery.get("items", []) or []:
        mid = it.get("media_id")
        if not mid:
            continue
        meta = media.get(mid) or {}
        src = (meta.get("s") or {}).get("u")
        if not src:
            previews = meta.get("p") or []
            if previews:
                src = previews[-1].get("u")
        if src:
            images.append(src.replace("&amp;", "&"))
    return images

def _guess_post_type(d: Dict[str, Any]) -> str:
    """Return one of: 'text' | 'image' | 'link'."""
    if d.get("is_self"):
        return "text"
    if d.get("is_gallery"):
        return "image"
    if str(d.get("post_hint")) == "image":
        return "image"
    return "link"

def _serialize_listing_post(d: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize one listing child into a DB-friendly dict."""
    post_type = _guess_post_type(d)
    images: List[str] = []
    if d.get("is_gallery"):
        images = _extract_images_from_gallery(d)
        post_type = "image"
    elif str(d.get("post_hint")) == "image" and d.get("url"):
        images = [d["url"]]
        post_type = "image"
    else:
        url = (d.get("url") or "").lower()
        if url.endswith((".jpg", ".jpeg", ".png", ".webp", ".gif")):
            images = [d.get("url")]
            post_type = "image"

    created_dt = datetime.fromtimestamp(d["created_utc"], tz=timezone.utc)
    return {
        "id": d["id"],
        "created_utc": int(d["created_utc"]),
        "created_iso": created_dt.isoformat(),
        "subreddit": d.get("subreddit"),
        "author": d.get("author") or "[deleted]",
        "title": d.get("title") or "",
        "permalink": f"https://www.reddit.com{d.get('permalink','')}",
        "url": d.get("url"),
        "post_type": post_type,
        "score": d.get("score", 0),
        "num_comments": d.get("num_comments", 0),
        "selftext": d.get("selftext", "") if post_type == "text" else "",
        "images": images,
    }

# --------- DB helpers (lean, no SSL) ----------
def _db_params():
    """Read connection settings from env with safe defaults."""
    return {
        "host": os.getenv("DB_HOST", "localhost"),
        "user": os.getenv("DB_USER", "admin"),
        "password": os.getenv("DB_PASS", ""),
        "database": os.getenv("DB_NAME", "densocial"),
        "port": int(os.getenv("DB_PORT", "3306")),
        "charset": "utf8mb4",
        "autocommit": True,
        "cursorclass": pymysql.cursors.Cursor,
    }

def _db_conn():
    return pymysql.connect(**_db_params())

def _db_upsert_posts(rows: List[Dict[str, Any]]) -> int:
    """
    Upsert normalized rows into reddit_posts.
    posted='no' on first insert; not overwritten on updates.
    """
    if not rows:
        return 0

    insert_sql = """
    INSERT INTO reddit_posts (
      reddit_id, subreddit, author, title, selftext, url, permalink, post_type,
      score, num_comments, created_utc, created_at,
      image_1_url, image_2_url, image_3_url, image_4_url, image_5_url,
      posted
    ) VALUES (
      %(reddit_id)s, %(subreddit)s, %(author)s, %(title)s, %(selftext)s, %(url)s, %(permalink)s, %(post_type)s,
      %(score)s, %(num_comments)s, %(created_utc)s, %(created_at)s,
      %(image_1_url)s, %(image_2_url)s, %(image_3_url)s, %(image_4_url)s, %(image_5_url)s,
      'no'
    )
    ON DUPLICATE KEY UPDATE
      subreddit=VALUES(subreddit),
      author=VALUES(author),
      title=VALUES(title),
      selftext=VALUES(selftext),
      url=VALUES(url),
      permalink=VALUES(permalink),
      post_type=VALUES(post_type),
      score=VALUES(score),
      num_comments=VALUES(num_comments),
      created_utc=VALUES(created_utc),
      created_at=VALUES(created_at),
      image_1_url=VALUES(image_1_url),
      image_2_url=VALUES(image_2_url),
      image_3_url=VALUES(image_3_url),
      image_4_url=VALUES(image_4_url),
      image_5_url=VALUES(image_5_url);
    """

    def _prep(row: Dict[str, Any]) -> Dict[str, Any]:
        imgs = row.get("images") or []
        return {
            "reddit_id": row["id"],
            "subreddit": row.get("subreddit"),
            "author": row.get("author"),
            "title": row.get("title"),
            "selftext": row.get("selftext") or None,
            "url": row.get("url"),
            "permalink": row.get("permalink"),
            "post_type": row.get("post_type"),
            "score": int(row.get("score", 0)),
            "num_comments": int(row.get("num_comments", 0)),
            "created_utc": int(row["created_utc"]),
            "created_at": datetime.utcfromtimestamp(int(row["created_utc"])).strftime("%Y-%m-%d %H:%M:%S"),
            "image_1_url": imgs[0] if len(imgs) > 0 else None,
            "image_2_url": imgs[1] if len(imgs) > 1 else None,
            "image_3_url": imgs[2] if len(imgs) > 2 else None,
            "image_4_url": imgs[3] if len(imgs) > 3 else None,
            "image_5_url": imgs[4] if len(imgs) > 4 else None,
        }

    conn = _db_conn()
    try:
        with conn.cursor() as cur:
            cur.executemany(insert_sql, [_prep(r) for r in rows])
        return len(rows)
    finally:
        conn.close()

# --------- Fetch → upsert ----------
def fetch_posts_via_public_json(
    subreddits: List[str] = None,
    limit_per_sub: int = None,
):
    """
    Fetch newest posts for each subreddit and upsert directly into MySQL.
    """
    if subreddits is None:
        subreddits = SUBREDDITS
    if limit_per_sub is None:
        limit_per_sub = POST_LIMIT

    headers = {"User-Agent": _ua()}
    all_rows: List[Dict[str, Any]] = []

    for sr in subreddits:
        resp = requests.get(
            f"https://www.reddit.com/r/{sr}/new.json",
            headers=headers,
            params={"limit": limit_per_sub, "raw_json": 1},
            timeout=20,
        )
        resp.raise_for_status()
        payload = resp.json()
        children = (payload.get("data") or {}).get("children") or []
        for c in children:
            d = c.get("data") or {}
            all_rows.append(_serialize_listing_post(d))

    all_rows.sort(key=lambda r: r["created_utc"], reverse=True)
    upserted = _db_upsert_posts(all_rows)
    print(f"[db] Upserted {upserted} post(s) into reddit_posts")

if __name__ == "__main__":
    fetch_posts_via_public_json()
