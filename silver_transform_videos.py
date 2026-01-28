import json
from pathlib import Path
from datetime import date

# 1) CHANGE THIS to your downloaded bronze file path (the latest videos_raw.json)
INPUT_FILE = Path(r"C:\Users\madha\Downloads\videos_raw (1).json")

# 2) Output folder (will be created)
INGEST_DATE = date.today().isoformat()
OUTPUT_FILE = Path("silver") / "youtube" / "videos" / f"ingest_date={INGEST_DATE}" / "videos_clean.json"

def safe_int(x):
    try:
        return int(x)
    except Exception:
        return None

def main():
    data = json.loads(INPUT_FILE.read_text(encoding="utf-8"))
    items = data.get("items", [])

    cleaned = []
    for v in items:
        vid = v.get("id")
        snippet = v.get("snippet", {}) or {}
        stats = v.get("statistics", {}) or {}
        content = v.get("contentDetails", {}) or {}

        cleaned.append({
            "video_id": vid,
            "title": snippet.get("title"),
            "published_at": snippet.get("publishedAt"),
            "channel_title": snippet.get("channelTitle"),
            "duration": content.get("duration"),
            "view_count": safe_int(stats.get("viewCount")),
            "like_count": safe_int(stats.get("likeCount")),
            "comment_count": safe_int(stats.get("commentCount")),
        })

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(cleaned, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"✅ Wrote {len(cleaned)} rows to: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
