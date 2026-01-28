import os, json
from datetime import date
from azure.storage.blob import BlobServiceClient

def env(name):
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v

def blob(container, path):
    svc = BlobServiceClient.from_connection_string(env("AZURE_STORAGE_CONNECTION_STRING"))
    return svc.get_blob_client(container=container, blob=path)

def read_json(container, path):
    data = blob(container, path).download_blob().readall()
    return json.loads(data)

def write_json(container, path, payload):
    blob(container, path).upload_blob(
        json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
        overwrite=True
    )

def main():
    ingest_date = date.today().isoformat()

    videos_path = f"youtube/videos/ingest_date={ingest_date}/videos_with_sentiment.json"
    comments_path = f"youtube/comments/ingest_date={ingest_date}/comments_with_sentiment.json"
    videos = read_json("gold", videos_path)
    comments = read_json("gold", comments_path)
    # handle both list-based and dict-based files
    v_items = videos if isinstance(videos, list) else videos.get("items", [])
    c_items = comments if isinstance(comments, list) else comments.get("items", [])

    # simple KPIs
    total_videos = len(v_items)
    total_comments = len(c_items)

    # count sentiment categories if present
    def count_sent(items):
        out = {}
        for it in items:
            s = (it.get("sentiment") or "unknown").lower()
            out[s] = out.get(s, 0) + 1
        return out

    payload = {
        "ingest_date": ingest_date,
        "total_videos": total_videos,
        "total_comments": total_comments,
        "video_sentiment_counts": count_sent(v_items),
        "comment_sentiment_counts": count_sent(c_items),
        "generated_at_utc": __import__("datetime").datetime.utcnow().isoformat() + "Z"
    }

    out_path = f"youtube/final/ingest_date={ingest_date}/kpis.json"
    write_json("gold", out_path, payload)
    print(f"OK - Wrote final KPIs to gold/{out_path}")

if __name__ == "__main__":
    main()
