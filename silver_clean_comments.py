import os, json
from datetime import date
from azure.storage.blob import BlobServiceClient

def env(name):
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v

def blob_client(container, path):
    svc = BlobServiceClient.from_connection_string(
        env("AZURE_STORAGE_CONNECTION_STRING")
    )
    return svc.get_blob_client(container=container, blob=path)

def main():
    ingest_date = date.today().isoformat()

    # containers
    BRONZE_CONTAINER = "bronze"
    SILVER_CONTAINER = "silver"

    # paths
    src_path = f"youtube/comments/ingest_date={ingest_date}/comments_raw.json"
    dst_path = f"youtube/comments/ingest_date={ingest_date}/comments_clean.json"

    # -------------------------
    # READ FROM BRONZE
    # -------------------------
    raw_bytes = blob_client(
        BRONZE_CONTAINER, src_path
    ).download_blob().readall()

    raw = json.loads(raw_bytes)

    # -------------------------
    # CLEAN DATA
    # -------------------------
    cleaned = []
    for c in raw.get("items", []):
        if "error" in c:
            continue

        text = (c.get("text") or "").strip()
        if not text:
            continue

        cleaned.append({
            "videoId": c.get("videoId"),
            "commentId": c.get("commentId"),
            "author": c.get("author"),
            "text": text,
            "likes": c.get("likes", 0),
            "publishedAt": c.get("publishedAt")
        })

    out = {
        "ingest_date": ingest_date,
        "rows": len(cleaned),
        "items": cleaned
    }

    # -------------------------
    # WRITE TO SILVER
    # -------------------------
    blob_client(
        SILVER_CONTAINER, dst_path
    ).upload_blob(
        json.dumps(out, ensure_ascii=False, indent=2),
        overwrite=True
    )

    print(
        f"OK - Wrote {len(cleaned)} cleaned comments to "
        f"{SILVER_CONTAINER}/{dst_path}"
    )

if __name__ == "__main__":
    main()
