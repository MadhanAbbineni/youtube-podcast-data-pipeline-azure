import os, json
from datetime import date
import requests
from azure.storage.blob import BlobServiceClient

def env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v

def blob_client(container: str, path: str):
    svc = BlobServiceClient.from_connection_string(env("AZURE_STORAGE_CONNECTION_STRING"))
    return svc.get_blob_client(container=container, blob=path)

def aoai_sentiment(text: str) -> dict:
    endpoint = env("AOAI_ENDPOINT").rstrip("/")
    key = env("AOAI_KEY")
    deployment = env("AOAI_DEPLOYMENT")
    api_version = os.getenv("AOAI_API_VERSION", "2024-10-21")

    url = f"{endpoint}/openai/deployments/{deployment}/chat/completions?api-version={api_version}"
    headers = {"Content-Type": "application/json", "api-key": key}

    prompt = (
        "Analyze sentiment and emotions for this YouTube comment.\n"
        "Return ONLY valid JSON like:\n"
        '{"sentiment":"positive|neutral|negative","score":-1.0,"emotion":"joy|anger|sadness|fear|surprise|disgust|neutral","summary":"..."}\n\n'
        f"Comment: {text}"
    )

    payload = {
        "messages": [
            {"role": "system", "content": "You are a strict JSON generator."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.2,
        "max_tokens": 120
    }

    r = requests.post(url, headers=headers, json=payload, timeout=120)
    r.raise_for_status()
    content = r.json()["choices"][0]["message"]["content"]

    # Make it robust if model adds extra text accidentally
    try:
        return json.loads(content)
    except Exception:
        # fallback: wrap raw
        return {"sentiment": "neutral", "score": 0.0, "emotion": "neutral", "summary": content[:200]}

def main():
    ingest_date = date.today().isoformat()

    # READ cleaned comments from SILVER
    src_container = "silver"
    src_path = f"youtube/comments/ingest_date={ingest_date}/comments_clean.json"

    # WRITE analytics sentiment to GOLD
    dst_container = "gold"
    dst_path = f"youtube/comments/ingest_date={ingest_date}/comments_with_sentiment.json"

    raw_bytes = blob_client(src_container, src_path).download_blob().readall()
    cleaned = json.loads(raw_bytes)

    out_items = []
    items = cleaned.get("items", [])

    for i, c in enumerate(items, start=1):
        text = (c.get("text") or "").strip()
        if not text:
            continue

        result = aoai_sentiment(text)

        out_items.append({
            **c,
            "sentiment": result.get("sentiment"),
            "sentiment_score": result.get("score"),
            "emotion": result.get("emotion"),
            "summary": result.get("summary")
        })

        # small progress print every 5
        if i % 5 == 0:
            print(f"Processed {i}/{len(items)} comments...")

    out = {
        "ingest_date": ingest_date,
        "rows": len(out_items),
        "items": out_items
    }

    blob_client(dst_container, dst_path).upload_blob(
        json.dumps(out, ensure_ascii=False, indent=2).encode("utf-8"),
        overwrite=True
    )

    print(f"OK - Wrote {len(out_items)} rows to {dst_container}/{dst_path}")

if __name__ == "__main__":
    main()
