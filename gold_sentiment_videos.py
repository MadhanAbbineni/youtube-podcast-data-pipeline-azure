import json
import os
from pathlib import Path
from datetime import date
import requests

# 1) CHANGE this to your local Silver file path (the one you created)
INGEST_DATE = date.today().isoformat()
SILVER_FILE = Path("silver") / "youtube" / "videos" / f"ingest_date={INGEST_DATE}" / "videos_clean.json"

# 2) Output (Gold)
GOLD_FILE = Path("gold") / "youtube" / "videos" / f"ingest_date={INGEST_DATE}" / "videos_with_sentiment.json"

# 3) Azure AI Foundry / Azure OpenAI settings (set these in environment variables)
# DO NOT paste keys in chat
AOAI_ENDPOINT = os.getenv("AOAI_ENDPOINT")       # e.g. https://ai-huberman-dev.services.ai.azure.com
AOAI_KEY = os.getenv("AOAI_KEY")                 # your key
AOAI_DEPLOYMENT = os.getenv("AOAI_DEPLOYMENT")   # e.g. gpt-4o


def require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v


def aoai_sentiment(title: str) -> dict:
    endpoint = require_env("AOAI_ENDPOINT").rstrip("/")
    key = require_env("AOAI_KEY")
    deployment = require_env("AOAI_DEPLOYMENT")

    url = f"{endpoint}/openai/deployments/{deployment}/chat/completions?api-version=2024-10-21"
    headers = {"api-key": key, "Content-Type": "application/json"}

    system = "Return ONLY valid JSON. No markdown. No extra text."
    user = f"""
Classify sentiment for this YouTube video title.
Return JSON with exactly:
- sentiment: one of ["positive","neutral","negative"]
- emotions: array of up to 5 emotions
- topics: array of up to 8 topics

TITLE: {title}
"""

    payload = {
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": 0.2,
        "response_format": {"type": "json_object"},
    }

    r = requests.post(url, headers=headers, json=payload, timeout=180)
    r.raise_for_status()
    content = r.json()["choices"][0]["message"]["content"]
    return json.loads(content)


def main():
    silver_rows = json.loads(SILVER_FILE.read_text(encoding="utf-8"))

    out = []
    for row in silver_rows:
        title = row.get("title") or ""
        analysis = aoai_sentiment(title)
        out.append({**row, **analysis})

    GOLD_FILE.parent.mkdir(parents=True, exist_ok=True)
    GOLD_FILE.write_text(json.dumps(out, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"✅ Wrote {len(out)} rows to: {GOLD_FILE}")


if __name__ == "__main__":
    main()
