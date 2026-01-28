import azure.functions as func
import logging
import os
import json
import requests
from datetime import datetime, date
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

# --------------------------------------------------
# Helper functions
# --------------------------------------------------

def _env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v


def _upload_json_to_blob(container: str, blob_path: str, payload: dict):
    conn = _env("AZURE_STORAGE_CONNECTION_STRING")
    service = BlobServiceClient.from_connection_string(conn)
    blob = service.get_blob_client(container=container, blob=blob_path)
    blob.upload_blob(
        json.dumps(payload, indent=2, ensure_ascii=False),
        overwrite=True
    )


# --------------------------------------------------
# FUNCTION 1: INGEST VIDEOS
# --------------------------------------------------

@app.route(route="ingest_youtube_videos", methods=["POST"])
def ingest_youtube_videos(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("ingest_youtube_videos started")

    YOUTUBE_API_KEY = _env("YOUTUBE_API_KEY")
    CONTAINER = os.getenv("STORAGE_CONTAINER", "bronze")

    CHANNEL_ID = "UC2D2CMWXMOVWx7giW1n3LIg"  # Huberman
    MAX_RESULTS = 10

    # Get uploads playlist
    channel_url = (
        "https://www.googleapis.com/youtube/v3/channels"
        f"?part=contentDetails&id={CHANNEL_ID}&key={YOUTUBE_API_KEY}"
    )
    channel_resp = requests.get(channel_url).json()
    uploads_playlist = channel_resp["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    # Get video IDs
    playlist_url = (
        "https://www.googleapis.com/youtube/v3/playlistItems"
        f"?part=contentDetails&playlistId={uploads_playlist}"
        f"&maxResults={MAX_RESULTS}&key={YOUTUBE_API_KEY}"
    )
    playlist_resp = requests.get(playlist_url).json()
    video_ids = [i["contentDetails"]["videoId"] for i in playlist_resp.get("items", [])]

    # Get video details
    videos_url = (
        "https://www.googleapis.com/youtube/v3/videos"
        f"?part=snippet,statistics,contentDetails"
        f"&id={','.join(video_ids)}"
        f"&key={YOUTUBE_API_KEY}"
    )
    videos_resp = requests.get(videos_url).json()

    ingest_date = date.today().isoformat()
    blob_path = f"youtube/videos/ingest_date={ingest_date}/videos_raw.json"

    payload = {
        "channelId": CHANNEL_ID,
        "pulledAt": datetime.utcnow().isoformat() + "Z",
        "videoCount": len(video_ids),
        "items": videos_resp.get("items", [])
    }

    _upload_json_to_blob(CONTAINER, blob_path, payload)

    return func.HttpResponse(
        f"OK - Saved {len(video_ids)} videos to {CONTAINER}/{blob_path}",
        status_code=200
    )


# --------------------------------------------------
# FUNCTION 2: INGEST COMMENTS
# --------------------------------------------------

def _youtube_comment_threads(api_key: str, video_id: str, max_results: int):
    url = "https://www.googleapis.com/youtube/v3/commentThreads"
    params = {
        "part": "snippet",
        "videoId": video_id,
        "maxResults": min(max_results, 100),
        "textFormat": "plainText",
        "key": api_key
    }
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    return r.json()


@app.route(route="ingest_youtube_comments", methods=["POST"])
def ingest_youtube_comments(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("ingest_youtube_comments started")

    body = req.get_json()
    video_ids = body.get("video_ids", [])
    max_comments = int(body.get("max_comments_per_video", 50))

    if not video_ids:
        return func.HttpResponse(
            "Provide video_ids in request body",
            status_code=400
        )

    YOUTUBE_API_KEY = _env("YOUTUBE_API_KEY")
    CONTAINER = os.getenv("STORAGE_CONTAINER", "bronze")

    all_comments = []

    for vid in video_ids:
        data = _youtube_comment_threads(YOUTUBE_API_KEY, vid, max_comments)
        for item in data.get("items", []):
            sn = item["snippet"]["topLevelComment"]["snippet"]
            all_comments.append({
                "videoId": vid,
                "commentId": item["snippet"]["topLevelComment"]["id"],
                "author": sn.get("authorDisplayName"),
                "text": sn.get("textDisplay"),
                "likes": sn.get("likeCount"),
                "publishedAt": sn.get("publishedAt")
            })

    ingest_date = date.today().isoformat()
    blob_path = f"youtube/comments/ingest_date={ingest_date}/comments_raw.json"

    payload = {
        "ingest_date": ingest_date,
        "video_count": len(video_ids),
        "comment_count": len(all_comments),
        "items": all_comments
    }

    _upload_json_to_blob(CONTAINER, blob_path, payload)

    return func.HttpResponse(
        f"OK - Saved {len(all_comments)} comments to {CONTAINER}/{blob_path}",
        status_code=200
    )
