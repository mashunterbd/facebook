#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import aiohttp
import asyncio
import os
import sys
import json
import time
import datetime
from typing import Optional, Dict, Any

GRAPH_API_VERSION = "v24.0"
MAX_RETRIES = 5
INITIAL_BACKOFF = 1.0
TIMEOUT_SECONDS = 300  # per request

def human_size(n: int) -> str:
    for u in ["B", "KB", "MB", "GB", "TB"]:
        if n < 1024:
            return f"{n:.2f}{u}"
        n /= 1024
    return f"{n:.2f}PB"

def parse_schedule_text(s: str) -> Optional[int]:
    try:
        dt = datetime.datetime.strptime(s.strip(), "%d-%m-%Y %I:%M %p")
        return int(dt.timestamp())
    except Exception:
        return None

def now_epoch() -> int:
    return int(time.time())

def validate_schedule(post_status: str, schedule_unix: Optional[int]) -> Optional[int]:
    ps = (post_status or "").strip().lower()
    if ps == "public":
        return None
    if ps != "schedule":
        raise SystemExit(json.dumps({
            "ok": False,
            "error": "Invalid FB_POST_STATUS",
            "detail": "Allowed values: public | schedule"
        }))
    if not schedule_unix:
        raise SystemExit(json.dumps({
            "ok": False,
            "error": "Missing schedule time",
            "detail": "FB_SCHEDULE_UNIX or FB_SCHEDULE_TXT required when FB_POST_STATUS = schedule"
        }))
    now = now_epoch()
    min_allowed = now + 10 * 60
    max_allowed = now + 75 * 24 * 60 * 60
    if schedule_unix < min_allowed:
        raise SystemExit(json.dumps({
            "ok": False,
            "error": "Schedule too soon",
            "detail": "Facebook requires at least 10 minutes in the future."
        }))
    if schedule_unix > max_allowed:
        raise SystemExit(json.dumps({
            "ok": False,
            "error": "Schedule too far",
            "detail": "Facebook allows at most 75 days in the future."
        }))
    return schedule_unix

async def http_post(session: aiohttp.ClientSession, url: str, *, data=None, form=None, ok: int = 200) -> Dict[str, Any]:
    backoff = INITIAL_BACKOFF
    attempt = 0
    while True:
        try:
            async with session.post(url, data=form if form else data, timeout=TIMEOUT_SECONDS) as r:
                txt = await r.text()
                if r.status != ok:
                    print(f"\n[HTTP {r.status}] {txt}")
                    raise aiohttp.ClientResponseError(request_info=r.request_info, history=r.history,
                                                      status=r.status, message=txt)
                try:
                    return json.loads(txt)
                except json.JSONDecodeError:
                    return {"raw": txt}
        except (aiohttp.ClientConnectionError, aiohttp.ServerTimeoutError, asyncio.TimeoutError) as e:
            attempt += 1
            if attempt > MAX_RETRIES:
                raise
            print(f"[retry {attempt}/{MAX_RETRIES}] {e} → wait {backoff:.1f}s")
            await asyncio.sleep(backoff)
            backoff *= 2.0

async def upload_reel(cfg: Dict[str, Any]) -> None:
    page_id      = cfg["page_id"]
    access_token = cfg["access_token"]
    video_path   = cfg["video_path"]
    description  = cfg.get("description") or ""
    comment      = cfg.get("comment") or ""
    post_status  = (cfg.get("post_status") or "public").lower()
    schedule     = validate_schedule(post_status, cfg.get("schedule_unix"))

    if not os.path.isfile(video_path):
        raise FileNotFoundError(f"Video not found: {video_path}")

    file_size = os.path.getsize(video_path)
    filename  = os.path.basename(video_path)
    base_url  = f"https://graph.facebook.com/{GRAPH_API_VERSION}/{page_id}/video_reels"

    connector = aiohttp.TCPConnector(limit=2)
    timeout   = aiohttp.ClientTimeout(total=None)

    print("✅ Runtime OK")
    print("🚀 Starting upload session...")

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        res = await http_post(session, base_url, data={
            "upload_phase": "start",
            "file_size": str(file_size),
            "access_token": access_token
        })
        upload_url = res.get("upload_url")
        video_id   = res.get("video_id")

        if not upload_url or not video_id:
            raise RuntimeError(f"Failed to start session: {res}")

        print(f"⬆️  Session started: video_id={video_id} | {human_size(file_size)}")

        chunk_size = 512 * 1024
        offset = 0

        with open(video_path, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                await http_post(session, upload_url, data=chunk, ok=200)
                offset += len(chunk)
                pct = offset / file_size * 100
                print(f"Uploading... {pct:.1f}%", end="\r")

        print("\n✅ Upload complete. Finalizing...")

        finish_data = {
            "upload_phase": "finish",
            "video_id": video_id,
            "access_token": access_token,
            "video_state": "PUBLISHED" if schedule is None else "SCHEDULED",
        }
        if description:
            finish_data["description"] = description
        if schedule:
            finish_data["scheduled_publish_time"] = str(schedule)

        await http_post(session, base_url, data=finish_data)
        print("✅ Reel published.")

        if comment:
            comment_url = f"https://graph.facebook.com/{GRAPH_API_VERSION}/{video_id}/comments"
            await http_post(session, comment_url, data={
                "access_token": access_token,
                "message": comment
            })
            print("💬 Comment posted.")

        print(f"🎬 Reel URL: https://www.facebook.com/{video_id}")

def get_config() -> Dict[str, Any]:
    env = os.getenv
    args = sys.argv[1:]
    cfg = {
        "page_id": env("FB_PAGE_ID"),
        "access_token": env("FB_ACCESS_TOKEN"),
        "video_path": env("FB_VIDEO_PATH"),
        "description": env("FB_DESCRIPTION"),
        "comment": env("FB_COMMENT"),
        "post_status": env("FB_POST_STATUS") or "public",
        "schedule_unix": None,
    }
    sch_txt = env("FB_SCHEDULE_TXT")
    sch_unx = env("FB_SCHEDULE_UNIX")
    if sch_unx and sch_unx.isdigit():
        cfg["schedule_unix"] = int(sch_unx)
    elif sch_txt:
        parsed = parse_schedule_text(sch_txt)
        if parsed:
            cfg["schedule_unix"] = parsed
    return cfg

if __name__ == "__main__":
    cfg = get_config()
    missing = [k for k in ("page_id", "access_token", "video_path") if not cfg.get(k)]
    if missing:
        print("❌ Missing:", ", ".join(missing))
        sys.exit(1)
    asyncio.run(upload_reel(cfg))