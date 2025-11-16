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

# -------------------- Helpers -------------------- #
def human_size(n: int) -> str:
    for u in ["B", "KB", "MB", "GB", "TB"]:
        if n < 1024:
            return f"{n:.2f}{u}"
        n /= 1024
    return f"{n:.2f}PB"

def parse_schedule_text(s: str) -> Optional[int]:
    """
    Parse '29-10-2025 06:58 AM' -> epoch seconds.
    If parsing fails, return None.
    """
    try:
        dt = datetime.datetime.strptime(s.strip(), "%d-%m-%Y %I:%M %p")
        return int(dt.timestamp())
    except Exception:
        return None

def now_epoch() -> int:
    return int(time.time())

def validate_schedule(post_status: str, schedule_unix: Optional[int]) -> Optional[int]:
    """
    - If post_status == 'public'  -> ignore schedule (return None).
    - If post_status == 'schedule' -> schedule_unix must be valid and inside FB window.
      Facebook window (pages): >= 10 minutes and <= 75 days in the future.
    """
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
    min_allowed = now + 10 * 60                    # 10 minutes
    max_allowed = now + 75 * 24 * 60 * 60          # 75 days

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

async def http_get(session: aiohttp.ClientSession, url: str, *, params=None, ok: int = 200) -> Dict[str, Any]:
    backoff = INITIAL_BACKOFF
    attempt = 0
    while True:
        try:
            async with session.get(url, params=params, timeout=TIMEOUT_SECONDS) as r:
                txt = await r.text()
                if r.status != ok:
                    print(f"\n[HTTP {r.status}] {txt}")
                    raise aiohttp.ClientResponseError(request_info=r.request_info, history=r.history,
                                                      status=r.status, message=txt)
                return json.loads(txt)
        except (aiohttp.ClientConnectionError, aiohttp.ServerTimeoutError, asyncio.TimeoutError) as e:
            attempt += 1
            if attempt > MAX_RETRIES:
                raise
            await asyncio.sleep(backoff)
            backoff *= 2.0

# -------------------- Core -------------------- #
async def upload_video(cfg: Dict[str, Any]) -> None:
    """
    Upload large video to Facebook Page, optionally schedule, set title/description,
    upload preferred thumbnail, and post a first comment.
    Produces a structured JSON summary on stdout for n8n.
    """
    page_id      = cfg["page_id"]
    access_token = cfg["access_token"]
    video_path   = cfg["video_path"]
    title        = cfg.get("title") or ""
    description  = cfg.get("description") or ""
    comment      = cfg.get("comment") or ""
    thumb_path   = cfg.get("thumb_path")
    post_status  = (cfg.get("post_status") or "public").lower()
    schedule     = validate_schedule(post_status, cfg.get("schedule_unix"))

    if not os.path.isfile(video_path):
        raise FileNotFoundError(f"Video not found: {video_path}")

    file_size = os.path.getsize(video_path)
    filename  = os.path.basename(video_path)
    base_url  = f"https://graph-video.facebook.com/{GRAPH_API_VERSION}/{page_id}/videos"

    summary: Dict[str, Any] = {
        "page_id": page_id,
        "video_file": video_path,
        "size": human_size(file_size),
        "video_id": None,
        "post_status": post_status,
        "scheduled_publish_time": schedule,
        "title_sent": title,
        "description_sent": description,
        "thumbnail_uploaded": False,
        "comment_posted": False,
        "finish_payload": {},
        "links": {},
    }

    connector = aiohttp.TCPConnector(limit=2)
    timeout   = aiohttp.ClientTimeout(total=None)

    print("✅ Runtime OK")
    print("🚀 Initializing upload session...")

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # 1) START
        start = await http_post(session, base_url, data={
            "upload_phase": "start",
            "file_size": str(file_size),
            "access_token": access_token
        })
        session_id = start.get("upload_session_id")
        video_id   = start.get("video_id")
        start_off  = int(start.get("start_offset", 0))
        end_off    = int(start.get("end_offset", 0))

        if not session_id:
            raise RuntimeError(f"Failed to start upload session: {start}")

        summary["video_id"] = video_id
        print(f"🎬 Started video_id={video_id} | {human_size(file_size)}")

        # 2) TRANSFER
        with open(video_path, "rb") as f:
            while True:
                if start_off == end_off:
                    break
                f.seek(start_off)
                chunk = f.read(end_off - start_off)

                form = aiohttp.FormData()
                form.add_field("upload_phase", "transfer")
                form.add_field("start_offset", str(start_off))
                form.add_field("upload_session_id", session_id)
                form.add_field("access_token", access_token)
                form.add_field("video_file_chunk", chunk,
                               filename=f"{filename}.part",
                               content_type="application/octet-stream")

                tr = await http_post(session, base_url, form=form)
                start_off = int(tr.get("start_offset", 0))
                end_off   = int(tr.get("end_offset", 0))
                pct = (start_off / file_size) * 100
                print(f"\r⬆️  Uploading… {pct:5.1f}%", end="")
        print("\n✅ Transfer complete")

        # 3) FINISH
        finish_payload = {
            "upload_phase": "finish",
            "upload_session_id": session_id,
            "access_token": access_token,
            "published": "false" if schedule is not None else "true",
        }
        if title:
            finish_payload["title"] = title
        if description:
            finish_payload["description"] = description
        if schedule is not None:
            finish_payload["scheduled_publish_time"] = str(schedule)

        summary["finish_payload"] = finish_payload
        await http_post(session, base_url, data=finish_payload)
        print("✅ Finish sent")

        # 4) Poll until ready
        vid_url = f"https://graph.facebook.com/{GRAPH_API_VERSION}/{video_id}"
        ready = False
        for _ in range(60):  # up to ~120s
            try:
                meta = await http_get(session, vid_url, params={
                    "fields": "status{video_status,processing_progress}",
                    "access_token": access_token
                })
                s = meta.get("status") or {}
                vstat = (s.get("video_status") or "").lower()
                prog = s.get("processing_progress")
            except aiohttp.ClientResponseError:
                meta = await http_get(session, vid_url, params={
                    "fields": "status",
                    "access_token": access_token
                })
                s = meta.get("status") or {}
                vstat = (s.get("video_status") or "").lower()
                prog = None

            if prog is not None:
                print(f"\r⏳ Processing… {prog}% ({vstat})", end="")
            else:
                print(f"\r⏳ Processing… ({vstat})", end="")

            if vstat in {"ready", "published"} or (prog is not None and int(prog) >= 100):
                ready = True
                break
            await asyncio.sleep(2)
        print("")
        if not ready:
            print("⚠️  Video not fully ready yet; continuing (thumbnail/comment may appear slightly later)")

        # 5) Thumbnail (optional)
        if thumb_path and os.path.isfile(thumb_path):
            print(f"🖼️ Uploading thumbnail: {thumb_path}")
            th_url = f"https://graph.facebook.com/{GRAPH_API_VERSION}/{video_id}/thumbnails"
            form = aiohttp.FormData()
            form.add_field("access_token", access_token)
            form.add_field("is_preferred", "true")
            form.add_field("source", open(thumb_path, "rb"),
                           filename=os.path.basename(thumb_path))
            await http_post(session, th_url, form=form)
            summary["thumbnail_uploaded"] = True
            print("✅ Thumbnail uploaded & set preferred")

        # 6) Comment (optional)
        if comment:
            print("💬 Posting comment…")
            c_url = f"https://graph.facebook.com/{GRAPH_API_VERSION}/{video_id}/comments"
            await http_post(session, c_url, data={
                "access_token": access_token,
                "message": comment
            })
            summary["comment_posted"] = True
            print("✅ Comment added")

        # 7) Summary for n8n
        summary["links"]["video"] = f"https://www.facebook.com/{video_id}"
        print(json.dumps(summary, ensure_ascii=False, indent=2))

# -------------------- Entry -------------------- #
def get_config() -> Dict[str, Any]:
    env = os.getenv
    args = sys.argv[1:]

    # Base config
    cfg: Dict[str, Any] = {
        "page_id": env("FB_PAGE_ID"),
        "access_token": env("FB_ACCESS_TOKEN"),
        "video_path": env("FB_VIDEO_PATH"),
        "title": env("FB_TITLE"),
        "description": env("FB_DESCRIPTION"),
        "comment": env("FB_COMMENT"),
        "thumb_path": env("FB_THUMB_PATH"),
        "post_status": (env("FB_POST_STATUS") or "public").lower(),
        "schedule_unix": None,
    }

    # Schedule from env: FB_SCHEDULE_UNIX or FB_SCHEDULE_TXT
    sch_txt = env("FB_SCHEDULE_TXT")
    sch_unx = env("FB_SCHEDULE_UNIX")
    if sch_unx and sch_unx.isdigit():
        cfg["schedule_unix"] = int(sch_unx)
    elif sch_txt:
        parsed = parse_schedule_text(sch_txt)
        if parsed:
            cfg["schedule_unix"] = parsed

    # CLI overrides (optional)
    if "-lv" in args:
        i = args.index("-lv") + 1
        if i < len(args): cfg["video_path"] = args[i]
    if "-t" in args:
        i = args.index("-t") + 1
        if i < len(args): cfg["thumb_path"] = args[i]
    if "-d" in args:
        i = args.index("-d") + 1
        if i < len(args): cfg["description"] = args[i]
    if "-c" in args:
        i = args.index("-c") + 1
        if i < len(args): cfg["comment"] = args[i]
    if "-s" in args:
        i = args.index("-s") + 1
        if i < len(args):
            ts = parse_schedule_text(args[i])
            if ts: cfg["schedule_unix"] = ts

    return cfg

if __name__ == "__main__":
    cfg = get_config()
    missing = [k for k in ("page_id", "access_token", "video_path") if not cfg.get(k)]
    if missing:
        print("❌ Missing:", ", ".join(missing))
        sys.exit(1)
    asyncio.run(upload_video(cfg))
