from __future__ import annotations

from .comment_processor import extract_comments
from .comments_db import CommentsDB
from .config import COMMENT_KEYWORDS, COOKIES_FILE, JSON_DIRECTORY
from .live_chat import NO_CHAT, RETRY, SUCCESS, delete_video_json, download_live_chat
from .channel import Channel
from .youtube import get_target_videos


def process_channel(
    channel_id: str,
    start_date,
    end_date,
    keywords: list[str] | None = None,
    db: CommentsDB | None = None,
) -> dict[str, int]:
    keywords = keywords if keywords is not None else COMMENT_KEYWORDS
    db = db or CommentsDB()

    videos = get_target_videos(channel_id, start_date, end_date)
    db.ensure_jobs(video["video_id"] for video in videos)

    stats = {
        "total": len(videos),
        "completed": 0,
        "excluded": 0,
        "retry": 0,
    }

    for video in videos:
        video_id = video["video_id"]

        if db.is_completed(video_id):
            stats["completed"] += 1
            print(f"SKIP completed: {video_id} {video['title']}")
            continue

        if db.is_excluded(video_id):
            stats["excluded"] += 1
            print(f"SKIP excluded: {video_id} {video['title']}")
            continue

        # 通常動画はliveStreamingDetailsがないのでここで対象外にする。
        if not video["is_live"]:
            db.mark_excluded(video_id)
            stats["excluded"] += 1
            print(f"EXCLUDE non-live: {video_id} {video['title']}")
            continue

        print(f"PROCESS: {video_id} {video['title']}")

        result = download_live_chat(
            video_id,
            JSON_DIRECTORY,
            cookies_file=COOKIES_FILE,
        )

        if result.status == NO_CHAT:
            db.mark_excluded(video_id)
            stats["excluded"] += 1
            print(f"EXCLUDE no-chat: {video_id}")
            continue

        if result.status == RETRY:
            stats["retry"] += 1
            print(f"RETRY next run: {video_id}: {result.error}")
            continue

        if result.status != SUCCESS or result.json_path is None:
            stats["retry"] += 1
            print(f"RETRY next run: {video_id}: unknown download result")
            continue

        try:
            comments = extract_comments(
                result.json_path,
                keywords,
                video,
            )
            db.save_comments(comments)

            # DB保存成功後にJSONを削除する。
            delete_video_json(video_id, JSON_DIRECTORY)
            db.mark_completed(video_id)
            stats["completed"] += 1

            print(
                f"COMPLETED: {video_id} "
                f"comments={len(comments)}"
            )

        except Exception as exc:
            # completed=0 のまま。JSONも残す。
            stats["retry"] += 1
            print(f"ERROR: {video_id}: {exc}")

    return stats


def process_channels(
    channels: list[Channel],
    start_date,
    end_date,
    keywords: list[str] | None = None,
    db: CommentsDB | None = None,
) -> dict[str, int]:
    db = db or CommentsDB()

    total_stats = {
        "total": 0,
        "completed": 0,
        "excluded": 0,
        "retry": 0,
    }

    for channel in channels:
        print()
        print("=" * 60)
        print(f"CHANNEL: {channel.channel_name} ({channel.channel_id})")
        print("=" * 60)

        stats = process_channel(
            channel_id=channel.channel_id,
            start_date=start_date,
            end_date=end_date,
            keywords=keywords,
            db=db,
        )

        for key in total_stats:
            total_stats[key] += stats[key]

    return total_stats
