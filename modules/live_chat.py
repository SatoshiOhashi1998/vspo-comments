from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from yt_dlp import YoutubeDL
from yt_dlp.utils import DownloadError


SUCCESS = "success"
NO_CHAT = "no_chat"
RETRY = "retry"


@dataclass
class DownloadResult:
    status: str
    json_path: Path | None = None
    error: str | None = None


def _candidate_json_files(video_id: str, output_dir: Path) -> list[Path]:
    return sorted(
        path
        for path in output_dir.glob(f"{video_id}*.json")
        if path.is_file()
    )


def delete_video_json(video_id: str, output_dir: str | Path) -> None:
    output_path = Path(output_dir)
    for path in _candidate_json_files(video_id, output_path):
        path.unlink(missing_ok=True)


def _is_no_chat_error(message: str) -> bool:
    text = message.lower()
    patterns = (
        "there are no subtitles for the requested languages",
        "no subtitles",
        "live chat is disabled",
        "live chat replay is not available",
        "live chat is unavailable",
        "requested subtitles are not available",
    )
    return any(pattern in text for pattern in patterns)


def _is_permanent_exclusion_error(message: str) -> bool:
    text = message.lower()
    patterns = (
        "private video",
        "video unavailable",
        "has been removed",
        "this video is no longer available",
    )
    return any(pattern in text for pattern in patterns)


def download_live_chat(
    video_id: str,
    output_dir: str | Path,
    cookies_file: str | Path | None = None,
) -> DownloadResult:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # 「失敗したらダウンロードから再開」の方針なので、前回のJSONは再利用しない。
    delete_video_json(video_id, output_path)

    video_url = f"https://www.youtube.com/watch?v={video_id}"

    ydl_opts = {
        "skip_download": True,
        "writesubtitles": True,
        "subtitleslangs": ["live_chat"],
        "outtmpl": str(output_path / "%(id)s.%(ext)s"),
    }

    cookie_path = Path(cookies_file) if cookies_file else None
    if cookie_path and cookie_path.exists():
        ydl_opts["cookiefile"] = str(cookie_path)

    try:
        with YoutubeDL(ydl_opts) as ydl:
            ydl.download([video_url])

    except DownloadError as exc:
        message = str(exc)

        if _is_no_chat_error(message) or _is_permanent_exclusion_error(message):
            delete_video_json(video_id, output_path)
            return DownloadResult(NO_CHAT, error=message)

        return DownloadResult(RETRY, error=message)

    except Exception as exc:
        return DownloadResult(RETRY, error=str(exc))

    candidates = _candidate_json_files(video_id, output_path)
    if not candidates:
        return DownloadResult(
            RETRY,
            error="yt-dlp completed but live_chat JSON was not found.",
        )

    return DownloadResult(SUCCESS, json_path=candidates[0])
