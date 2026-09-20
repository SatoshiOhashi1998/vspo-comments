from __future__ import annotations

import re
from datetime import date, datetime, time, timedelta, timezone

from myutils.youtube_api import YouTubeAPI


UTC = timezone.utc


def _to_utc_z(value: str | date | datetime, *, end_date: bool = False) -> str:
    """日付/日時をYouTube API・youtube.db用のUTC ISO文字列へ変換する。"""
    if isinstance(value, datetime):
        dt = value

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)

        dt = dt.astimezone(UTC)

        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    if isinstance(value, date):
        dt = datetime.combine(
            value,
            time.min,
            tzinfo=UTC,
        )

        if end_date:
            dt += timedelta(days=1)

        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    text = str(value).strip()

    if not text:
        raise ValueError("date is required")

    # YYYY-M-D / YYYY-MM-DD の両方を許可する
    match = re.fullmatch(
        r"(\d{4})-(\d{1,2})-(\d{1,2})",
        text,
    )

    if match:
        year, month, day = map(int, match.groups())

        parsed_date = date(
            year,
            month,
            day,
        )

        return _to_utc_z(
            parsed_date,
            end_date=end_date,
        )

    # 日付以外の日時文字列
    normalized = text.replace("Z", "+00:00")

    dt = datetime.fromisoformat(normalized)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)

    dt = dt.astimezone(UTC)

    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def get_target_videos(
    channel_id: str,
    start_date: str | date | datetime,
    end_date: str | date | datetime,
    api: YouTubeAPI | None = None,
) -> list[dict]:
    """
    指定チャンネル・指定期間の動画をyoutube.dbへ反映し、
    youtube.dbから対象動画を返す。

    live_streaming_detailsの有無で通常動画を判定し、
    is_live=True/Falseを付加する。
    """
    api = api or YouTubeAPI()

    start = _to_utc_z(start_date)
    end = _to_utc_z(end_date, end_date=True)

    # 既存キャッシュが部分的でも漏れないよう、対象期間をAPIで更新する。
    api.fetch_and_save_videos_from_channel(
        channel_id,
        published_after=start,
        published_before=end,
    )

    rows = api.db.get_videos_by_channel_and_date(
        channel_id,
        start,
        end,
    )

    video_ids = [row[0] for row in rows]
    live_video_ids = api.get_live_streaming_video_ids(video_ids)

    videos = []
    for row in rows:
        video_id = row[0]
        channel_row = api.db.get_channel_by_id(row[2])
        channel_title = channel_row[1] if channel_row else ""

        videos.append(
            {
                "video_id": video_id,
                "title": row[1],
                "channel_id": row[2],
                "channel": channel_title,
                "published_at": row[3],
                "duration": row[4],
                "is_live": video_id in live_video_ids,
                "url": f"https://www.youtube.com/watch?v={video_id}",
            }
        )

    return videos
