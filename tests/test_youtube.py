from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from modules.youtube import _to_utc_z, get_target_videos

UTC = timezone.utc


def test_to_utc_z_converts_date():
    assert _to_utc_z(date(2026, 9, 1)) == "2026-09-01T00:00:00Z"


def test_to_utc_z_converts_end_date_to_next_day():
    assert _to_utc_z(date(2026, 9, 1), end_date=True) == "2026-09-02T00:00:00Z"


def test_to_utc_z_accepts_short_date_string():
    assert _to_utc_z("2026-9-1") == "2026-09-01T00:00:00Z"


def test_to_utc_z_accepts_full_date_string():
    assert _to_utc_z("2026-09-01") == "2026-09-01T00:00:00Z"


def test_to_utc_z_accepts_iso_datetime_without_timezone():
    assert _to_utc_z("2026-09-01T12:34:56") == "2026-09-01T12:34:56Z"


def test_to_utc_z_accepts_z_datetime():
    assert _to_utc_z("2026-09-01T12:34:56Z") == "2026-09-01T12:34:56Z"


def test_to_utc_z_converts_offset_aware_datetime():
    value = "2026-09-01T21:34:56+09:00"

    assert _to_utc_z(value) == "2026-09-01T12:34:56Z"


def test_to_utc_z_converts_offset_aware_datetime_object():
    value = datetime(2026, 9, 1, 21, 34, 56, tzinfo=timezone(timedelta(hours=9)))

    assert _to_utc_z(value) == "2026-09-01T12:34:56Z"


def test_to_utc_z_treats_naive_datetime_as_utc():
    value = datetime(2026, 9, 1, 12, 34, 56)

    assert _to_utc_z(value) == "2026-09-01T12:34:56Z"


def test_to_utc_z_rejects_empty_string():
    with pytest.raises(ValueError, match="date is required"):
        _to_utc_z("   ")


def test_to_utc_z_rejects_invalid_date():
    with pytest.raises(ValueError):
        _to_utc_z("2026-02-30")


def make_api(rows, live_video_ids=None, channel_titles=None):
    api = MagicMock()
    api.db.get_videos_by_channel_and_date.return_value = rows
    api.get_live_streaming_video_ids.return_value = live_video_ids or set()
    channel_titles = channel_titles or {}
    api.db.get_channel_by_id.side_effect = lambda channel_id: (
        channel_id,
        channel_titles.get(channel_id, ""),
    )
    return api


def test_get_target_videos_updates_cache_and_returns_video_dicts():
    rows = [
        (
            "video1",
            "動画1",
            "channel1",
            "2026-09-01T12:00:00Z",
            120,
            None,
            None,
            None,
        ),
        (
            "video2",
            "動画2",
            "channel1",
            "2026-09-02T12:00:00Z",
            None,
            None,
            None,
            None,
        ),
    ]
    api = make_api(
        rows,
        live_video_ids={"video1"},
        channel_titles={"channel1": "テストチャンネル"},
    )

    result = get_target_videos(
        "channel1",
        "2026-09-01",
        "2026-09-02",
        api=api,
    )

    api.fetch_and_save_videos_from_channel.assert_called_once_with(
        "channel1",
        published_after="2026-09-01T00:00:00Z",
        published_before="2026-09-03T00:00:00Z",
    )
    api.db.get_videos_by_channel_and_date.assert_called_once_with(
        "channel1",
        "2026-09-01T00:00:00Z",
        "2026-09-03T00:00:00Z",
    )
    api.get_live_streaming_video_ids.assert_called_once_with(
        ["video1", "video2"]
    )

    assert result == [
        {
            "video_id": "video1",
            "title": "動画1",
            "channel_id": "channel1",
            "channel": "テストチャンネル",
            "published_at": "2026-09-01T12:00:00Z",
            "duration": 120,
            "is_live": True,
            "url": "https://www.youtube.com/watch?v=video1",
        },
        {
            "video_id": "video2",
            "title": "動画2",
            "channel_id": "channel1",
            "channel": "テストチャンネル",
            "published_at": "2026-09-02T12:00:00Z",
            "duration": None,
            "is_live": False,
            "url": "https://www.youtube.com/watch?v=video2",
        },
    ]


def test_get_target_videos_handles_missing_channel_title():
    api = MagicMock()
    api.db.get_videos_by_channel_and_date.return_value = [
        (
            "video1",
            "動画1",
            "channel1",
            "2026-09-01T00:00:00Z",
            None,
        )
    ]
    api.db.get_channel_by_id.return_value = None
    api.get_live_streaming_video_ids.return_value = set()

    result = get_target_videos(
        "channel1",
        "2026-09-01",
        "2026-09-01",
        api=api,
    )

    assert result[0]["channel"] == ""
    assert result[0]["is_live"] is False
