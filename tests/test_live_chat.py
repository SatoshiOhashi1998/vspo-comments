from pathlib import Path
from unittest.mock import MagicMock

import pytest
from yt_dlp.utils import DownloadError

import modules.live_chat as live_chat


def make_ydl_mock():
    context = MagicMock()
    ydl = MagicMock()
    context.__enter__.return_value = ydl
    context.__exit__.return_value = False
    return context, ydl


def test_candidate_json_files_returns_matching_files_only(tmp_path):
    (tmp_path / "video1.json").write_text("{}", encoding="utf-8")
    (tmp_path / "video1-extra.json").write_text("{}", encoding="utf-8")
    (tmp_path / "video2.json").write_text("{}", encoding="utf-8")
    (tmp_path / "video1.txt").write_text("x", encoding="utf-8")

    result = live_chat._candidate_json_files("video1", tmp_path)

    assert result == sorted(
        [tmp_path / "video1-extra.json", tmp_path / "video1.json"]
    )


def test_delete_video_json_removes_matching_json(tmp_path):
    target = tmp_path / "video1.json"
    other = tmp_path / "video2.json"
    target.write_text("{}", encoding="utf-8")
    other.write_text("{}", encoding="utf-8")

    live_chat.delete_video_json("video1", tmp_path)

    assert not target.exists()
    assert other.exists()


@pytest.mark.parametrize(
    "message",
    [
        "There are no subtitles for the requested languages",
        "no subtitles available",
        "Live chat is disabled",
        "Live chat replay is not available",
        "Live chat is unavailable",
        "Requested subtitles are not available",
    ],
)
def test_is_no_chat_error_matches_known_messages(message):
    assert live_chat._is_no_chat_error(message)


def test_is_no_chat_error_is_case_insensitive():
    assert live_chat._is_no_chat_error("LIVE CHAT IS DISABLED")


def test_is_no_chat_error_returns_false_for_other_error():
    assert not live_chat._is_no_chat_error("network timeout")


@pytest.mark.parametrize(
    "message",
    [
        "Private video",
        "Video unavailable",
        "This video has been removed",
        "This video is no longer available",
    ],
)
def test_is_permanent_exclusion_error_matches_known_messages(message):
    assert live_chat._is_permanent_exclusion_error(message)


def test_is_permanent_exclusion_error_returns_false_for_retryable_error():
    assert not live_chat._is_permanent_exclusion_error("network timeout")


def test_download_live_chat_success(monkeypatch, tmp_path):
    context, ydl = make_ydl_mock()
    monkeypatch.setattr(live_chat, "YoutubeDL", MagicMock(return_value=context))

    stale = tmp_path / "video1.json"
    stale.write_text("stale", encoding="utf-8")

    new_path = tmp_path / "video1.json"

    def download(_urls):
        new_path.write_text("{}", encoding="utf-8")

    ydl.download.side_effect = download

    result = live_chat.download_live_chat("video1", tmp_path)

    assert result.status == live_chat.SUCCESS
    assert result.json_path == new_path
    assert result.error is None
    ydl.download.assert_called_once_with(
        ["https://www.youtube.com/watch?v=video1"]
    )


def test_download_live_chat_passes_existing_cookie_file(monkeypatch, tmp_path):
    context, ydl = make_ydl_mock()
    youtube_dl = MagicMock(return_value=context)
    monkeypatch.setattr(live_chat, "YoutubeDL", youtube_dl)

    cookie_file = tmp_path / "cookies.txt"
    cookie_file.write_text("cookies", encoding="utf-8")
    json_path = tmp_path / "video1.json"
    ydl.download.side_effect = lambda _urls: json_path.write_text("{}", encoding="utf-8")

    result = live_chat.download_live_chat(
        "video1",
        tmp_path,
        cookies_file=cookie_file,
    )

    assert result.status == live_chat.SUCCESS
    options = youtube_dl.call_args.args[0]
    assert options["cookiefile"] == str(cookie_file)


def test_download_live_chat_ignores_missing_cookie_file(monkeypatch, tmp_path):
    context, ydl = make_ydl_mock()
    youtube_dl = MagicMock(return_value=context)
    monkeypatch.setattr(live_chat, "YoutubeDL", youtube_dl)

    ydl.download.side_effect = lambda _urls: (tmp_path / "video1.json").write_text(
        "{}", encoding="utf-8"
    )

    missing_cookie = tmp_path / "missing-cookies.txt"
    result = live_chat.download_live_chat(
        "video1",
        tmp_path,
        cookies_file=missing_cookie,
    )

    options = youtube_dl.call_args.args[0]
    assert result.status == live_chat.SUCCESS
    assert "cookiefile" not in options


def test_download_live_chat_returns_no_chat_for_no_chat_download_error(monkeypatch, tmp_path):
    context, ydl = make_ydl_mock()
    monkeypatch.setattr(live_chat, "YoutubeDL", MagicMock(return_value=context))
    ydl.download.side_effect = DownloadError("live chat is disabled")

    stale = tmp_path / "video1.json"
    stale.write_text("stale", encoding="utf-8")

    result = live_chat.download_live_chat("video1", tmp_path)

    assert result.status == live_chat.NO_CHAT
    assert result.json_path is None
    assert "live chat is disabled" in result.error
    assert not stale.exists()


def test_download_live_chat_returns_no_chat_for_permanent_exclusion(monkeypatch, tmp_path):
    context, ydl = make_ydl_mock()
    monkeypatch.setattr(live_chat, "YoutubeDL", MagicMock(return_value=context))
    ydl.download.side_effect = DownloadError("Private video")

    result = live_chat.download_live_chat("video1", tmp_path)

    assert result.status == live_chat.NO_CHAT


def test_download_live_chat_returns_retry_for_other_download_error(monkeypatch, tmp_path):
    context, ydl = make_ydl_mock()
    monkeypatch.setattr(live_chat, "YoutubeDL", MagicMock(return_value=context))
    ydl.download.side_effect = DownloadError("network timeout")

    result = live_chat.download_live_chat("video1", tmp_path)

    assert result.status == live_chat.RETRY
    assert result.json_path is None
    assert result.error == "network timeout"


def test_download_live_chat_returns_retry_for_unexpected_error(monkeypatch, tmp_path):
    context, ydl = make_ydl_mock()
    monkeypatch.setattr(live_chat, "YoutubeDL", MagicMock(return_value=context))
    ydl.download.side_effect = RuntimeError("unexpected")

    result = live_chat.download_live_chat("video1", tmp_path)

    assert result.status == live_chat.RETRY
    assert result.error == "unexpected"


def test_download_live_chat_returns_retry_when_json_is_missing(monkeypatch, tmp_path):
    context, ydl = make_ydl_mock()
    monkeypatch.setattr(live_chat, "YoutubeDL", MagicMock(return_value=context))

    result = live_chat.download_live_chat("video1", tmp_path)

    assert result.status == live_chat.RETRY
    assert result.error == "yt-dlp completed but live_chat JSON was not found."
