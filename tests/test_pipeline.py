from pathlib import Path

from modules.channel import Channel
from modules.comments_db import CommentsDB
from modules.live_chat import DownloadResult
import modules.pipeline as pipeline


def video(video_id="video1", *, is_live=True):
    return {
        "video_id": video_id,
        "title": f"動画 {video_id}",
        "channel_id": "channel1",
        "channel": "テストチャンネル",
        "published_at": "2026-09-01T00:00:00Z",
        "duration": 120,
        "is_live": is_live,
        "url": f"https://www.youtube.com/watch?v={video_id}",
    }


def setup_common(monkeypatch, videos):
    monkeypatch.setattr(pipeline, "get_target_videos", lambda *args: videos)
    monkeypatch.setattr(pipeline, "JSON_DIRECTORY", Path("json-output"))
    monkeypatch.setattr(pipeline, "COOKIES_FILE", "cookies.txt")


def test_process_channel_excludes_non_live_video(monkeypatch, tmp_path):
    db = CommentsDB(tmp_path / "comments.db")
    setup_common(monkeypatch, [video("video1", is_live=False)])

    result = pipeline.process_channel(
        "channel1",
        "2026-09-01",
        "2026-09-01",
        keywords=["かわいい"],
        db=db,
    )

    assert result == {"total": 1, "completed": 0, "excluded": 1, "retry": 0}
    assert db.is_excluded("video1")


def test_process_channel_skips_completed_job(monkeypatch, tmp_path):
    db = CommentsDB(tmp_path / "comments.db")
    db.mark_completed("video1")
    setup_common(monkeypatch, [video("video1")])

    download = lambda *args, **kwargs: (_ for _ in ()).throw(
        AssertionError("download should not be called")
    )
    monkeypatch.setattr(pipeline, "download_live_chat", download)

    result = pipeline.process_channel(
        "channel1",
        "2026-09-01",
        "2026-09-01",
        keywords=["かわいい"],
        db=db,
    )

    assert result == {"total": 1, "completed": 1, "excluded": 0, "retry": 0}


def test_process_channel_skips_excluded_job(monkeypatch, tmp_path):
    db = CommentsDB(tmp_path / "comments.db")
    db.mark_excluded("video1")
    setup_common(monkeypatch, [video("video1")])

    monkeypatch.setattr(
        pipeline,
        "download_live_chat",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("download should not be called")
        ),
    )

    result = pipeline.process_channel(
        "channel1",
        "2026-09-01",
        "2026-09-01",
        keywords=["かわいい"],
        db=db,
    )

    assert result == {"total": 1, "completed": 0, "excluded": 1, "retry": 0}


def test_process_channel_marks_no_chat_as_excluded(monkeypatch, tmp_path):
    db = CommentsDB(tmp_path / "comments.db")
    setup_common(monkeypatch, [video()])
    monkeypatch.setattr(
        pipeline,
        "download_live_chat",
        lambda *args, **kwargs: DownloadResult(pipeline.NO_CHAT, error="no chat"),
    )

    result = pipeline.process_channel(
        "channel1",
        "2026-09-01",
        "2026-09-01",
        keywords=["かわいい"],
        db=db,
    )

    assert result == {"total": 1, "completed": 0, "excluded": 1, "retry": 0}
    assert db.is_excluded("video1")


def test_process_channel_keeps_retryable_job_pending(monkeypatch, tmp_path):
    db = CommentsDB(tmp_path / "comments.db")
    setup_common(monkeypatch, [video()])
    monkeypatch.setattr(
        pipeline,
        "download_live_chat",
        lambda *args, **kwargs: DownloadResult(
            pipeline.RETRY,
            error="temporary error",
        ),
    )

    result = pipeline.process_channel(
        "channel1",
        "2026-09-01",
        "2026-09-01",
        keywords=["かわいい"],
        db=db,
    )

    assert result == {"total": 1, "completed": 0, "excluded": 0, "retry": 1}
    assert not db.is_completed("video1")
    assert not db.is_excluded("video1")


def test_process_channel_saves_comments_deletes_json_and_completes(
    monkeypatch,
    tmp_path,
):
    db = CommentsDB(tmp_path / "comments.db")
    setup_common(monkeypatch, [video()])

    json_path = tmp_path / "video1.json"
    json_path.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(
        pipeline,
        "download_live_chat",
        lambda *args, **kwargs: DownloadResult(
            pipeline.SUCCESS,
            json_path=json_path,
        ),
    )

    comments = [
        {
            "video_id": "video1",
            "timestamp": "00:00:01",
            "comment": "かわいい",
            "author_name": "太郎",
            "title": "動画 video1",
            "channel": "テストチャンネル",
            "url": "https://www.youtube.com/watch?v=video1",
            "date": "2026-09-01T00:00:00Z",
        }
    ]
    monkeypatch.setattr(pipeline, "extract_comments", lambda *args: comments)
    deleted = []
    monkeypatch.setattr(
        pipeline,
        "delete_video_json",
        lambda video_id, output_dir: deleted.append((video_id, Path(output_dir))),
    )

    result = pipeline.process_channel(
        "channel1",
        "2026-09-01",
        "2026-09-01",
        keywords=["かわいい"],
        db=db,
    )

    assert result == {"total": 1, "completed": 1, "excluded": 0, "retry": 0}
    assert db.is_completed("video1")
    assert not db.is_excluded("video1")
    assert db.get_all_comments()[0]["comment"] == "かわいい"
    assert deleted == [("video1", Path("json-output"))]


def test_process_channel_keeps_job_pending_on_processing_error(monkeypatch, tmp_path):
    db = CommentsDB(tmp_path / "comments.db")
    setup_common(monkeypatch, [video()])

    json_path = tmp_path / "video1.json"
    json_path.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(
        pipeline,
        "download_live_chat",
        lambda *args, **kwargs: DownloadResult(
            pipeline.SUCCESS,
            json_path=json_path,
        ),
    )
    monkeypatch.setattr(
        pipeline,
        "extract_comments",
        lambda *args: (_ for _ in ()).throw(RuntimeError("parse failed")),
    )
    delete = lambda *args: (_ for _ in ()).throw(
        AssertionError("JSON must not be deleted after failed processing")
    )
    monkeypatch.setattr(pipeline, "delete_video_json", delete)

    result = pipeline.process_channel(
        "channel1",
        "2026-09-01",
        "2026-09-01",
        keywords=["かわいい"],
        db=db,
    )

    assert result == {"total": 1, "completed": 0, "excluded": 0, "retry": 1}
    assert not db.is_completed("video1")
    assert not db.is_excluded("video1")
    assert json_path.exists()


def test_process_channel_handles_unknown_download_status(monkeypatch, tmp_path):
    db = CommentsDB(tmp_path / "comments.db")
    setup_common(monkeypatch, [video()])
    monkeypatch.setattr(
        pipeline,
        "download_live_chat",
        lambda *args, **kwargs: DownloadResult("unknown"),
    )

    result = pipeline.process_channel(
        "channel1",
        "2026-09-01",
        "2026-09-01",
        keywords=["かわいい"],
        db=db,
    )

    assert result == {"total": 1, "completed": 0, "excluded": 0, "retry": 1}


def test_process_channels_aggregates_stats(monkeypatch, tmp_path):
    db = CommentsDB(tmp_path / "comments.db")
    channels = [
        Channel("チャンネルA", "UC001"),
        Channel("チャンネルB", "UC002"),
    ]

    calls = []
    stats = {
        "UC001": {"total": 2, "completed": 1, "excluded": 1, "retry": 0},
        "UC002": {"total": 3, "completed": 2, "excluded": 0, "retry": 1},
    }

    def fake_process_channel(**kwargs):
        calls.append(kwargs)
        return stats[kwargs["channel_id"]]

    monkeypatch.setattr(pipeline, "process_channel", fake_process_channel)

    result = pipeline.process_channels(
        channels,
        "2026-09-01",
        "2026-09-02",
        keywords=["かわいい"],
        db=db,
    )

    assert result == {"total": 5, "completed": 3, "excluded": 1, "retry": 1}
    assert [call["channel_id"] for call in calls] == ["UC001", "UC002"]
    assert all(call["db"] is db for call in calls)
    assert all(call["keywords"] == ["かわいい"] for call in calls)
