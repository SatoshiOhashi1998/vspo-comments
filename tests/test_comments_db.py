import sqlite3

from modules.comments_db import CommentsDB


def make_comment(
    video_id="video1",
    timestamp="00:00:01",
    comment="かわいい",
    author_name="太郎",
    title="テスト動画",
    channel="テストチャンネル",
    url="https://www.youtube.com/watch?v=video1",
    date="2026-09-01T00:00:00Z",
):
    return {
        "video_id": video_id,
        "timestamp": timestamp,
        "comment": comment,
        "author_name": author_name,
        "title": title,
        "channel": channel,
        "url": url,
        "date": date,
    }


def test_db_creates_parent_directory_and_tables(tmp_path):
    db_path = tmp_path / "nested" / "comments.db"

    db = CommentsDB(db_path)

    assert db_path.exists()
    with sqlite3.connect(db_path) as conn:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }

    assert {"live_chat_jobs", "comments"}.issubset(tables)


def test_ensure_job_creates_pending_job(tmp_path):
    db = CommentsDB(tmp_path / "comments.db")

    db.ensure_job("video1")
    job = db.get_job("video1")

    assert job["video_id"] == "video1"
    assert job["completed"] == 0
    assert job["excluded"] == 0
    assert not db.is_completed("video1")
    assert not db.is_excluded("video1")


def test_ensure_job_does_not_overwrite_existing_state(tmp_path):
    db = CommentsDB(tmp_path / "comments.db")

    db.mark_completed("video1")
    db.ensure_job("video1")

    assert db.is_completed("video1")
    assert not db.is_excluded("video1")


def test_ensure_jobs_creates_multiple_jobs(tmp_path):
    db = CommentsDB(tmp_path / "comments.db")

    db.ensure_jobs(["video1", "video2", "video3"])

    assert [db.get_job(video_id)["video_id"] for video_id in ["video1", "video2", "video3"]] == [
        "video1",
        "video2",
        "video3",
    ]


def test_get_job_returns_none_for_unknown_video(tmp_path):
    db = CommentsDB(tmp_path / "comments.db")

    assert db.get_job("missing") is None
    assert not db.is_completed("missing")
    assert not db.is_excluded("missing")


def test_mark_completed_sets_completed_and_clears_excluded(tmp_path):
    db = CommentsDB(tmp_path / "comments.db")

    db.mark_excluded("video1")
    db.mark_completed("video1")

    job = db.get_job("video1")
    assert job["completed"] == 1
    assert job["excluded"] == 0


def test_mark_excluded_sets_excluded_and_clears_completed(tmp_path):
    db = CommentsDB(tmp_path / "comments.db")

    db.mark_completed("video1")
    db.mark_excluded("video1")

    job = db.get_job("video1")
    assert job["completed"] == 0
    assert job["excluded"] == 1


def test_save_comments_and_get_all_comments(tmp_path):
    db = CommentsDB(tmp_path / "comments.db")
    comments = [
        make_comment(timestamp="00:00:02", comment="あとから"),
        make_comment(timestamp="00:00:01", comment="先に"),
    ]

    db.save_comments(comments)
    rows = db.get_all_comments()

    assert len(rows) == 2
    assert {row["comment"] for row in rows} == {"あとから", "先に"}


def test_save_comments_accepts_empty_list(tmp_path):
    db = CommentsDB(tmp_path / "comments.db")

    db.save_comments([])

    assert db.get_all_comments() == []


def test_save_comments_ignores_duplicates(tmp_path):
    db = CommentsDB(tmp_path / "comments.db")
    comment = make_comment()

    db.save_comments([comment, comment.copy()])

    rows = db.get_all_comments()
    assert len(rows) == 1


def test_save_comments_uses_date_from_comment(tmp_path):
    db = CommentsDB(tmp_path / "comments.db")
    comment = make_comment(date="2026-09-10T00:00:00Z")

    db.save_comments([comment])

    row = db.get_all_comments()[0]
    assert row["date"] == "2026-09-10T00:00:00Z"


def test_search_comments_by_channel(tmp_path):
    db = CommentsDB(tmp_path / "comments.db")
    db.save_comments(
        [
            make_comment(channel="A", comment="かわいい"),
            make_comment(channel="B", comment="かわいい", timestamp="00:00:02"),
        ]
    )

    rows = db.search_comments(channel="A")

    assert len(rows) == 1
    assert rows[0]["channel"] == "A"


def test_search_comments_by_keyword(tmp_path):
    db = CommentsDB(tmp_path / "comments.db")
    db.save_comments(
        [
            make_comment(comment="かわいい", timestamp="00:00:01"),
            make_comment(comment="面白い", timestamp="00:00:02"),
        ]
    )

    rows = db.search_comments(keyword="かわいい")

    assert len(rows) == 1
    assert rows[0]["comment"] == "かわいい"


def test_search_comments_by_channel_and_keyword(tmp_path):
    db = CommentsDB(tmp_path / "comments.db")
    db.save_comments(
        [
            make_comment(channel="A", comment="かわいい", timestamp="00:00:01"),
            make_comment(channel="A", comment="面白い", timestamp="00:00:02"),
            make_comment(channel="B", comment="かわいい", timestamp="00:00:03"),
        ]
    )

    rows = db.search_comments(channel="A", keyword="かわいい")

    assert len(rows) == 1
    assert rows[0]["channel"] == "A"
    assert rows[0]["comment"] == "かわいい"


def test_search_comments_orders_by_date_then_id(tmp_path):
    db = CommentsDB(tmp_path / "comments.db")
    db.save_comments(
        [
            make_comment(
                comment="新しい",
                timestamp="00:00:01",
                date="2026-09-02T00:00:00Z",
            ),
            make_comment(
                comment="古い",
                timestamp="00:00:02",
                date="2026-09-01T00:00:00Z",
            ),
        ]
    )

    rows = db.search_comments()

    assert [row["comment"] for row in rows] == ["古い", "新しい"]
