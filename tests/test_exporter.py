import csv

import pytest

from modules.comments_db import CommentsDB
from modules.exporter import (
    build_timestamp_url,
    export_comments_to_csv,
    parse_timestamp,
)


def make_comment(**overrides):
    data = {
        "video_id": "video1",
        "timestamp": "01:02:03",
        "comment": "かわいい",
        "author_name": "太郎",
        "title": "テスト動画",
        "channel": "テストチャンネル",
        "url": "https://www.youtube.com/watch?v=video1",
        "date": "2026-09-01T00:00:00Z",
    }
    data.update(overrides)
    return data


@pytest.mark.parametrize(
    ("timestamp", "expected"),
    [
        ("01:02:03", 3723),
        ("02:03", 123),
        ("03", 3),
        (" 01:02:03 ", 3723),
        ("-01:02", -62),
        ("0", 0),
    ],
)
def test_parse_timestamp(timestamp, expected):
    assert parse_timestamp(timestamp) == expected


@pytest.mark.parametrize("timestamp", [None, "", "abc", "1:2:3:4"])
def test_parse_timestamp_returns_none_for_invalid(timestamp):
    assert parse_timestamp(timestamp) is None


def test_build_timestamp_url_adds_seconds():
    assert build_timestamp_url("abc123", "01:02:03") == (
        "https://www.youtube.com/watch?v=abc123&t=3723s"
    )


@pytest.mark.parametrize("timestamp", ["-01:02", "invalid", ""])
def test_build_timestamp_url_returns_base_url_for_unusable_timestamp(timestamp):
    assert build_timestamp_url("abc123", timestamp) == (
        "https://www.youtube.com/watch?v=abc123"
    )


def test_export_comments_to_csv_writes_expected_columns(tmp_path):
    db = CommentsDB(tmp_path / "comments.db")
    db.save_comments([make_comment()])
    output = tmp_path / "out" / "comments.csv"

    result = export_comments_to_csv(str(output), db=db)

    assert result == str(output)
    assert output.exists()

    with output.open("r", encoding="utf-8-sig", newline="") as file:
        rows = list(csv.DictReader(file))

    assert rows == [
        {
            "Title": "テスト動画",
            "URL": "https://www.youtube.com/watch?v=video1&t=3723s",
            "timestamp": "01:02:03",
            "Comment": "かわいい",
            "Author": "太郎",
            "Channel": "テストチャンネル",
            "Date": "2026-09-01T00:00:00Z",
        }
    ]


def test_export_comments_to_csv_supports_filters(tmp_path):
    db = CommentsDB(tmp_path / "comments.db")
    db.save_comments(
        [
            make_comment(channel="A", comment="かわいい"),
            make_comment(channel="A", comment="面白い", timestamp="00:00:02"),
            make_comment(channel="B", comment="かわいい", timestamp="00:00:03"),
        ]
    )
    output = tmp_path / "comments.csv"

    export_comments_to_csv(
        str(output),
        db=db,
        channel="A",
        keyword="かわいい",
    )

    with output.open("r", encoding="utf-8-sig", newline="") as file:
        rows = list(csv.DictReader(file))

    assert len(rows) == 1
    assert rows[0]["Channel"] == "A"
    assert rows[0]["Comment"] == "かわいい"


def test_export_comments_to_csv_handles_empty_db(tmp_path):
    db = CommentsDB(tmp_path / "comments.db")
    output = tmp_path / "comments.csv"

    export_comments_to_csv(str(output), db=db)

    with output.open("r", encoding="utf-8-sig", newline="") as file:
        rows = list(csv.DictReader(file))

    assert rows == []
