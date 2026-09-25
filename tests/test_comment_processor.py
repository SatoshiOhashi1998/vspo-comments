import json

import pytest

from modules.comment_processor import (
    _load_live_chat_json,
    _message_renderers,
    _simple_text,
    _timestamp_from_renderer,
    _walk,
    extract_comments,
)


def test_walk_recursively_yields_dicts():
    data = {"a": {"b": 1}, "c": [{"d": 2}]}

    result = list(_walk(data))

    assert result == [
        data,
        {"b": 1},
        {"d": 2},
    ]


def test_simple_text_accepts_string():
    assert _simple_text("hello") == "hello"


def test_simple_text_accepts_simple_text():
    assert _simple_text({"simpleText": "hello"}) == "hello"


def test_simple_text_joins_runs():
    value = {"runs": [{"text": "こん"}, {"text": "にちは"}]}

    assert _simple_text(value) == "こんにちは"


def test_simple_text_ignores_invalid_runs():
    value = {"runs": [{"text": "a"}, "invalid", {"other": "b"}]}

    assert _simple_text(value) == "a"


def test_simple_text_returns_empty_for_unsupported_value():
    assert _simple_text(None) == ""
    assert _simple_text(123) == ""
    assert _simple_text({}) == ""


def test_timestamp_uses_timestamp_text_first():
    renderer = {
        "timestampText": {"simpleText": "01:23:45"},
        "timestampUsec": "99999999",
    }

    assert _timestamp_from_renderer(renderer) == "01:23:45"


def test_timestamp_converts_timestamp_usec():
    renderer = {"timestampUsec": 3723000000}

    assert _timestamp_from_renderer(renderer) == "01:02:03"


def test_timestamp_supports_hours_over_24():
    renderer = {"timestampUsec": 90061000000}

    assert _timestamp_from_renderer(renderer) == "25:01:01"


def test_timestamp_returns_none_for_invalid_usec():
    assert _timestamp_from_renderer({"timestampUsec": "invalid"}) is None


def test_timestamp_returns_none_when_missing():
    assert _timestamp_from_renderer({}) is None


def test_load_live_chat_json_reads_single_json(tmp_path):
    path = tmp_path / "chat.json"
    path.write_text(json.dumps({"actions": []}), encoding="utf-8")

    assert _load_live_chat_json(path) == {"actions": []}


def test_load_live_chat_json_reads_consecutive_json_objects(tmp_path):
    path = tmp_path / "chat.json"
    path.write_text(
        '{"id": 1}\n{"id": 2}\n',
        encoding="utf-8",
    )

    assert _load_live_chat_json(path) == [{"id": 1}, {"id": 2}]


def test_load_live_chat_json_accepts_utf8_bom(tmp_path):
    path = tmp_path / "chat.json"
    path.write_text('{"id": 1}', encoding="utf-8-sig")

    assert _load_live_chat_json(path) == {"id": 1}


def test_load_live_chat_json_returns_empty_list_for_empty_file(tmp_path):
    path = tmp_path / "chat.json"
    path.write_text("", encoding="utf-8")

    assert _load_live_chat_json(path) == []


def test_message_renderers_extracts_comment():
    data = {
        "liveChatTextMessageRenderer": {
            "message": {"runs": [{"text": "こんにちは"}]},
            "authorName": {"simpleText": "太郎"},
            "timestampText": {"simpleText": "00:01:02"},
        }
    }

    assert list(_message_renderers(data)) == [
        {
            "timestamp": "00:01:02",
            "comment": "こんにちは",
            "author_name": "太郎",
        }
    ]


def test_message_renderers_strips_comment_whitespace():
    data = {
        "liveChatTextMessageRenderer": {
            "message": {"simpleText": "  かわいい  "},
            "authorName": {"simpleText": "太郎"},
            "timestampText": {"simpleText": "00:00:01"},
        }
    }

    result = list(_message_renderers(data))

    assert result[0]["comment"] == "かわいい"


@pytest.mark.parametrize(
    "renderer",
    [
        {},
        {"message": {"simpleText": "hello"}},
        {
            "message": {"simpleText": "hello"},
            "authorName": {"simpleText": ""},
            "timestampText": {"simpleText": "00:00:01"},
        },
        {
            "message": {"simpleText": ""},
            "authorName": {"simpleText": "太郎"},
            "timestampText": {"simpleText": "00:00:01"},
        },
        {
            "message": {"simpleText": "hello"},
            "authorName": {"simpleText": "太郎"},
        },
    ],
)
def test_message_renderers_skips_incomplete_renderer(renderer):
    data = {"liveChatTextMessageRenderer": renderer}

    assert list(_message_renderers(data)) == []


def make_chat_json(path, comments):
    actions = []
    for comment in comments:
        actions.append(
            {
                "addChatItemAction": {
                    "item": {
                        "liveChatTextMessageRenderer": {
                            "message": {"simpleText": comment["comment"]},
                            "authorName": {"simpleText": comment["author"]},
                            "timestampText": {"simpleText": comment["timestamp"]},
                        }
                    }
                }
            }
        )
    path.write_text(json.dumps({"actions": actions}, ensure_ascii=False), encoding="utf-8")


def sample_video():
    return {
        "video_id": "video1",
        "title": "テスト動画",
        "channel": "テストチャンネル",
        "published_at": "2026-09-01T00:00:00Z",
        "url": "https://www.youtube.com/watch?v=video1",
    }


def test_extract_comments_filters_by_any_keyword(tmp_path):
    path = tmp_path / "chat.json"
    make_chat_json(
        path,
        [
            {"comment": "かわいいですね", "author": "A", "timestamp": "00:00:01"},
            {"comment": "これは普通", "author": "B", "timestamp": "00:00:02"},
            {"comment": "めっちゃ草", "author": "C", "timestamp": "00:00:03"},
        ],
    )

    result = extract_comments(path, ["かわいい", "草"], sample_video())

    assert result == [
        {
            "video_id": "video1",
            "timestamp": "00:00:01",
            "comment": "かわいいですね",
            "author_name": "A",
            "title": "テスト動画",
            "channel": "テストチャンネル",
            "url": "https://www.youtube.com/watch?v=video1",
            "date": "2026-09-01T00:00:00Z",
        },
        {
            "video_id": "video1",
            "timestamp": "00:00:03",
            "comment": "めっちゃ草",
            "author_name": "C",
            "title": "テスト動画",
            "channel": "テストチャンネル",
            "url": "https://www.youtube.com/watch?v=video1",
            "date": "2026-09-01T00:00:00Z",
        },
    ]


def test_extract_comments_strips_empty_keywords(tmp_path):
    path = tmp_path / "chat.json"
    make_chat_json(
        path,
        [{"comment": "かわいい", "author": "A", "timestamp": "00:00:01"}],
    )

    result = extract_comments(path, ["", "  ", " かわいい "], sample_video())

    assert len(result) == 1


def test_extract_comments_raises_for_no_keywords(tmp_path):
    path = tmp_path / "chat.json"
    path.write_text("{}", encoding="utf-8")

    with pytest.raises(ValueError, match="At least one comment keyword is required"):
        extract_comments(path, ["", "  "], sample_video())


def test_extract_comments_returns_empty_when_no_comment_matches(tmp_path):
    path = tmp_path / "chat.json"
    make_chat_json(
        path,
        [{"comment": "普通のコメント", "author": "A", "timestamp": "00:00:01"}],
    )

    assert extract_comments(path, ["存在しない"], sample_video()) == []
