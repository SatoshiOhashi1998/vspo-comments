from __future__ import annotations

import json
from pathlib import Path


def _walk(value):
    """JSONデータ内のdictを再帰的に走査する。"""
    if isinstance(value, dict):
        yield value

        for child in value.values():
            yield from _walk(child)

    elif isinstance(value, list):
        for child in value:
            yield from _walk(child)


def _simple_text(value) -> str:
    """
    YouTubeのsimpleText / runs形式などから文字列を取得する。
    """
    if value is None:
        return ""

    if isinstance(value, str):
        return value

    if isinstance(value, dict):
        simple_text = value.get("simpleText")

        if isinstance(simple_text, str):
            return simple_text

        runs = value.get("runs")

        if isinstance(runs, list):
            return "".join(
                run.get("text", "")
                for run in runs
                if isinstance(run, dict)
            )

    return ""


def _timestamp_from_renderer(renderer: dict) -> str | None:
    """
    チャットrendererから配信内のタイムスタンプを取得する。
    """
    timestamp = _simple_text(
        renderer.get("timestampText")
    )

    if timestamp:
        return timestamp

    timestamp_usec = renderer.get("timestampUsec")

    if timestamp_usec is not None:
        try:
            total_seconds = int(timestamp_usec) // 1_000_000

            hours, remainder = divmod(
                total_seconds,
                3600,
            )

            minutes, seconds = divmod(
                remainder,
                60,
            )

            return (
                f"{hours:02d}:"
                f"{minutes:02d}:"
                f"{seconds:02d}"
            )

        except (TypeError, ValueError):
            pass

    return None


def _load_live_chat_json(json_path: str | Path):
    """
    yt-dlpが出力するlive_chat JSONを読み込む。

    live_chatのファイルは、通常のJSONファイルのように
    1つのJSONオブジェクトだけが格納されているとは限らず、
    複数のJSONオブジェクトが連続して保存されている。

    そのためjson.load()ではなくJSONDecoder.raw_decode()
    を使って順番に読み込む。
    """
    path = Path(json_path)

    with path.open(
        "r",
        encoding="utf-8-sig",
    ) as file:
        text = file.read()

    decoder = json.JSONDecoder()

    values = []
    position = 0
    length = len(text)

    while position < length:
        # 空白や改行をスキップ
        while (
            position < length
            and text[position].isspace()
        ):
            position += 1

        if position >= length:
            break

        value, position = decoder.raw_decode(
            text,
            position,
        )

        values.append(value)

    if not values:
        return []

    # JSONが1個だけなら、そのまま返す。
    if len(values) == 1:
        return values[0]

    # 複数JSONならlistとして返す。
    return values


def _message_renderers(data: object):
    """
    JSONデータからコメントrendererを抽出する。
    """
    for node in _walk(data):
        for key, value in node.items():
            if (
                not key.endswith("Renderer")
                or not isinstance(value, dict)
            ):
                continue

            message = value.get("message")
            author_name = _simple_text(
                value.get("authorName")
            )

            if message is None or not author_name:
                continue

            comment = _simple_text(message).strip()
            timestamp = _timestamp_from_renderer(value)

            if not comment or not timestamp:
                continue

            yield {
                "timestamp": timestamp,
                "comment": comment,
                "author_name": author_name,
            }


def extract_comments(
    json_path: str | Path,
    keywords: list[str],
    video: dict,
) -> list[dict]:
    """
    live_chat JSONから指定キーワードを含むコメントだけを抽出する。

    キーワードは複数指定可能。
    どれか1つでもコメントに含まれていれば採用する。
    """
    keywords = [
        keyword.strip()
        for keyword in keywords
        if keyword.strip()
    ]

    if not keywords:
        raise ValueError(
            "At least one comment keyword is required."
        )

    path = Path(json_path)

    data = _load_live_chat_json(path)

    results = []

    for item in _message_renderers(data):
        comment_text = item["comment"]

        if not any(
            keyword in comment_text
            for keyword in keywords
        ):
            continue

        results.append(
            {
                "video_id": video["video_id"],
                "timestamp": item["timestamp"],
                "comment": comment_text,
                "author_name": item["author_name"],
                "title": video["title"],
                "channel": video["channel"],
                "url": video["url"],
                "date": video.get("published_at"),
            }
        )

    return results
