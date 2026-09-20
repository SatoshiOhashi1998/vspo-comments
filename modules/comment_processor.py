from __future__ import annotations

import json
from pathlib import Path


def _walk(value):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk(child)


def _simple_text(value) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        if isinstance(value.get("simpleText"), str):
            return value["simpleText"]
        runs = value.get("runs")
        if isinstance(runs, list):
            return "".join(
                run.get("text", "")
                for run in runs
                if isinstance(run, dict)
            )
    return ""


def _timestamp_from_renderer(renderer: dict) -> str | None:
    timestamp = _simple_text(renderer.get("timestampText"))
    if timestamp:
        return timestamp

    # timestampUsecは絶対時刻ではなく、配信内の時刻として扱えるケースがある。
    timestamp_usec = renderer.get("timestampUsec")
    if timestamp_usec is not None:
        try:
            total_seconds = int(timestamp_usec) // 1_000_000
            hours, remainder = divmod(total_seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        except (TypeError, ValueError):
            pass

    return None


def _message_renderers(data: object):
    for node in _walk(data):
        for key, value in node.items():
            if not key.endswith("Renderer") or not isinstance(value, dict):
                continue

            message = value.get("message")
            author_name = _simple_text(value.get("authorName"))

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
    keywords = [keyword.strip() for keyword in keywords if keyword.strip()]
    if not keywords:
        raise ValueError("At least one comment keyword is required.")

    path = Path(json_path)
    with path.open("r", encoding="utf-8") as file:
        data = json.load(file)

    results = []
    for item in _message_renderers(data):
        if not any(keyword in item["comment"] for keyword in keywords):
            continue

        results.append(
            {
                "video_id": video["video_id"],
                "timestamp": item["timestamp"],
                "comment": item["comment"],
                "author_name": item["author_name"],
                "title": video["title"],
                "channel": video["channel"],
                "url": video["url"],
                "date": video.get("published_at"),
            }
        )

    return results
