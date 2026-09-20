from __future__ import annotations

import csv
import os
from pathlib import Path

from .comments_db import CommentsDB


def parse_timestamp(timestamp: str) -> int | None:
    if not timestamp:
        return None

    text = timestamp.strip()
    sign = -1 if text.startswith("-") else 1
    text = text.lstrip("-")

    try:
        parts = [int(part) for part in text.split(":")]
    except ValueError:
        return None

    if len(parts) == 3:
        hours, minutes, seconds = parts
    elif len(parts) == 2:
        hours = 0
        minutes, seconds = parts
    elif len(parts) == 1:
        hours = minutes = 0
        seconds = parts[0]
    else:
        return None

    return sign * (hours * 3600 + minutes * 60 + seconds)


def build_timestamp_url(video_id: str, timestamp: str) -> str:
    base_url = f"https://www.youtube.com/watch?v={video_id}"
    seconds = parse_timestamp(timestamp)
    if seconds is None or seconds < 0:
        return base_url
    return f"{base_url}&t={seconds}s"


def export_comments_to_csv(
    output_csv: str,
    db: CommentsDB | None = None,
    channel: str | None = None,
    keyword: str | None = None,
) -> str:
    db = db or CommentsDB()
    rows = db.search_comments(channel=channel, keyword=keyword)

    output_path = Path(output_csv)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "Title",
        "URL",
        "timestamp",
        "Comment",
        "Author",
        "Channel",
        "Date",
    ]

    with output_path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()

        for row in rows:
            writer.writerow(
                {
                    "Title": row["title"],
                    "URL": build_timestamp_url(
                        row["video_id"],
                        row["timestamp"],
                    ),
                    "timestamp": row["timestamp"],
                    "Comment": row["comment"],
                    "Author": row["author_name"],
                    "Channel": row["channel"],
                    "Date": row["date"] or "",
                }
            )

    print(f"CSVを出力しました: {output_path}")
    print(f"コメント数: {len(rows)}")
    return str(output_path)
