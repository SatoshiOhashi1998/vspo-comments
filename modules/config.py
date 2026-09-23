import csv
import os
from pathlib import Path

from dotenv import load_dotenv

from .channel import Channel

load_dotenv()


YOUTUBE_DB_PATH = os.getenv("YOUTUBE_DB_PATH", "youtube.db")
COMMENTS_DB_PATH = os.getenv("COMMENTS_DB_PATH", "data/comments.db")

CHANNEL_DATAS = os.getenv("CHANNEL_DATAS", "data/channels.csv")
JSON_DIRECTORY = Path(os.getenv("JSON_DIRECTORY", "data/live_chat"))
FILTERED_DATA = os.getenv("FILTERED_DATA", "data/comments.csv")

PUBLISHED_AFTER_DATE = os.getenv("PUBLISHED_AFTER_DATE")
PUBLISHED_BEFORE_DATE = os.getenv("PUBLISHED_BEFORE_DATE")

# 例: COMMENT_KEYWORD=かわいい,草,面白い
COMMENT_KEYWORD = os.getenv("COMMENT_KEYWORD", "")
COMMENT_KEYWORDS = [
    keyword.strip()
    for keyword in COMMENT_KEYWORD.split(",")
    if keyword.strip()
]

SUB_KEYWORD = os.getenv("SUB_KEYWORD", "")
SUB_DATAS = os.getenv("SUB_DATAS", "data/sub_comments.csv")

COOKIES_FILE = os.getenv("COOKIES_FILE", "cookies.txt")


def validate() -> None:
    if not COMMENT_KEYWORDS:
        raise ValueError(
            "COMMENT_KEYWORD is required. "
            "Specify one or more keywords separated by commas."
        )


def get_channels(csv_path: str = CHANNEL_DATAS) -> list[Channel]:
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as file:
        return [
            Channel(
                channel_name=row["channel_name"],
                channel_id=row["channel_id"],
            )
            for row in csv.DictReader(file)
        ]


def find_channels_by_name(
    partial_name: str,
    csv_path: str = CHANNEL_DATAS,
) -> list[Channel]:
    channels = get_channels(csv_path)

    return [
        channel
        for channel in channels
        if partial_name.lower() in channel.channel_name.lower()
    ]
