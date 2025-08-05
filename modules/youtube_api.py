# modules/youtube_api.py

import os
import csv
import subprocess
import logging
from datetime import datetime
import sqlite3
from googleapiclient.discovery import build
from dotenv import load_dotenv

from myutils.youtube_api.fetch_youtube_data import YouTubeAPI

load_dotenv()

# ログ出力設定
logging.basicConfig(
    filename='logs/error_log.txt',
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)

API_KEY = os.getenv('YOUTUBE_API_KEY')
TARGET_CHANNEL = os.getenv('TARGET_CHANNEL')
PUBLISHED_BEFORE_DATE = os.getenv('PUBLISHED_BEFORE_DATE')
PUBLISHED_AFTER_DATE = os.getenv('PUBLISHED_AFTER_DATE')
CSV_FILENAME = os.getenv("CSV_FILENAME", "data/youtube_videos.csv")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "data/live_chat")


def save_to_csv(videos, filename=CSV_FILENAME):
    file_exists = os.path.isfile(filename)
    with open(filename, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["Title", "URL", "date"])
        for video in videos:
            writer.writerow([video["title"], video["url"], video["published_at"]])


# def get_videos_from_channel(channel_id, max_results=50):
#     youtube = build("youtube", "v3", developerKey=API_KEY)
#     next_page_token = None
#     videos = []

#     try:
#         while True:
#             request = youtube.search().list(
#                 part="id,snippet",
#                 channelId=channel_id,
#                 maxResults=max_results,
#                 order="date",
#                 publishedBefore=PUBLISHED_BEFORE_DATE,
#                 publishedAfter=PUBLISHED_AFTER_DATE,
#                 pageToken=next_page_token
#             )
#             response = request.execute()

#             for item in response.get("items", []):
#                 if item["id"]["kind"] == "youtube#video":
#                     video_id = item["id"]["videoId"]
#                     title = item["snippet"]["title"]
#                     published_at = item["snippet"]["publishedAt"]
#                     url = f"https://www.youtube.com/watch?v={video_id}"
#                     videos.append({
#                         "title": title,
#                         "url": url,
#                         "published_at": published_at
#                     })

#             next_page_token = response.get("nextPageToken")
#             if not next_page_token:
#                 break

#         save_to_csv(videos)

#     except Exception as e:
#         logging.error(f"エラー発生: {e}")
#         save_to_csv(videos)

def get_videos_from_channel(channel_id, max_results=50):
    api = YouTubeAPI()
    videos = []

    try:
        results = api.get_channel_videos_with_cache(
            channel_id=channel_id,
            start_date=PUBLISHED_AFTER_DATE,
            end_date=PUBLISHED_BEFORE_DATE
        )

        for row in results:
            video_id = row[0]  # videos.video_id
            title = row[1]     # videos.title
            published_at = row[3]  # videos.published_at
            url = f"https://www.youtube.com/watch?v={video_id}"

            videos.append({
                "title": title,
                "url": url,
                "published_at": published_at
            })

        save_to_csv(videos)

    except Exception as e:
        logging.error(f"エラー発生: {e}")
        save_to_csv(videos)


def download_live_chat(video_url):
    try:
        command = [
            "dl",  # yt-dlp コマンドのエイリアス
            "--skip-download",
            "--write-subs",
            "--sub-lang", "live_chat",
            "-o", f"{OUTPUT_DIR}/%(title)s [%(id)s].%(ext)s",
            "--cookies", "cookies.txt",
            video_url
        ]
        subprocess.run(command, check=True)
        print(f"✅ {video_url} のライブチャットを取得しました。")
    except subprocess.CalledProcessError as e:
        logging.error(f"エラー: {video_url} - {e}")


def get_chat_from_csv(filename=CSV_FILENAME):
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    if not os.path.isfile(filename):
        logging.error(f"{filename} が見つかりません。")
        return

    with open(filename, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            title, url, _ = row
            print(f"▶ {title} のライブチャットを取得中...")
            download_live_chat(url)

    print("🎉 すべてのライブチャット取得が完了しました！")
