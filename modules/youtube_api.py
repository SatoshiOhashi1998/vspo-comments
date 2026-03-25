# modules/youtube_api.py

import os
import csv
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from myutils.youtube_api.fetch_youtube_data import YouTubeAPI
from yt_dlp import YoutubeDL

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

# yt-dlpのオプション設定
ydl_opts = {
    "skip_download": True,
    "writesubtitles": True,
    "subtitleslangs": ["live_chat"],
    "outtmpl": f"{OUTPUT_DIR}/%(title)s [%(id)s].%(ext)s",
    "cookiefile": "cookies.txt",
}


def save_to_csv(videos, filename=CSV_FILENAME):
    file_exists = os.path.isfile(filename)
    with open(filename, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["Title", "URL", "date"])
        for video in videos:
            writer.writerow([video["title"], video["url"], video["published_at"]])


def get_videos_from_channel(channel_id, max_results=50):
    api = YouTubeAPI()
    videos = []

    try:
        # データを完全に取得する用
        # results = api.fetch_and_save_videos_from_channel(
        #     channel_id=channel_id,
        #     published_after=PUBLISHED_AFTER_DATE,
        #     published_before=PUBLISHED_BEFORE_DATE
        # )

        # APIのリソース確保のため基本はこちらを使う
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
    with YoutubeDL(ydl_opts) as ydl:
        try:
            ydl.download([video_url])
            print(f"✅ {video_url} のライブチャットを取得しました。")
        except Exception as e:
            logging.error(f"エラー: {video_url} - {e}")


def get_chat_from_csv(filename=CSV_FILENAME, max_workers=5):
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    if not os.path.isfile(filename):
        logging.error(f"{filename} が見つかりません。")
        return

    with open(filename, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)
        video_list = [(row[0], row[1]) for row in reader]

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(download_live_chat, url): title for title, url in video_list}

        for future in as_completed(futures):
            title = futures[future]
            try:
                future.result()
            except Exception as e:
                logging.error(f"{title} の取得中にエラー発生: {e}")

    print("🎉 すべてのライブチャット取得が完了しました！")
