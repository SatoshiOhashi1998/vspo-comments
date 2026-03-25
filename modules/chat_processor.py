# modules/chat_processor.py

import os
import glob
import json
import csv
import re
import sqlite3
import logging
import unicodedata
import pandas as pd
import ctypes
from dotenv import load_dotenv
import orjson

load_dotenv()

# ログ設定
logging.basicConfig(
    filename='logs/error_log.txt',
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)

# 環境変数
JSON_DIRECTORY = os.getenv('JSON_DIRECTORY', 'data/live_chat')
SAVE_CHANNEL = os.getenv('SAVE_CHANNEL', '一ノ瀬うるは')
DB_FILE = os.getenv('DB_FILE', 'data/comments.db')
FILTERED_DB_FILE = os.getenv('FILTERED_DB_FILE', 'data/filtered_comments.db')
FILTERED_DATA = os.getenv('FILTERED_DATA', 'data/comments.csv')
GETED_DATA = os.getenv('GETED_DATA', 'data/youtube_videos.csv')
COMMENT_KEYWORD = os.getenv('COMMENT_KEYWORD')


def create_db(db=DB_FILE):
    """
    コメント保存用DBの初期化。
    video_id を追加し、重複判定の精度を向上させます。
    """
    with sqlite3.connect(db) as conn:
        c = conn.cursor()

        # テーブル作成
        c.execute('''
            CREATE TABLE IF NOT EXISTS comments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                video_id TEXT NOT NULL,         -- YouTubeの動画ID (11桁)
                timestamp INTEGER NOT NULL,     -- 配信開始からの経過秒
                comment TEXT NOT NULL,          -- コメント本文
                author_name TEXT,               -- ★投稿者名（重複回避の精度向上のため）
                title TEXT,                     -- 動画タイトル（表示用）
                channel TEXT,                   -- チャンネル名
                url TEXT,                       -- 動画URL
                date TEXT,                      -- 投稿日
                -- 同時多発コメントを許容しつつ、同じ実行での二重登録を防ぐ
                -- ※author_nameを含めることで、同じ秒数の別人の「草」を保存可能に
                UNIQUE(video_id, timestamp, author_name, comment)
            )
        ''')

        # 検索を高速化するためのインデックス
        c.execute('CREATE INDEX IF NOT EXISTS idx_comments_video_id ON comments(video_id)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_comments_comment ON comments(comment)')
        
        conn.commit()


def getVideoData():
    try:
        return pd.read_csv(GETED_DATA)
    except FileNotFoundError:
        logging.error(f"{GETED_DATA} が見つかりません。")
        return pd.DataFrame()


def extract_comments_from_json(json_file, channel_name):
    comments = []
    df = getVideoData()

    try:
        with open(json_file, "rb") as f:
            for line in f:
                try:
                    data = orjson.loads(line)
                    actions = data.get("replayChatItemAction", {}).get("actions", [])

                    for action in actions:
                        renderer = action.get("addChatItemAction", {}).get("item", {}).get("liveChatTextMessageRenderer", {})
                        message_data = renderer.get("message", {})

                        if 'emoji' in message_data:
                            continue

                        comment_text = "".join(run.get("text", "") for run in message_data.get("runs", []))
                        timestamp_text = renderer.get("timestampText", {}).get("simpleText", "")
                        timestamp = parse_timestamp(timestamp_text)

                        insert_title = os.path.splitext(os.path.basename(json_file))[0].strip().replace('⧸', '/')
                        insert_title = unicodedata.normalize('NFKC', insert_title)
                        df['Title'] = df['Title'].apply(lambda x: unicodedata.normalize('NFKC', x))
                        filtered_df = df[df['Title'] == insert_title]

                        if not filtered_df.empty:
                            comments.append({
                                "timestamp": timestamp,
                                "comment": comment_text,
                                "title": insert_title,
                                "channel": channel_name,
                                "url": filtered_df['URL'].values[0],
                                "date": filtered_df['date'].values[0]
                            })
                except orjson.JSONDecodeError as e:
                    logging.error(f"JSONエラー: {e}")
    except Exception as e:
        logging.error(f"{json_file} の処理中にエラー: {e}")

    return comments


def parse_timestamp(timestamp_text: str) -> int:
    try:
        negative = timestamp_text.startswith("-")
        parts = timestamp_text.lstrip("-").split(":")
        parts = list(map(int, parts))
        seconds = 0
        if len(parts) == 3:
            seconds = parts[0] * 3600 + parts[1] * 60 + parts[2]
        elif len(parts) == 2:
            seconds = parts[0] * 60 + parts[1]
        return -seconds if negative else seconds
    except Exception:
        return 0


def save_comments_to_db(comments, db=DB_FILE):
    """
    抽出したコメントリストをDBに一括保存します。
    """
    if not comments:
        return

    with sqlite3.connect(db) as conn:
        c = conn.cursor()
        try:
            c.executemany('''
                INSERT OR IGNORE INTO comments (
                    video_id, timestamp, comment, author_name, title, channel, url, date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', [
                (
                    c["video_id"], c["timestamp"], c["comment"], c.get("author_name"),
                    c["title"], c["channel"], c["url"], c["date"]
                ) for c in comments
            ])
            conn.commit()
        except sqlite3.Error as e:
            logging.error(f"DB保存エラー: {e}")


def process_json_files(directory, channel_name):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    for filename in os.listdir(directory):
        if filename.endswith(".json"):
            json_file = os.path.join(directory, filename)

            # ファイル名からタイトル抽出（正規化）
            title = os.path.splitext(filename)[0].strip().replace('⧸', '/')
            title = unicodedata.normalize('NFKC', title)

            # 既にこのタイトルのコメントが登録されているか？
            c.execute("SELECT 1 FROM comments WHERE title = ? LIMIT 1", (title,))
            if c.fetchone():
                print(f"⏭ スキップ: {filename}（既に登録済み）")
                continue

            print(f"▶ {filename} を処理中...")
            comments = extract_comments_from_json(json_file, channel_name)
            if comments:
                save_comments_to_db(comments)

    conn.close()


def rename_json():
    pattern = re.compile(r" \[[a-zA-Z0-9_-]+\]\.live_chat\.json$")
    for filename in os.listdir(JSON_DIRECTORY):
        if filename.endswith(".json"):
            new_filename = pattern.sub(".json", filename)
            old_path = os.path.join(JSON_DIRECTORY, filename)
            new_path = os.path.join(JSON_DIRECTORY, new_filename)
            if old_path != new_path and not os.path.exists(new_path):
                os.rename(old_path, new_path)
                print(f"Renamed: {filename} -> {new_filename}")


def search_comments(db=DB_FILE, channel=None, title=None, date=None, comment=COMMENT_KEYWORD):
    query = "SELECT * FROM comments WHERE 1=1"
    params = []
    if channel:
        query += " AND channel = ?"
        params.append(channel)
    if title:
        query += " AND title LIKE ?"
        params.append(f"%{title}%")
    if date:
        query += " AND date = ?"
        params.append(date)
    if comment:
        query += " AND comment LIKE ?"
        params.append(f"%{comment}%")

    try:
        conn = sqlite3.connect(db)
        c = conn.cursor()
        c.execute(query, params)
        results = c.fetchall()
        conn.close()
        return results
    except sqlite3.Error as e:
        logging.error(f"検索失敗: {e}")
        return []


def save_to_csv(data, filename=FILTERED_DATA):
    if not data:
        return

    with open(filename, "w", newline='', encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["ID", "Timestamp", "Comment", "Title", "Channel", "URL", "Date"])

        for row in data:
            (
                id,
                timestamp,
                comment,
                title,
                channel,
                url,
                date
            ) = row

            # 時間指定URLを生成（負の値は0に補正）
            timestamp_int = int(timestamp) if isinstance(timestamp, (int, float)) else 0
            timestamp_url = f"{url}&t={max(0, timestamp_int)}s" if url else ""

            writer.writerow([
                id,
                timestamp_int,     # Timestamp列に元の秒数
                comment,
                title,
                channel,
                timestamp_url,     # URL列に時間指定付きURL
                date
            ])


def clean_data_files():
    json_files = glob.glob(os.path.join(JSON_DIRECTORY, '*.json'))
    for file in json_files:
        os.remove(file)

    csv_path = GETED_DATA

    df = pd.read_csv(csv_path, nrows=0)

    df.to_csv(csv_path, index=False)


def migrate_filtered_data(src_db=DB_FILE, dest_db=FILTERED_DB_FILE, comment_keyword=COMMENT_KEYWORD, channel_val=os.getenv('DEFAULT_CHANNEL')):
    # 元DBからLIKE検索でデータ取得
    src_conn = sqlite3.connect(src_db)
    src_c = src_conn.cursor()
    src_c.execute('''
        SELECT timestamp, comment, title, channel, url, date
        FROM comments
        WHERE comment LIKE ? AND channel = ?
    ''', (f'%{comment_keyword}%', channel_val))
    rows = src_c.fetchall()
    src_conn.close()

    # 新DB作成してデータをコピー
    create_db(dest_db)
    dest_conn = sqlite3.connect(dest_db)
    dest_c = dest_conn.cursor()
    dest_c.executemany('''
        INSERT OR IGNORE INTO comments (
            timestamp, comment, title, channel, url, date
        ) VALUES (?, ?, ?, ?, ?, ?)
    ''', rows)
    dest_conn.commit()
    dest_conn.close()
