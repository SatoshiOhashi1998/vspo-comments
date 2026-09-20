from __future__ import annotations

import os
import sqlite3
from typing import Iterable


class CommentsDB:
    def __init__(self, db_path: str | None = None):
        self.db_path = db_path or os.getenv("COMMENTS_DB_PATH", "data/comments.db")
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        parent = os.path.dirname(os.path.abspath(self.db_path))
        if parent:
            os.makedirs(parent, exist_ok=True)

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS live_chat_jobs (
                    video_id TEXT PRIMARY KEY,
                    completed INTEGER NOT NULL DEFAULT 0,
                    excluded INTEGER NOT NULL DEFAULT 0,
                    CHECK (completed IN (0, 1)),
                    CHECK (excluded IN (0, 1))
                )
                """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS comments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    video_id TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    comment TEXT NOT NULL,
                    author_name TEXT NOT NULL,
                    title TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    url TEXT NOT NULL,
                    date TEXT,
                    UNIQUE(video_id, timestamp, author_name, comment)
                )
                """
            )

            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_jobs_completed "
                "ON live_chat_jobs(completed, excluded)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_comments_video_id "
                "ON comments(video_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_comments_channel "
                "ON comments(channel)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_comments_date "
                "ON comments(date)"
            )

    def ensure_job(self, video_id: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO live_chat_jobs(video_id, completed, excluded)
                VALUES (?, 0, 0)
                """,
                (video_id,),
            )

    def ensure_jobs(self, video_ids: Iterable[str]) -> None:
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT OR IGNORE INTO live_chat_jobs(video_id, completed, excluded)
                VALUES (?, 0, 0)
                """,
                ((video_id,) for video_id in video_ids),
            )

    def get_job(self, video_id: str) -> sqlite3.Row | None:
        with self._connect() as conn:
            return conn.execute(
                "SELECT * FROM live_chat_jobs WHERE video_id = ?",
                (video_id,),
            ).fetchone()

    def is_completed(self, video_id: str) -> bool:
        job = self.get_job(video_id)
        return bool(job and job["completed"])

    def is_excluded(self, video_id: str) -> bool:
        job = self.get_job(video_id)
        return bool(job and job["excluded"])

    def mark_completed(self, video_id: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO live_chat_jobs(video_id, completed, excluded)
                VALUES (?, 1, 0)
                ON CONFLICT(video_id) DO UPDATE SET
                    completed = 1,
                    excluded = 0
                """,
                (video_id,),
            )

    def mark_excluded(self, video_id: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO live_chat_jobs(video_id, completed, excluded)
                VALUES (?, 0, 1)
                ON CONFLICT(video_id) DO UPDATE SET
                    completed = 0,
                    excluded = 1
                """,
                (video_id,),
            )

    def save_comments(self, comments: list[dict]) -> None:
        if not comments:
            return

        rows = [
            (
                comment["video_id"],
                comment["timestamp"],
                comment["comment"],
                comment["author_name"],
                comment["title"],
                comment["channel"],
                comment["url"],
                comment.get("date"),
            )
            for comment in comments
        ]

        with self._connect() as conn:
            conn.executemany(
                """
                INSERT OR IGNORE INTO comments(
                    video_id,
                    timestamp,
                    comment,
                    author_name,
                    title,
                    channel,
                    url,
                    date
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

    def search_comments(
        self,
        channel: str | None = None,
        keyword: str | None = None,
    ) -> list[sqlite3.Row]:
        query = "SELECT * FROM comments WHERE 1=1"
        params: list[str] = []

        if channel:
            query += " AND channel = ?"
            params.append(channel)

        if keyword:
            query += " AND comment LIKE ?"
            params.append(f"%{keyword}%")

        query += " ORDER BY date ASC, id ASC"

        with self._connect() as conn:
            return conn.execute(query, params).fetchall()

    def get_all_comments(self) -> list[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute(
                "SELECT * FROM comments ORDER BY date ASC, id ASC"
            ).fetchall()
