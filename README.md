# vspo-comments refactor

## 構成

- `config.py`: 環境変数
- `youtube.py`: youtube.dbから対象動画を取得
- `comments_db.py`: comments.dbの管理
- `live_chat.py`: yt-dlpによるlive chat JSON取得とエラー分類
- `comment_processor.py`: JSON解析とキーワード抽出
- `pipeline.py`: 全体の処理フロー
- `exporter.py`: 最終CSV出力
- `main.py`: 実行入口

## 状態

`live_chat_jobs` は以下の3状態だけを表現する。

| completed | excluded | 意味 |
|---:|---:|---|
| 0 | 0 | 未処理 / 再試行対象 |
| 1 | 0 | 正常終了 |
| 0 | 1 | 対象外 |

## 処理ルール

通常動画はYouTube Data APIの `liveStreamingDetails` が存在しない動画として除外する。

ライブ配信でもlive chatが取得できない場合は `excluded=1` にする。

通信、解析、DBなどの一時的なエラーは `completed=0, excluded=0` のままにして、次回の実行時にJSONダウンロードから再開する。

コメントDBへの保存成功後にJSONを削除し、その後 `completed=1` にする。

## COMMENT_KEYWORD

カンマ区切りで複数指定する。

```env
COMMENT_KEYWORD=かわいい,草,面白い
```

空白のキーワードは無視する。

## 日付

`2026-09-01` のような日付だけを指定した場合、UTCの00:00を境界として扱う。
`2026-09-01T00:00:00Z` のような完全な日時も指定できる。
