# youtube-live-chat-collector

YouTubeのライブ配信からLive Chatを取得し、指定したキーワードを含むコメントを抽出・保存・CSV出力するツール。

YouTubeの動画情報取得には、共通ライブラリ `myutils.youtube_api` を使用する。

## 構成

```text
youtube-live-chat-collector/
├── main.py
├── modules/
│   ├── channel.py
│   ├── comment_processor.py
│   ├── comments_db.py
│   ├── config.py
│   ├── exporter.py
│   ├── live_chat.py
│   ├── pipeline.py
│   └── youtube.py
├── tests/
├── requirements.txt
├── requirements-freeze.txt
└── README.md
```

### 各モジュール

* `config.py`: 環境変数の読み込み、チャンネル設定、設定値の検証
* `channel.py`: YouTubeチャンネルを表すデータクラス
* `youtube.py`: `myutils.youtube_api` を利用した対象動画の取得
* `comments_db.py`: `comments.db` の管理
* `live_chat.py`: yt-dlpによるLive Chat JSONの取得とエラー分類
* `comment_processor.py`: Live Chat JSONの解析とキーワード抽出
* `pipeline.py`: 動画単位・チャンネル単位の処理フロー
* `exporter.py`: コメントのCSV出力
* `main.py`: 実行入口

## 依存関係

このプロジェクトは `myutils` に依存する。

```text
youtube-live-chat-collector
        ↓
     myutils
        ↓
  myutils.youtube_api
```

`myutils` はYouTube Data APIへのアクセスやYouTube動画情報のキャッシュを担当する。

開発時は `requirements.txt` からローカルの `myutils` をeditable installする。

```text
-e ../myutils
```

## 環境構築

仮想環境を作成して有効化した後、依存パッケージをインストールする。

```bash
pip install -r requirements.txt
```

依存関係を含めた開発環境全体を固定する場合は、`requirements-freeze.txt` を使用する。

テストはpytestで実行する。

```bash
pytest
```

## チャンネル設定

チャンネル情報はCSVで管理する。

```text
channel_name,channel_id
```

`CHANNEL_DATAS` でCSVファイルの場所を指定できる。

## COMMENT_KEYWORD

抽出対象とするキーワードをカンマ区切りで複数指定できる。

```env
COMMENT_KEYWORD=かわいい,草,面白い
```

空白のキーワードは無視する。

`COMMENT_KEYWORD` が空の場合、設定エラーとして扱う。

## 日付

`PUBLISHED_AFTER_DATE` と `PUBLISHED_BEFORE_DATE` で対象期間を指定できる。

```env
PUBLISHED_AFTER_DATE=2026-09-01
PUBLISHED_BEFORE_DATE=2026-09-30
```

`2026-09-01` のような日付だけを指定した場合、UTCの00:00を境界として扱う。

完全な日時も指定できる。

```env
PUBLISHED_AFTER_DATE=2026-09-01T00:00:00Z
PUBLISHED_BEFORE_DATE=2026-09-30T00:00:00Z
```

## 処理状態

`live_chat_jobs` は以下の3状態だけを表現する。

| completed | excluded | 意味          |
| --------: | -------: | ----------- |
|         0 |        0 | 未処理 / 再試行対象 |
|         1 |        0 | 正常終了        |
|         0 |        1 | 対象外         |

## 処理ルール

通常動画は、YouTube Data APIの `liveStreamingDetails` が存在しない動画として判定し、対象外とする。

ライブ配信であってもLive Chatを取得できない場合は `excluded=1` とする。

通信、解析、DBなどの一時的なエラーは、

```text
completed=0
excluded=0
```

のままにする。

そのため、次回の実行時には未完了の動画として再試行される。

コメントDBへの保存に成功した後でLive Chat JSONを削除し、その後 `completed=1` にする。

この順序により、JSON削除後にDB保存が失敗することでデータを失うことを防ぐ。

## データ

主に以下のデータを使用する。

* YouTube動画情報: `youtube.db`
* Live Chat処理状態・抽出コメント: `comments.db`
* Live Chat取得用JSON: `JSON_DIRECTORY`
* 抽出コメントCSV: `FILTERED_DATA`

デフォルトの保存先は設定ファイルで定義されている。

## requirements.txt と requirements-freeze.txt

`requirements.txt` には、このプロジェクトが直接依存するパッケージを記載する。

`requirements-freeze.txt` には、間接依存を含む実際のPython環境のパッケージとバージョンを記録する。

`requirements-freeze.txt` は以下で更新できる。

```bash
pip freeze > requirements-freeze.txt
```
