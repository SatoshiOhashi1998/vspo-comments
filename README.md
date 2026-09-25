# youtube-live-chat-collector

YouTubeのライブ配信からLive Chatを取得し、指定したキーワードを含むコメントを抽出・保存・CSV出力するツール。

YouTubeの動画情報取得には、共通ライブラリ `myutils.youtube_api` を使用する。

## 構成

```text
youtube-live-chat-collector/
├── main.py
├── modules/
│   ├── __init__.py
│   ├── channel.py
│   ├── comment_processor.py
│   ├── comments_db.py
│   ├── config.py
│   ├── exporter.py
│   ├── live_chat.py
│   ├── pipeline.py
│   └── youtube.py
├── tests/
│   ├── test_channel.py
│   ├── test_comment_processor.py
│   ├── test_comments_db.py
│   ├── test_config.py
│   ├── test_exporter.py
│   ├── test_live_chat.py
│   ├── test_pipeline.py
│   └── test_youtube.py
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
* `exporter.py`: 抽出したコメントのCSV出力
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

`myutils.youtube_api` はYouTube Data APIへのアクセスと、YouTube動画情報のSQLiteキャッシュを担当する。

開発時は `requirements.txt` からローカルの `myutils` をeditable installする。

```text
-e ../myutils
```

## 環境構築

仮想環境を作成して有効化した後、依存パッケージをインストールする。

```bash
pip install -r requirements.txt
```

## 環境変数

### チャンネル

`CHANNEL_DATAS` でチャンネル情報を記録したCSVファイルを指定する。

デフォルト:

```text
data/channels.csv
```

CSVは以下の形式にする。

```csv
channel_name,channel_id
チャンネルA,UCxxxxxxxxxxxxxxxxxxxxxx
チャンネルB,UCyyyyyyyyyyyyyyyyyyyyyy
```

### コメント

`COMMENT_KEYWORD` で抽出対象とするキーワードを指定する。

カンマ区切りで複数指定できる。

```env
COMMENT_KEYWORD=かわいい,草,面白い
```

キーワードの前後の空白は削除され、空白だけのキーワードは無視される。

`COMMENT_KEYWORD` が空の場合、設定エラーとして扱う。

`FILTERED_DATA` で抽出コメントのCSV出力先を指定する。

デフォルト:

```text
data/comments.csv
```

### 動画取得期間

`PUBLISHED_AFTER_DATE` と `PUBLISHED_BEFORE_DATE` で対象期間の初期値を指定できる。

```env
PUBLISHED_AFTER_DATE=2026-09-01
PUBLISHED_BEFORE_DATE=2026-09-30
```

実行時に別の日付を入力することもできる。

`2026-09-01` のような日付だけを指定した場合、UTCの00:00を境界として扱う。

完全な日時も指定できる。

```env
PUBLISHED_AFTER_DATE=2026-09-01T00:00:00Z
PUBLISHED_BEFORE_DATE=2026-09-30T00:00:00Z
```

### Live Chat

`JSON_DIRECTORY` でLive Chat JSONの保存先を指定する。

デフォルト:

```text
data/live_chat
```

`COOKIES_FILE` でyt-dlpが使用するCookieファイルを指定する。

デフォルト:

```text
cookies.txt
```

### コメントDB

`COMMENTS_DB_PATH` でコメントDBの保存先を指定する。

デフォルト:

```text
data/comments.db
```

YouTube動画情報のDBについては、`myutils.youtube_api` 側で管理する。

## 実行

```bash
python main.py
```

実行するとチャンネル名を部分一致で検索し、対象チャンネルと取得期間を指定して処理する。

複数のチャンネルが一致した場合は、対象チャンネルを番号で選択する。

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
* Live Chat取得用JSON: `data/live_chat/`
* 抽出コメントCSV: `data/comments.csv`

保存先は環境変数で変更できる。

## テスト

pytestを使用する。

```bash
pytest
```

テストではYouTube Data APIやyt-dlpへの実際の通信は行わず、モックを使用して各モジュールをテストする。

SQLiteを使用するテストでは、一時ディレクトリを利用してテスト用DBを作成する。

## requirements.txt と requirements-freeze.txt

`requirements.txt` には、このプロジェクトが直接依存するパッケージを記載する。

現在の直接依存は以下のとおり。

```text
pandas
pygame
python-dotenv
yt-dlp
myutils
```

`requirements-freeze.txt` には、間接依存を含む実際のPython環境のパッケージとバージョンを記録する。

環境を更新した場合は、以下のコマンドでfreezeファイルを更新できる。

```bash
pip freeze > requirements-freeze.txt
```
