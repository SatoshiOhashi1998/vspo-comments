from __future__ import annotations

from modules.config import (
    CHANNEL_DATAS,
    COMMENT_KEYWORDS,
    FILTERED_DATA,
    PUBLISHED_AFTER_DATE,
    PUBLISHED_BEFORE_DATE,
    find_channels_by_name,
    get_channels,
    validate,
)
from modules.exporter import export_comments_to_csv
from modules.pipeline import process_channel, process_channels


def _input_date(label: str, default: str | None) -> str:
    if default:
        value = input(f"{label} [{default}]: ").strip()
        return value or default

    while True:
        value = input(f"{label}: ").strip()
        if value:
            return value
        print("日付を入力してください。")


def interactive_mode() -> None:
    validate()

    channel_name = input("チャンネル名（部分一致）: ").strip()
    matches = find_channels_by_name(channel_name)

    if not matches:
        print("チャンネルが見つかりませんでした。")
        return

    if len(matches) > 1:
        print("複数のチャンネルが見つかりました:")
        for index, channel in enumerate(matches, start=1):
            print(
                f"{index}: {channel['channel_name']} "
                f"({channel['channel_id']})"
            )

        while True:
            try:
                selected = int(input("番号: "))
                channel = matches[selected - 1]
                break
            except (ValueError, IndexError):
                print("正しい番号を入力してください。")
    else:
        channel = matches[0]

    start_date = _input_date(
        "開始日",
        PUBLISHED_AFTER_DATE,
    )
    end_date = _input_date(
        "終了日",
        PUBLISHED_BEFORE_DATE,
    )

    print(f"キーワード: {', '.join(COMMENT_KEYWORDS)}")

    stats = process_channel(
        channel_id=channel.channel_id,
        start_date=start_date,
        end_date=end_date,
        keywords=COMMENT_KEYWORDS,
    )

    print()
    print(f"処理結果: {stats}")

    export_comments_to_csv(
        FILTERED_DATA,
        channel=channel.channel_name,
    )


def run_all_channels() -> None:
    validate()

    channels = get_channels(CHANNEL_DATAS)
    if not channels:
        print("チャンネルがありません。")
        return

    start_date = _input_date(
        "開始日",
        PUBLISHED_AFTER_DATE,
    )
    end_date = _input_date(
        "終了日",
        PUBLISHED_BEFORE_DATE,
    )

    print(f"キーワード: {', '.join(COMMENT_KEYWORDS)}")

    stats = process_channels(
        channels=channels,
        start_date=start_date,
        end_date=end_date,
        keywords=COMMENT_KEYWORDS,
    )

    print()
    print(f"全体の処理結果: {stats}")

    export_comments_to_csv(FILTERED_DATA)


if __name__ == "__main__":
    interactive_mode()
