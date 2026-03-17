# main.py
import os
from dotenv import load_dotenv
from modules import youtube_api, chat_processor
from myutils.playsound import success
import pandas as pd

load_dotenv()


COMMENT_KEYWORD = os.getenv('COMMENT_KEYWORD')
CHANNEL_DATAS = os.getenv('CHANNEL_DATAS')


def find_channels_by_name(partial_name: str, csv_path: str = CHANNEL_DATAS) -> list:
    """
    指定された部分文字列に一致する channel_name と channel_id をまとめて返す。

    Args:
        partial_name (str): 検索する文字列（部分一致）。
        csv_path (str): CSVファイルのパス。

    Returns:
        list[dict]: 一致したチャンネルの情報（channel_name と channel_id の辞書）リスト。
        test
    """
    try:
        df = pd.read_csv(csv_path)
        df.columns = df.columns.str.strip()  # 空白除去

        if 'channel_name' not in df.columns or 'channel_id' not in df.columns:
            raise ValueError(f"必要な列が見つかりません: {df.columns.tolist()}")

        matched = df[df['channel_name'].str.contains(partial_name, case=False, na=False)]
        
        return matched[['channel_name', 'channel_id']].to_dict(orient='records')
    
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        return []


def get_all_channels(csv_path: str = CHANNEL_DATAS) -> list:
    """
    CSVからすべての channel_name と channel_id を取得して返す。

    Args:
        csv_path (str): CSVファイルのパス。

    Returns:
        list[dict]: 各チャンネルの情報（channel_name と channel_id の辞書）リスト。
    """
    try:
        df = pd.read_csv(csv_path)
        df.columns = df.columns.str.strip()  # 空白除去

        if 'channel_name' not in df.columns or 'channel_id' not in df.columns:
            raise ValueError(f"必要な列が見つかりません: {df.columns.tolist()}")

        return df[['channel_name', 'channel_id']].to_dict(orient='records')

    except Exception as e:
        print(f"エラーが発生しました: {e}")

        return []


def run_get_youtube_chat(channel_data):
    youtube_api.get_videos_from_channel(channel_id=channel_data['channel_id'])
    youtube_api.get_chat_from_csv()
    # success()


def run_use_chat_data(channel_data):
    chat_processor.rename_json()
    chat_processor.create_db()

    chat_processor.process_json_files(chat_processor.JSON_DIRECTORY, channel_data['channel_name'])
    chat_processor.clean_data_files()

    chat_processor.migrate_filtered_data(channel_val=str(channel_data['channel_name']))

    results = chat_processor.search_comments(db=chat_processor.FILTERED_DB_FILE, comment=COMMENT_KEYWORD)
    chat_processor.save_to_csv(results)


def main():
    for channel_data in get_all_channels():
        run_get_youtube_chat(channel_data)
        run_use_chat_data(channel_data)


def sub():
    results = chat_processor.search_comments(comment=os.getenv('SUB_KEYWORD'))
    chat_processor.save_to_csv(data=results, filename=os.getenv('SUB_DATAS'))


def interactive_mode():
    print("対話モード：チャンネル名を入力してください（終了するには 'exit' と入力）")

    while True:
        user_input = input("検索したいチャンネル名（部分一致OK）> ").strip()

        if user_input.lower() in ['exit', 'quit']:
            print("終了します。")
            break

        channel_list = find_channels_by_name(partial_name=user_input)

        if not channel_list:
            print("該当するチャンネルが見つかりませんでした。")
            continue

        for channel_data in channel_list:
            print(f"処理対象: {channel_data['channel_name']} ({channel_data['channel_id']})")
            run_get_youtube_chat(channel_data)
            run_use_chat_data(channel_data)
            # success()


if __name__ == '__main__':
    main()
    # interactive_mode()
    sub()
    # success()
