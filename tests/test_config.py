import csv

import pytest

import modules.config as config


def write_channels_csv(path, rows):
    with path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=["channel_name", "channel_id"],
        )
        writer.writeheader()
        writer.writerows(rows)


def test_get_channels_reads_csv(tmp_path):
    csv_path = tmp_path / "channels.csv"
    write_channels_csv(
        csv_path,
        [
            {"channel_name": "チャンネルA", "channel_id": "UC001"},
            {"channel_name": "チャンネルB", "channel_id": "UC002"},
        ],
    )

    channels = config.get_channels(str(csv_path))

    assert channels == [
        config.Channel("チャンネルA", "UC001"),
        config.Channel("チャンネルB", "UC002"),
    ]


def test_get_channels_accepts_utf8_bom(tmp_path):
    csv_path = tmp_path / "channels.csv"
    write_channels_csv(
        csv_path,
        [{"channel_name": "あいう", "channel_id": "UC001"}],
    )

    channels = config.get_channels(str(csv_path))

    assert channels[0].channel_name == "あいう"


def test_find_channels_by_name_is_case_insensitive(tmp_path):
    csv_path = tmp_path / "channels.csv"
    write_channels_csv(
        csv_path,
        [
            {"channel_name": "Python Channel", "channel_id": "UC001"},
            {"channel_name": "Another Channel", "channel_id": "UC002"},
            {"channel_name": "python live", "channel_id": "UC003"},
        ],
    )

    result = config.find_channels_by_name("PYTHON", str(csv_path))

    assert [channel.channel_id for channel in result] == ["UC001", "UC003"]


def test_find_channels_by_name_returns_empty_when_not_found(tmp_path):
    csv_path = tmp_path / "channels.csv"
    write_channels_csv(
        csv_path,
        [{"channel_name": "チャンネルA", "channel_id": "UC001"}],
    )

    assert config.find_channels_by_name("存在しない", str(csv_path)) == []


def test_validate_succeeds_when_keywords_exist(monkeypatch):
    monkeypatch.setattr(config, "COMMENT_KEYWORDS", ["かわいい"])

    config.validate()


def test_validate_raises_when_keywords_are_empty(monkeypatch):
    monkeypatch.setattr(config, "COMMENT_KEYWORDS", [])

    with pytest.raises(ValueError, match="COMMENT_KEYWORD is required"):
        config.validate()
