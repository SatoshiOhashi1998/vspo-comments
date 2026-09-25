from dataclasses import FrozenInstanceError

import pytest

from modules.channel import Channel


def test_channel_stores_name_and_id():
    channel = Channel("テストチャンネル", "UC123")

    assert channel.channel_name == "テストチャンネル"
    assert channel.channel_id == "UC123"


def test_channel_is_frozen():
    channel = Channel("テストチャンネル", "UC123")

    with pytest.raises(FrozenInstanceError):
        channel.channel_id = "UC456"
