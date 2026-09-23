from dataclasses import dataclass


@dataclass(frozen=True)
class Channel:
    channel_name: str
    channel_id: str
