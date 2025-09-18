from dataclasses import dataclass
from enum import Enum, auto

class MsgKind(Enum):
    USER = auto()
    ACK = auto()
    BARRIER = auto()
    HEARTBEAT = auto()
    RENUMBER = auto()
    TOKEN = auto()

@dataclass
class Message:
    kind: MsgKind
    payload: object | None
    lamport: int
    sender: int | None

@dataclass
class AckMessage(Message):
    seq: int
    def __init__(self, seq: int, sender: int):
        super().__init__(MsgKind.ACK, None, 0, sender)
        self.seq = seq