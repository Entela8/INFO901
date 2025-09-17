from dataclasses import dataclass
from Message import Message, MsgKind

@dataclass
class Token(Message):
    holder: int

    def __init__(self, holder: int):
        super().__init__(MsgKind.TOKEN, payload=None, lamport=0, sender=None)
        self.holder = holder
