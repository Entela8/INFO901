from Message import Message, MsgKind

class BroadcastMessage(Message):
    def __init__(self, payload, lamport, sender):
        super().__init__(MsgKind.USER, payload, lamport, sender)
