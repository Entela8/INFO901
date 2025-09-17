from Message import Message, MsgKind

class BarrierMessage(Message):
    def __init__(self, sender):
        super().__init__(MsgKind.BARRIER, payload=None, lamport=0, sender=sender)
