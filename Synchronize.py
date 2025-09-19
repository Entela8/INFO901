from Message import Message, MsgKind

class BarrierMessage(Message):
    """
    Message système minimal pour une barrière (non indispensable ici car la
    barrière est gérée par le Bus). Conservé pour clarté/extension éventuelle.
    """
    def __init__(self, sender):
        super().__init__(MsgKind.BARRIER, payload=None, lamport=0, sender=sender)