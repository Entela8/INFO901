from Message import Message, MsgKind

class MessageTo(Message):
    def __init__(self, payload, lamport, sender, dest):
        super().__init__(MsgKind.USER, payload, lamport, sender)
        self.dest = dest