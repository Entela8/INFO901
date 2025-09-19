from Message import Message, MsgKind

class MessageTo(Message):
    """
    Message applicatif adressé à UN destinataire (point-à-point).
    `dest` est l'id logique du destinataire.
    """
    def __init__(self, payload, lamport, sender, dest):
        super().__init__(MsgKind.USER, payload, lamport, sender)
        self.dest = dest