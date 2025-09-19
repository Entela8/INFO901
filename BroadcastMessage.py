from Message import Message, MsgKind

class BroadcastMessage(Message):
    """ Message applicatif diffusé à *tous* les processus.
        Hérite de `Message`. Le champ `kind` vaut `MsgKind.USER` (message utilisateur,
        donc il participe à l'horloge de Lamport côté réception)."""

    def __init__(self, payload, lamport, sender):
        """ Crée un message de broadcast."""
        super().__init__(MsgKind.USER, payload, lamport, sender)
