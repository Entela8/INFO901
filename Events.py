from dataclasses import dataclass

@dataclass
class UserEvent:
    """
    Évènement PyBus côté application publié par l'émetteur à chaque envoi.
    """
    sender: int     # id logique de l'émetteur
    lamport: int    # timestamp Lamport au moment de l'envoi
    payload: object # contenu applicatif

@dataclass
class TokenEvent:
    holder: int  # id qui détient le token