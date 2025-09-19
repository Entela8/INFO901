from dataclasses import dataclass
from enum import Enum, auto

class MsgKind(Enum):
    """
    Typage des messages échangés :
    - USER: message applicatif (participe à l'horloge)
    - ACK: accusé de réception (pour les primitives synchrones)
    - BARRIER: (réservé) messages de barrière
    - HEARTBEAT: (optionnel) vie/santé d'un Com
    - RENUMBER: mapping {uid->id} (si renumérotation dynamique)
    - TOKEN: jeton de section critique
    """
    USER = auto()
    ACK = auto()
    BARRIER = auto()
    HEARTBEAT = auto()
    RENUMBER = auto()
    TOKEN = auto()

@dataclass
class Message:
    """
    Message générique.
        kind (MsgKind): type de message
        payload (object|None): contenu applicatif ou système
        lamport (int): horloge Lamport apposée à l'envoi (0 pour messages système)
        sender (int|None): id logique de l'émetteur (None pour certains systèmes)
    """
    kind: MsgKind
    payload: object | None
    lamport: int
    sender: int | None

@dataclass
class AckMessage(Message):
    """
    Accusé de réception d’un message 'sync'.
    La séquence `seq` permet de réveiller le bon émetteur.
    """
    seq: int
    def __init__(self, seq: int, sender: int):
        super().__init__(MsgKind.ACK, None, 0, sender)
        self.seq = seq