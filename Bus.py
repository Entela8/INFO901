from __future__ import annotations
import threading, time
from typing import TYPE_CHECKING
from Com import HEARTBEAT_TIMEOUT_SEC
from Message import Message

if TYPE_CHECKING:
    from Com import Com

class Bus:
    """ Bus mémoire partagé simulant le 'réseau' entre communicateurs `Com`.
        - Maintient l'annuaire des participants (`_subscribers`, `_directory`)
        - Diffuse les messages (broadcast / sendto)
        - Gère une barrière globale (barrier_arrive)
    """

    def __init__(self):
        """Initialise les structures internes (protégées par un RLock)."""
        self._lock = threading.RLock()
        self._subscribers: dict[str, "Com"] = {}
        self._directory: list[str] = []
        self._last_hb: dict[str, float] = {}
        self._barrier_waiting: set[str] = set()

    def join(self, com: "Com") -> int:
        """
        Enregistre un nouveau `Com` sur le bus et retourne son id logique.
        - Ajoute son UID dans l'annuaire
        - Met à jour la liste triée `_directory` (source de numérotation)
        - Déclenche `_renumber()` pour informer tout le monde du mapping à jour
        """
        with self._lock:
            node_uid = com.node_uid
            self._subscribers[node_uid] = com
            self._last_hb[node_uid] = time.time()
            if node_uid not in self._directory:
                self._directory.append(node_uid)
                self._directory.sort()
            self._renumber()
            return self._directory.index(node_uid)

    def leave(self, com: "Com"):
        """Retire un `Com` du bus (départ/arrêt) et renumérote les restants."""
        with self._lock:
            self._subscribers.pop(com.node_uid, None)
            self._last_hb.pop(com.node_uid, None)
            if com.node_uid in self._directory:
                self._directory.remove(com.node_uid)
            self._renumber()

    def _renumber(self):
        """" Recalcule la table {uid -> id} à partir de `_directory` et notifie tous les `Com`. """
        mapping = {uid: idx for idx, uid in enumerate(self._directory)}
        for c in list(self._subscribers.values()):
            c._onRenumber(mapping)

    def broadcast(self, msg: Message, exclude_uid: str | None = None):
        """ Diffuse `msg` à tous les `Com` enregistrés, sauf éventuellement `exclude_uid`. """
        with self._lock:
            for uid, c in self._subscribers.items():
                if uid == exclude_uid:
                    continue
                c._deliver(msg)

    def sendto(self, dest_id: int, msg: Message):
        """ Envoie `msg` à un seul destinataire par identifiant logique. """
        with self._lock:
            if 0 <= dest_id < len(self._directory):
                uid = self._directory[dest_id]
                c = self._subscribers.get(uid)
                if c:
                    c._deliver(msg)

    def heartbeat(self, sender_uid: str):
        """ Marque un heartbeat pour `sender_uid`. Sert à la détection de pannes. """
        with self._lock:
            self._last_hb[sender_uid] = time.time()

    def check_timeouts(self):
        """
        Détecte les `Com` silencieux (au-delà de HEARTBEAT_TIMEOUT_SEC) et les enlève.
        Déclenche ensuite une renumérotation.
        """
        with self._lock:
            now = time.time()
            dead = [uid for uid, t0 in self._last_hb.items()
                    if now - t0 > HEARTBEAT_TIMEOUT_SEC]
            if dead:
                for uid in dead:
                    self._subscribers.pop(uid, None)
                    self._last_hb.pop(uid, None)
                    if uid in self._directory:
                        self._directory.remove(uid)
                self._renumber()

    def barrier_arrive(self, uid: str):
        """
        Point de rendez-vous de la barrière:
        - Ajoute `uid` dans l'ensemble des arrivés.
        - Si tous les vivants sont arrivés, libère la barrière chez tous.
        """
        with self._lock:
            self._barrier_waiting.add(uid)
            if self._barrier_waiting == set(self._subscribers.keys()):
                self._barrier_waiting.clear()
                for c in list(self._subscribers.values()):
                    c._onBarrierRelease()