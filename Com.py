import threading, time, uuid
from collections import defaultdict, deque
from typing import Callable

from Message import Message, MsgKind
from BroadcastMessage import BroadcastMessage
from MessageTo import MessageTo
from Token import Token
from Synchronize import BarrierMessage

HEARTBEAT_SEC = 1.0
HEARTBEAT_TIMEOUT_SEC = 3.5

class Bus:
    """Bus mémoire partagé: pas de variable de classe dans Com.
       On passe une instance de Bus au constructeur de Com.
    """
    def __init__(self):
        self._lock = threading.RLock()
        self._subscribers: dict[str, "Com"] = {}
        self._directory: list[str] = [] 
        self._last_hb: dict[str, float] = {}
        self._barrier_waiting: set[str] = set()

    # === Annuaire et diffusion ===
    def join(self, com: "Com") -> int:
        with self._lock:
            node_uid = com.node_uid
            self._subscribers[node_uid] = com
            self._last_hb[node_uid] = time.time()
            # ordre par uid => numérotation déterministe sans variable de classe
            if node_uid not in self._directory:
                self._directory.append(node_uid)
                self._directory.sort()
            self._renumber()
            return self._directory.index(node_uid)

    def leave(self, com: "Com"):
        with self._lock:
            self._subscribers.pop(com.node_uid, None)
            self._last_hb.pop(com.node_uid, None)
            if com.node_uid in self._directory:
                self._directory.remove(com.node_uid)
            self._renumber()

    def _renumber(self):
        # envoie un message RENUMBER à tous
        mapping = {uid: idx for idx, uid in enumerate(self._directory)}
        for c in list(self._subscribers.values()):
            c._onRenumber(mapping)

    def broadcast(self, msg: Message, exclude_uid: str | None = None):
        with self._lock:
            for uid, c in self._subscribers.items():
                if uid == exclude_uid:
                    continue
                c._deliver(msg)

    def sendto(self, dest_id: int, msg: Message):
        with self._lock:
            if 0 <= dest_id < len(self._directory):
                uid = self._directory[dest_id]
                c = self._subscribers.get(uid)
                if c:
                    c._deliver(msg)

    def heartbeat(self, sender_uid: str):
        with self._lock:
            self._last_hb[sender_uid] = time.time()

    def check_timeouts(self):
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

    # === Barrière ===
    def barrier_arrive(self, uid: str):
        with self._lock:
            self._barrier_waiting.add(uid)
            if self._barrier_waiting == set(self._subscribers.keys()):
                # tous présents: vider et notifier
                self._barrier_waiting.clear()
                for c in list(self._subscribers.values()):
                    c._onBarrierRelease()


class Com:
    def __init__(self, bus: Bus, on_receive: Callable[[Message], None] | None = None):
        self.bus = bus
        self.node_uid = f"{uuid.uuid4()}"
        self.id_lock = threading.RLock()
        self.id: int = -1

        # Horloge Lamport
        self.clock_lock = threading.RLock()
        self.clock = 0

        # BAL
        self.mailbox = deque()
        self.mailbox_lock = threading.RLock()
        self.mailbox_has_msg = threading.Event()

        # Sync primitives
        self._ack_lock = threading.RLock()
        self._pending_acks: dict[int, threading.Event] = {}
        self._ack_seq = 0

        # Barrière
        self._barrier_evt = threading.Event()

        # SC via Token
        self._token_evt = threading.Event()
        self._token_next_lock = threading.RLock()

        # Callbacks
        self.on_receive = on_receive

        # Threads
        self._stop = threading.Event()
        self._hb_thread = threading.Thread(target=self._hb_loop, daemon=True)
        self._timeout_thread = threading.Thread(target=self._timeout_loop, daemon=True)
        self._token_thread = threading.Thread(target=self._token_loop, daemon=True)

        # Join & start
        self.id = self.bus.join(self)
        if self.id == 0:
            self._deliver(Token(holder=0))
        self._hb_thread.start()
        self._timeout_thread.start()
        self._token_thread.start()

    # === Horloge ===
    def inc_clock(self, delta: int = 1):
        with self.clock_lock:
            self.clock += delta
            return self.clock

    def _update_clock_on_recv(self, other: int):
        with self.clock_lock:
            self.clock = max(self.clock, other) + 1

    # === API asynchrone ===
    def broadcast(self, payload: object):
        ts = self.inc_clock()
        msg = BroadcastMessage(payload=payload, lamport=ts, sender=self.id)
        self.bus.broadcast(msg, exclude_uid=self.node_uid)

    def sendTo(self, payload: object, dest: int):
        ts = self.inc_clock()
        msg = MessageTo(payload=payload, lamport=ts, sender=self.id, dest=dest)
        self.bus.sendto(dest, msg)

    def receive(self, block: bool = True, timeout: float | None = None) -> Message | None:
        if block:
            if not self.mailbox_has_msg.wait(timeout=timeout):
                return None
        with self.mailbox_lock:
            if not self.mailbox:
                self.mailbox_has_msg.clear()
                return None
            m = self.mailbox.popleft()
            if not self.mailbox:
                self.mailbox_has_msg.clear()
            return m

    # === API synchrone (bloquante) ===
    def broadcastSync(self, payload: object, from_id: int):
        if self.id == from_id:
            ts = self.inc_clock()
            seq = self._new_seq()
            msg = BroadcastMessage(payload=payload, lamport=ts, sender=self.id)
            # envoyer et attendre n-1 ACKs (un seul event global suffit si on compte)
            waiter = self._pending_acks[seq] = threading.Event()
            self._acks_needed = self._world_size() - 1
            self._ack_seq_current = seq
            self.bus.broadcast(msg, exclude_uid=self.node_uid)
            waiter.wait()
        else:
            # côté receveur: attend réception (normal) => ACK auto dans _deliver
            pass

    def sendToSync(self, payload: object, dest: int):
        ts = self.inc_clock()
        seq = self._new_seq()
        msg = MessageTo(payload=payload, lamport=ts, sender=self.id, dest=dest)
        waiter = self._pending_acks[seq] = threading.Event()
        msg._ack_seq = seq
        self.bus.sendto(dest, msg)
        waiter.wait()

    def recvFromSync(self, from_id: int, timeout: float | None = None) -> Message | None:
        t0 = time.time()
        while True:
            m = self.receive(block=True, timeout=timeout)
            if m and getattr(m, "sender", None) == from_id and m.kind == MsgKind.USER:
                # l’ACK part dans _deliver()
                return m
            if timeout is not None and (time.time() - t0) > timeout:
                return None

    # === Barrière ===
    def synchronize(self):
        self.bus.barrier_arrive(self.node_uid)
        self._barrier_evt.wait()
        self._barrier_evt.clear()

    # === Section critique distribuée (token) ===
    def requestSC(self):
        self._token_evt.wait()

    def releaseSC(self):
        with self._token_next_lock:
            next_id = (self.id + 1) % self._world_size()
        self._token_evt.clear()
        self.bus.sendto(next_id, Token(holder=next_id))

    # === Arrêt propre ===
    def close(self):
        self._stop.set()
        self.bus.leave(self)

    def _deliver(self, msg: Message):
        if msg.kind == MsgKind.USER:
            self._update_clock_on_recv(msg.lamport)
            ack = Message(kind=MsgKind.ACK, payload=None, lamport=0, sender=self.id)
            if hasattr(msg, "dest"):
                self.bus.sendto(msg.sender, ack)
            else:
                self.bus.sendto(msg.sender, ack)

        elif msg.kind == MsgKind.ACK:
            with self._ack_lock:
                if hasattr(self, "_ack_seq_current"):
                    self._acks_needed -= 1
                    if self._acks_needed <= 0:
                        evt = self._pending_acks.pop(self._ack_seq_current, None)
                        if evt:
                            evt.set()
            return

        elif msg.kind == MsgKind.BARRIER:
            return

        elif msg.kind == MsgKind.HEARTBEAT:
            return

        elif msg.kind == MsgKind.RENUMBER:
            mapping = msg.payload
            with self.id_lock:
                self.id = mapping[self.node_uid]
            return

        elif msg.kind == MsgKind.TOKEN:
            if isinstance(msg, Token) and msg.holder == self.id:
                self._token_evt.set()
            return

        # USER messages vont en BAL
        with self.mailbox_lock:
            self.mailbox.append(msg)
            self.mailbox_has_msg.set()

        if self.on_receive:
            try:
                self.on_receive(msg)
            except Exception:
                pass

    def _new_seq(self) -> int:
        with self._ack_lock:
            self._ack_seq += 1
            return self._ack_seq

    def _onRenumber(self, mapping: dict[str, int]):
        m = Message(kind=MsgKind.RENUMBER, payload=mapping, lamport=0, sender=None)
        self._deliver(m)

    def _onBarrierRelease(self):
        self._barrier_evt.set()

    def _world_size(self) -> int:
        return len(self.bus._directory)

    # threads
    def _hb_loop(self):
        while not self._stop.is_set():
            self.bus.heartbeat(self.node_uid)
            time.sleep(HEARTBEAT_SEC)

    def _timeout_loop(self):
        while not self._stop.is_set():
            self.bus.check_timeouts()
            time.sleep(HEARTBEAT_SEC)

    def _token_loop(self):
        # rien à faire: le token circule via messages
        while not self._stop.is_set():
            time.sleep(0.1)
