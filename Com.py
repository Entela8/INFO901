from __future__ import annotations
import threading, time, uuid
from collections import deque
from typing import TYPE_CHECKING, Callable

from Message import AckMessage, Message, MsgKind
from BroadcastMessage import BroadcastMessage
from MessageTo import MessageTo
from Token import Token
from Events import UserEvent, TokenEvent

try:
    from pyeventbus3.pyeventbus3 import PyBus
    _HAS_PYBUS = True
except Exception:
    _HAS_PYBUS = False

if TYPE_CHECKING:
    from Bus import Bus

HEARTBEAT_SEC = 1.0
HEARTBEAT_TIMEOUT_SEC = 3.5

if TYPE_CHECKING:
    from Bus import Bus

class Com:
    def __init__(self, bus: "Bus", on_receive: Callable[[Message], None] | None = None):
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
        self._pending_acks: dict[int, tuple[threading.Event, int]] = {}
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

    def update_clock_on_recv(self, other: int):
        with self.clock_lock:
            self.clock = max(self.clock, other) + 1

    # === API asynchrone ===
    def broadcast(self, payload: object):
        ts = self.inc_clock()
        msg = BroadcastMessage(payload=payload, lamport=ts, sender=self.id)
        if _HAS_PYBUS:
            try:
                PyBus.Instance().post(UserEvent(sender=self.id, lamport=ts, payload=payload))
            except Exception:
                pass
        self.bus.broadcast(msg, exclude_uid=self.node_uid)


    def sendTo(self, payload: object, dest: int):
        ts = self.inc_clock()
        msg = MessageTo(payload=payload, lamport=ts, sender=self.id, dest=dest)
        if _HAS_PYBUS:
            try:
                PyBus.Instance().post(UserEvent(sender=self.id, lamport=ts, payload=payload))
            except Exception:
                pass
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
            msg.ack_seq = seq

            if _HAS_PYBUS:
                try:
                    PyBus.Instance().post(UserEvent(sender=self.id, lamport=ts, payload=payload))
                except Exception:
                    pass
            remaining = self._world_size() - 1
            evt = threading.Event()
            with self._ack_lock:
                self._pending_acks[seq] = (evt, remaining)
            self.bus.broadcast(msg, exclude_uid=self.node_uid)
            evt.wait()

    def sendToSync(self, payload: object, dest: int):
        ts = self.inc_clock()
        seq = self._new_seq()
        msg = MessageTo(payload=payload, lamport=ts, sender=self.id, dest=dest)
        msg.ack_seq = seq
        # NEW:
        if _HAS_PYBUS:
            try:
                PyBus.Instance().post(UserEvent(sender=self.id, lamport=ts, payload=payload))
            except Exception:
                pass
        evt = threading.Event()
        with self._ack_lock:
            self._pending_acks[seq] = (evt, 1)
        self.bus.sendto(dest, msg)
        evt.wait()

    def recvFromSync(self, from_id: int, timeout: float | None = None) -> Message | None:
        t0 = time.time()
        while True:
            m = self.receive(block=True, timeout=timeout)
            if m and getattr(m, "sender", None) == from_id and m.kind == MsgKind.USER:
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

    #in release juste liberer la SC, et pas renvoiyer le token, function token propre pour mettre à jour le token
    def releaseSC(self):
        with self._token_next_lock:
            next_id = (self.id + 1) % self._world_size() #const
        self._token_evt.clear()
        self.bus.sendto(next_id, Token(holder=next_id))

    # === Arrêt ===
    def close(self):
        self._stop.set()
        self.bus.leave(self)

    def _deliver(self, msg: Message):
        if msg.kind == MsgKind.USER:
            self.update_clock_on_recv(msg.lamport)
            ack_seq = getattr(msg, "ack_seq", None)
            if ack_seq is not None and msg.sender is not None:
                # msg.sender est un id logique (int) → ok pour sendto
                self.bus.sendto(msg.sender, AckMessage(seq=ack_seq, sender=self.id))

            with self.mailbox_lock:
                self.mailbox.append(msg)
                self.mailbox_has_msg.set()

            if self.on_receive:
                try: self.on_receive(msg)
                except Exception: pass
            return

        elif msg.kind == MsgKind.ACK:
            seq = getattr(msg, "seq", None)
            if seq is not None:
                with self._ack_lock:
                    evt, remaining = self._pending_acks.get(seq, (None, 0))
                    if evt:
                        remaining -= 1
                        if remaining <= 0:
                            self._pending_acks.pop(seq, None)
                            evt.set()
                        else:
                            self._pending_acks[seq] = (evt, remaining)
            return

        elif msg.kind == MsgKind.RENUMBER:
            mapping = msg.payload
            with self.id_lock:
                self.id = mapping[self.node_uid]
            return

        elif msg.kind == MsgKind.TOKEN:
            if isinstance(msg, Token) and msg.holder == self.id:
                self._token_evt.set()
                if _HAS_PYBUS:
                    try:
                        PyBus.Instance().post(TokenEvent(holder=self.id))
                    except Exception:
                        pass
            return

    def _new_seq(self) -> int:
        with self._ack_lock:
            self._ack_seq += 1
            return self._ack_seq

    #mets à jour l'identifiant logique
    def _onRenumber(self, mapping: dict[str, int]):
        m = Message(kind=MsgKind.RENUMBER, payload=mapping, lamport=0, sender=None)
        self._deliver(m)

    def _onBarrierRelease(self):
        self._barrier_evt.set()

    def _world_size(self) -> int:
        return len(self.bus._directory)

    # threads
    # heartbeats
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
