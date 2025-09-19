# Com.py
from __future__ import annotations
import threading, time, uuid
from collections import deque
from typing import TYPE_CHECKING, Callable

from Message import AckMessage, Message, MsgKind
from BroadcastMessage import BroadcastMessage
from MessageTo import MessageTo
from Token import Token
from Events import UserEvent, TokenEvent
from pyeventbus3.pyeventbus3 import PyBus, subscribe, Mode

try:
    from pyeventbus3.pyeventbus3 import PyBus
    _HAS_PYBUS = True
except Exception:
    _HAS_PYBUS = False

if TYPE_CHECKING:
    from Bus import Bus  # typing-only to avoid circular import

# === Constantes ===
WORLD_SIZE = 3
HEARTBEAT_SEC = 1.0
HEARTBEAT_TIMEOUT_SEC = 3.5

class Com:
    def __init__(self, bus: "Bus", on_receive: Callable[[Message], None] | None = None):
        self.bus = bus
        self.node_uid = f"{uuid.uuid4()}"
        self.id_lock = threading.RLock()
        self.id: int = -1

        # --- Horloge Lamport ---
        self.clock_lock = threading.RLock()
        self.clock = 0

        # --- BAL ---
        self.mailbox = deque()
        self.mailbox_lock = threading.RLock()
        self.mailbox_has_msg = threading.Event()

        # --- Sync (ACKs) ---
        self._ack_lock = threading.RLock()
        # seq -> (event, remaining_acks)
        self._pending_acks: dict[int, tuple[threading.Event, int]] = {}
        self._ack_seq = 0

        # --- Barrière ---
        self._barrier_evt = threading.Event()

        # --- SC state machine (demandée par le prof) ---
        # idle -> request -> sc -> release -> idle
        self._sc_lock = threading.RLock()
        self.sc_state = "idle"
        self._in_sc_evt = threading.Event()  # réveille requestSC() quand on passe en "sc"

        # --- Callbacks app (optionnel) ---
        self.on_receive = on_receive

        # --- Join ---
        self.id = self.bus.join(self)

        # Jeton initial au P0
        if self.id == 0:
            self._deliver(Token(holder=0))

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

            remaining = WORLD_SIZE - 1
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

    # === Section critique distribuée (style prof) ===
    def requestSC(self):
        # Je demande la SC et j'attends d'y entrer
        with self._sc_lock:
            self.sc_state = "request"
        self._in_sc_evt.wait()

    def releaseSC(self):
        with self._sc_lock:
            self.sc_state = "release"
        self._in_sc_evt.clear()

    # === Arrêt ===
    def close(self):
        self.bus.leave(self)

    # === Délivrance de tout message entrant ===
    def _deliver(self, msg: Message):
        if msg.kind == MsgKind.USER:
            # Horloge Lamport
            self.update_clock_on_recv(msg.lamport)
            # ACK si message sync
            ack_seq = getattr(msg, "ack_seq", None)
            if ack_seq is not None and msg.sender is not None:
                self.bus.sendto(msg.sender, AckMessage(seq=ack_seq, sender=self.id))

            # Dépôt BAL
            with self.mailbox_lock:
                self.mailbox.append(msg)
                self.mailbox_has_msg.set()

            # Callback éventuel
            if self.on_receive:
                try:
                    self.on_receive(msg)
                except Exception:
                    pass
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
                if _HAS_PYBUS:
                    try:
                        PyBus.Instance().post(TokenEvent(holder=self.id))
                    except Exception:
                        pass

                with self._sc_lock:
                    want = (self.sc_state == "request")

                if not want:
                    # Pas besoin: forward immédiat (ASYNCHRONE pour éviter la récursion)
                    self._forward_token_async((self.id + 1) % WORLD_SIZE)
                    return

                # J'entre en SC
                with self._sc_lock:
                    self.sc_state = "sc"
                self._in_sc_evt.set()  # réveille requestSC()

                # Attendre que releaseSC() bascule l'état sur "release"
                while True:
                    time.sleep(0.05)
                    with self._sc_lock:
                        if self.sc_state == "release":
                            self.sc_state = "idle"
                            break

                # Forward au suivant (ASYNCHRONE)
                self._in_sc_evt.clear()
                self._forward_token_async((self.id + 1) % WORLD_SIZE)
            return

    # === Helpers ===
    def _new_seq(self) -> int:
        with self._ack_lock:
            self._ack_seq += 1
            return self._ack_seq

    def _onRenumber(self, mapping: dict[str, int]):
        # Utilisé seulement si vous conservez la renumérotation via Bus
        m = Message(kind=MsgKind.RENUMBER, payload=mapping, lamport=0, sender=None)
        self._deliver(m)

    def _onBarrierRelease(self):
        self._barrier_evt.set()

    @subscribe(threadMode=Mode.PARALLEL, onEvent=TokenEvent)
    def on_token(self, e: TokenEvent):
        if e.holder == self.com.id:
            print(f"[{self.name}] @subscribe Token received → I can enter SC")

    def _forward_token_async(self, next_id: int):
        def _send():
            self.bus.sendto(next_id, Token(holder=next_id))
        threading.Thread(target=_send, daemon=True).start()
