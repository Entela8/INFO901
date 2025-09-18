from Com import Com, Bus
from geeteventbus.subscriber import subscriber
from geeteventbus.eventbus import eventbus
from geeteventbus.event import event

class Process:
    def __init__(self, bus: Bus):
        self.com = Com(bus, on_receive=self._on_receive)

    @subscribe(threadMode = Mode.PARALLEL, onEvent=token)
    def _on_receive(self, msg):
        kind = msg.kind.name
        sender = msg.sender
        print(f"[P{self.com.id}] RECV {kind} from {sender} payload={getattr(msg, 'payload', None)}")

    def run_example(self):
        print(f"[P{self.com.id}] start, world_size={len(self.com.bus._directory)}")

        # ASYNC
        self.com.broadcast({"BONJOUR": f"from {self.com.id}"})
        self.com.sendTo(
            {"dm": f"to {(self.com.id + 1) % len(self.com.bus._directory)}"},
            dest=(self.com.id + 1) % len(self.com.bus._directory)
        )

        # SYNC
        self.com.broadcastSync(
            {"sync_all": f"from {self.com.id}"}, 
            from_id=self.com.id
        )
        self.com.sendToSync(
            {"sync_one": f"to {(self.com.id + 1) % len(self.com.bus._directory)} "},
            dest=(self.com.id + 1) % len(self.com.bus._directory)
        )
        _ = self.com.recvFromSync(
            from_id=(self.com.id - 1) % len(self.com.bus._directory), timeout=2
        )

        # BARRIÈRE
        print(f"[P{self.com.id}] waiting barrier")
        self.com.synchronize()
        print(f"[P{self.com.id}] passed barrier")

        # SECTION CRITIQUE
        print(f"[P{self.com.id}] request SC")
        self.com.requestSC()
        print(f"[P{self.com.id}] IN  SC")
        print(f"[P{self.com.id}] release SC")
        self.com.releaseSC()

        print(f"[P{self.com.id}] done")

    def close(self):
        self.com.close()