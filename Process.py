from Bus import Bus
from Com import Com
from Events import UserEvent, TokenEvent

from pyeventbus3.pyeventbus3 import PyBus, subscribe, Mode

class Process:    
    """
    'Application' côté élève : utilise UNIQUEMENT `Com` pour communiquer.
    S'enregistre auprès de PyBus pour logger les UserEvent façon `@subscribe`.
    """
    def __init__(self, bus: Bus, name: str = ""):
        """
        Construit le processus applicatif, crée son `Com` et s'enregistre
        comme subscriber PyBus pour recevoir les `UserEvent`.
        """
        self.com = Com(bus, on_receive=None)
        self.name = name or f"P{self.com.id}"
        PyBus.Instance().register(self, self)

    @subscribe(threadMode=Mode.PARALLEL, onEvent=UserEvent)
    def on_user_msg(self, e: UserEvent):
        """ Handler d'évènements applicatifs façon prof."""
        print(f"[{self.name}] @subscribe UserEvent from {e.sender} L={e.lamport} payload={e.payload}")

    @subscribe(threadMode=Mode.PARALLEL, onEvent=TokenEvent)
    def on_token(self, e: TokenEvent):
        if e.holder == self.com.id:
            print(f"[{self.name}] @subscribe Token received → I can enter SC")
            
    def run_example(self):
        """
        Petit scénario de démonstration :
        - broadcast + sendTo (async)
        - broadcastSync + sendToSync + recvFromSync (sync/ACK)
        - synchronize() (barrière globale)
        - requestSC()/releaseSC() (SC via jeton, handler onToken)
        """
        print(f"[{self.name}] start, world_size={len(self.com.bus._directory)}")

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
            {"sync_one": f"to {(self.com.id + 1) % len(self.com.bus._directory)}"},
            dest=(self.com.id + 1) % len(self.com.bus._directory)
        )
        _ = self.com.recvFromSync(from_id=(self.com.id - 1) % len(self.com.bus._directory), timeout=2)

        # BARRIÈRE
        print(f"[{self.name}] waiting barrier")
        self.com.synchronize()
        print(f"[{self.name}] passed barrier")

        # SECTION CRITIQUE
        print(f"[{self.name}] request SC")
        self.com.requestSC()
        print(f"[{self.name}] IN  SC")
        print(f"[{self.name}] release SC")
        self.com.releaseSC()

        print(f"[{self.name}] done")

    def close(self):
        """ Désinscrit les handlers PyBus (évite doublons si relancé dans le même
        interpréteur), puis ferme le Com."""
        try:
            PyBus.Instance().unregister(self, self)
        except Exception:
            pass
        self.com.close()
