from Com import Com, Bus
from BroadcastMessage import BroadcastMessage

class Process:
    def __init__(self, bus: Bus):
        self.com = Com(bus)

    def run_example(self):
        self.com.broadcast({"hello": "world"})
        self.com.sendTo({"to": "someone"}, dest=(self.com.id + 1) %  self.com._world_size())

        self.com.broadcastSync({"sync": "all"}, from_id=self.com.id)
        self.com.sendToSync({"one": "to one"}, dest=(self.com.id + 1) % self.com._world_size())
        m = self.com.recvFromSync(from_id=(self.com.id - 1) % self.com._world_size(), timeout=3)

        # Barrière
        self.com.synchronize()

        # Section critique
        self.com.requestSC()
        self.com.releaseSC()

    def close(self):
        self.com.close()
