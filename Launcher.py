from Bus import Bus
from Process import Process
import threading

def main():
    """ Démarre une petite démo :
    - crée un Bus
    - instancie N Process (ici 3)
    - lance leur scénario `run_example()`
    - attend la fin puis ferme proprement"""
    bus = Bus()
    procs = [Process(bus, name=f"P{i}") for i in range(3)]
    threads = [threading.Thread(target=p.run_example) for p in procs]
    for t in threads: t.start()
    for t in threads: t.join()
    for p in procs: p.close()

if __name__ == "__main__":
    print("[MAIN] starting demo")
    main()
    print("[MAIN] done")