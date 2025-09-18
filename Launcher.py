from Process import Process
import threading
from Bus import Bus

def main():
    bus = Bus()
    procs = [Process(bus) for _ in range(3)]

    threads = []
    for p in procs:
        t = threading.Thread(target=p.run_example)
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    for p in procs:
        p.close()

if __name__ == "__main__":
    print("[MAIN] starting demo")
    main()
    print("[MAIN] done")