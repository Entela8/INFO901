from Com import Bus
from Process import Process
import threading, time

def main():
    bus = Bus()
    procs = [Process(bus) for _ in range(3)]

    threads = []
    for p in procs:
        t = threading.Thread(target=p.run_example, daemon=True)
        t.start()
        threads.append(t)

    time.sleep(5)
    for p in procs:
        p.close()

    for t in threads:
        t.join(timeout=1)

if __name__ == "__main__":
    main()
