import server.ota_server as server
import client.ota_client as client
import sys
from threading import Thread

if __name__ == "__main__":
    client_thread = Thread(target=client.main)
    client_thread.start()
    if not client.subscribed.wait(timeout=15):
        print("client did not subscribe in time, not starting server")
    else:
        server_thread = Thread(target=server.main, args=(sys.argv[1] if len(sys.argv) > 1 else "all",))
        server_thread.start()
        server_thread.join()
    client_thread.join()
