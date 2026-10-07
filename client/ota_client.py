import base64
import hashlib
import json
import os
import threading
import time

import paho.mqtt.client as mqtt
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

BROKER_HOST = os.environ["BROKER_HOST"]
BROKER_PORT = int(os.environ["BROKER_PORT"])
QOS = int(os.environ["QOS"])
TOPIC_BASE = os.environ["TOPIC_BASE"]
TIMEOUT = int(os.environ["TIMEOUT"])
OUTPUT_FILE = os.path.join(BASE_DIR, os.environ["OUTPUT_FILE"])
LOG_FILE = os.path.join(BASE_DIR, os.environ["LOG_FILE"])

manifest = None
chunks = {}
first_message_time = None
subscribed = threading.Event()


def log(text):
    line = time.strftime("%Y-%m-%d %H:%M:%S") + " " + text
    print(line)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")


def merkle_root(chunks):
    h0 = hashlib.sha256(chunks[0]).digest()
    h1 = hashlib.sha256(chunks[1]).digest()
    h2 = hashlib.sha256(chunks[2]).digest()
    h3 = hashlib.sha256(chunks[3]).digest()
    h01 = hashlib.sha256(h0 + h1).digest()
    h23 = hashlib.sha256(h2 + h3).digest()

    return hashlib.sha256(h01 + h23).digest()


def on_connect(client, userdata, flags, reason_code, properties):
    print("connected to broker:", reason_code)
    client.subscribe([(TOPIC_BASE + "/+/manifest", QOS), (TOPIC_BASE + "/+/chunk/+", QOS)])


def on_subscribe(client, userdata, mid, reason_code_list, properties):
    print("subscribed:", reason_code_list)
    subscribed.set()


def on_message(client, userdata, msg):
    global manifest, first_message_time

    if first_message_time is None:
        first_message_time = time.time()

    try:
        payload = json.loads(msg.payload)
    except ValueError:
        print("ignored message on", msg.topic, "(not valid json)")
        return

    if msg.topic.endswith("/manifest"):
        manifest = payload
        print("manifest received: version", manifest.get("version"), "root", manifest.get("merkle_root"))
        return

    index = payload.get("index")
    if index in chunks:
        print("duplicate chunk", index, "ignored")
        return
    chunks[index] = payload
    print("chunk", index, "received")


def verify_update(timed_out):
    if manifest is None:
        return None, "no manifest received"

    if manifest.get("total_chunks") != 4:
        return None, "manifest says " + str(manifest.get("total_chunks")) + " chunks, expected 4"

    if set(chunks.keys()) != {0, 1, 2, 3}:
        reason = "received chunk indices " + str(list(chunks.keys())) + ", expected [0, 1, 2, 3]"
        if timed_out:
            reason = "timeout after " + str(TIMEOUT) + "s, " + reason
        return None, reason

    data = []
    for i in range(4):
        if chunks[i].get("version") != manifest.get("version"):
            return None, "chunk " + str(i) + " is for version " + str(chunks[i].get("version")) + \
                   " but manifest is version " + str(manifest.get("version"))
        try:
            data.append(base64.b64decode(chunks[i]["data"], validate=True))
        except (KeyError, ValueError):
            return None, "chunk " + str(i) + " has invalid data"

    firmware = b"".join(data)  # in index order, not arrival order
    if len(firmware) != manifest.get("firmware_size"):
        return None, "size mismatch: got " + str(len(firmware)) + " bytes, manifest says " + \
               str(manifest.get("firmware_size"))

    root = merkle_root(data).hex()
    if root != manifest.get("merkle_root"):
        return None, "merkle root mismatch: calculated " + root + ", manifest has " + str(manifest.get("merkle_root"))

    return firmware, None


def main():
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="ota-client")
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_subscribe = on_subscribe
    client.connect(BROKER_HOST, BROKER_PORT)
    client.loop_start()

    print("waiting for update...")
    timed_out = False
    while True:
        time.sleep(0.5)
        if manifest is not None and len(chunks) >= 4:
            break
        if first_message_time is not None and time.time() - first_message_time > TIMEOUT:
            timed_out = True
            break

    client.loop_stop()
    client.disconnect()

    firmware, reason = verify_update(timed_out)
    if firmware is None:
        chunks.clear()
        log("update REJECTED: " + reason)
        return

    with open(OUTPUT_FILE, "wb") as f:
        f.write(firmware)
    log("update ACCEPTED: version " + manifest["version"] + ", " + str(len(firmware)) + " bytes written to " + OUTPUT_FILE)


if __name__ == "__main__":
    main()
