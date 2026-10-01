import base64
import hashlib
import json
import os

import paho.mqtt.client as mqtt

FIRMWARE_FILE = "firmware.txt"
FIRMWARE_VERSION = "1.0.0"
OUTPUT_DIR = "output"

BROKER_HOST = "172.20.10.3"
BROKER_PORT = 1883
QOS = 1
TOPIC_BASE = "ota/firmware"


def split_firmware(data):
    if len(data) < 4:
        raise ValueError("firmware too small, need at least 4 bytes")

    base = len(data) // 4  #9
    print("base:", base)
    rest = len(data) % 4   #1
    print("rest:", rest)

    chunks = []
    start = 0
    print("start:", start)
    for i in range(4):
        size = base #9
        print("size:", size)
        if i < rest:
            size += 1
            print("new size:", size)
        chunks.append(data[start:start + size])
        print("array is from ", start, " to ", start + size, "")
        start += size
        print("start:", start)
    return chunks


def merkle_root(chunks):
    h0 = hashlib.sha256(chunks[0]).digest()
    h1 = hashlib.sha256(chunks[1]).digest()
    h2 = hashlib.sha256(chunks[2]).digest()
    h3 = hashlib.sha256(chunks[3]).digest()
    h01 = hashlib.sha256(h0 + h1).digest()
    h23 = hashlib.sha256(h2 + h3).digest()

    return hashlib.sha256(h01 + h23).digest()


def main():
    with open(FIRMWARE_FILE, "rb") as f:
        firmware = f.read()

    chunks = split_firmware(firmware)
    root = merkle_root(chunks)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    chunk_list = []
    for i in range(len(chunks)):
        filename = "chunk_" + str(i) + ".bin"
        with open(os.path.join(OUTPUT_DIR, filename), "wb") as f:
            f.write(chunks[i])
        chunk_list.append({"index": i, "filename": filename})

    manifest = {
        "version": FIRMWARE_VERSION,
        "firmware_size": len(firmware),
        "total_chunks": len(chunks),
        "merkle_root": root.hex(),
        "chunks": chunk_list,
    }

    with open(os.path.join(OUTPUT_DIR, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2)

    print("manifest created:")
    print(json.dumps(manifest, indent=2))

    publish_update(manifest, chunks)


def publish_update(manifest, chunks):
    # topics: ota/firmware/<version>/manifest
    #         ota/firmware/<version>/chunk/<index>
    version = manifest["version"]

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="ota-server")
    client.connect(BROKER_HOST, BROKER_PORT)
    client.loop_start()

    # manifest first so the client knows what to expect
    topic = TOPIC_BASE + "/" + version + "/manifest"
    info = client.publish(topic, json.dumps(manifest), qos=QOS)
    info.wait_for_publish()
    print("published", topic)

    for i in range(len(chunks)):
        # binary data as base64 inside json, version + index also in payload
        payload = {
            "version": version,
            "index": i,
            "filename": manifest["chunks"][i]["filename"],
            "data": base64.b64encode(chunks[i]).decode("ascii"),
        }
        topic = TOPIC_BASE + "/" + version + "/chunk/" + str(i)
        info = client.publish(topic, json.dumps(payload), qos=QOS)
        info.wait_for_publish()
        print("published", topic, "(" + str(len(chunks[i])) + " bytes)")

    client.loop_stop()
    client.disconnect()


if __name__ == "__main__":
    main()
