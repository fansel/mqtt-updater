import base64
import hashlib
import json
import os

import paho.mqtt.client as mqtt
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

FIRMWARE_FILE = os.path.join(BASE_DIR, os.environ["FIRMWARE_FILE"])
FIRMWARE_VERSION = os.environ["FIRMWARE_VERSION"]
OUTPUT_DIR = os.path.join(BASE_DIR, os.environ["OUTPUT_DIR"])

BROKER_HOST = os.environ["BROKER_HOST"]
BROKER_PORT = int(os.environ["BROKER_PORT"])
QOS = int(os.environ["QOS"])
TOPIC_BASE = os.environ["TOPIC_BASE"]


def split_firmware(data):
    if len(data) < 4:
        raise ValueError("firmware too small, need at least 4 bytes")

    base = len(data) // 4
    rest = len(data) % 4

    chunks = []
    start = 0
    for i in range(4):
        size = base
        if i < rest:
            size += 1
        chunks.append(data[start:start + size])
        start += size
    return chunks


def merkle_root(chunks):
    h0 = hashlib.sha256(chunks[0]).digest()
    h1 = hashlib.sha256(chunks[1]).digest()
    h2 = hashlib.sha256(chunks[2]).digest()
    h3 = hashlib.sha256(chunks[3]).digest()
    h01 = hashlib.sha256(h0 + h1).digest()
    h23 = hashlib.sha256(h2 + h3).digest()

    return hashlib.sha256(h01 + h23).digest()


def prepare():
    # step 1: split firmware, build merkle root, write chunks + manifest to OUTPUT_DIR
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


def load_update():
    with open(os.path.join(OUTPUT_DIR, "manifest.json"), "r") as f:
        manifest = json.load(f)

    chunks = []
    for entry in manifest["chunks"]:
        with open(os.path.join(OUTPUT_DIR, entry["filename"]), "rb") as f:
            chunks.append(f.read())
        print("read", entry["filename"], "(" + str(len(chunks[-1])) + " bytes)")

    return manifest, chunks


def main(mode="all"):
    if mode in ("all", "prepare"):
        prepare()
    if mode in ("all", "publish"):
        manifest, chunks = load_update()
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
    import sys

    main(sys.argv[1] if len(sys.argv) > 1 else "all")
