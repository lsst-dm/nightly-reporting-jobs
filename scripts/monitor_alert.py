from confluent_kafka import Consumer
import fastavro
import io
import json
import os
from collections import defaultdict
import time
import signal
import sys

# Graceful shutdown handler
shutdown = False


def visit_to_seqnum(visit):
    return int(visit) % 100000


def visit_to_dayobs(visit):
    return int(visit) // 100000


def signal_handler(sig, frame):
    global shutdown
    print("\n>>> Shutdown signal received <<<")
    shutdown = True


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

schema_base_dir = os.environ.get("ALERT_PACKET_DIR")
if not schema_base_dir:
    raise ValueError("ALERT_PACKET_DIR environment variable not set")
schema_dir = f"{schema_base_dir}/python/lsst/alert/packet/schema/10/0/"

schema_files = [
    f"{schema_dir}lsst.v10_0.diaSource.avsc",
    f"{schema_dir}lsst.v10_0.diaForcedSource.avsc",
    f"{schema_dir}lsst.v10_0.diaObject.avsc",
    f"{schema_dir}lsst.v10_0.ssSource.avsc",
    f"{schema_dir}lsst.v10_0.ssObject.avsc",
    f"{schema_dir}lsst.v10_0.mpc_orbits.avsc",
    f"{schema_dir}lsst.v10_0.alert.avsc",
]

named_schemas = {}
for schema_file in schema_files:
    with open(schema_file, "r") as f:
        schema_dict = json.load(f)
        parsed_schema = fastavro.parse_schema(schema_dict, named_schemas)

alert_schema = parsed_schema
print(f"Loaded schema: {alert_schema['name']}")

kafka_password = os.environ.get("ALERT_PASSWORD")
if not kafka_password:
    raise ValueError("ALERT_PASSWORD environment variable not set")

conf = {
    "bootstrap.servers": "usdf-alert-stream-dev.lsst.cloud:9094",
    "group.id": "kafka-alert-consumer-k8s",
    "auto.offset.reset": "latest",
    "enable.auto.commit": True,
    "security.protocol": "SASL_PLAINTEXT",
    "sasl.mechanism": "SCRAM-SHA-512",
    "sasl.username": "kafka-admin",
    "sasl.password": kafka_password,
}

consumer = Consumer(conf)
consumer.subscribe(["lsst-alerts-v10.0"])

# Track visits with their alert counts and last seen time
visit_data = defaultdict(lambda: {"count": 0, "last_seen": 0})
total_messages = 0
last_summary_time = time.time()

SUMMARY_INTERVAL = int(os.environ.get("SUMMARY_INTERVAL", "60"))  # Default 1 minute
EXPIRY_TIME = int(os.environ.get("EXPIRY_TIME", "600"))  # Default 10 minutes


def print_summary():
    """Print summary of active visits"""
    current_time = time.time()

    # Remove expired visits
    expired_visits = []
    for visit_id, data in visit_data.items():
        if current_time - data["last_seen"] > EXPIRY_TIME:
            expired_visits.append(visit_id)

    for visit_id in expired_visits:
        del visit_data[visit_id]

    # Print summary
    print("\n" + "=" * 60)
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] PERIODIC SUMMARY")
    print("=" * 60)
    print(f"Total messages processed: {total_messages}")
    print(f"Active visits (last 10 minutes): {len(visit_data)}")

    if visit_data:
        # Sort by visit ID
        for visit_id in sorted(visit_data.keys()):
            count = visit_data[visit_id]["count"]
            print(
                f'{{"visit": {visit_id}, "alert_count": {count}, "day_obs": {visit_to_dayobs(visit_id)}, "seqnum": {visit_to_seqnum(visit_id)}}}'
            )
    else:
        print("\nNo active visits")

    print("=" * 60 + "\n")


print(f"Starting continuous consumer...")
print(f"- Summary interval: {SUMMARY_INTERVAL} seconds (1 minute)")
print(f"- Visit expiry: {EXPIRY_TIME} seconds (10 minutes)")
print("Waiting for messages...\n")

try:
    while not shutdown:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            # Check if it's time for periodic summary
            if time.time() - last_summary_time >= SUMMARY_INTERVAL:
                print_summary()
                last_summary_time = time.time()
            continue

        if msg.error():
            print(f"[ERROR] {msg.error()}")
            continue

        # Decode message
        avro_bytes = msg.value()[5:]
        bytes_io = io.BytesIO(avro_bytes)
        alert_data = fastavro.schemaless_reader(bytes_io, alert_schema)

        # Extract visit and update tracking
        visit = alert_data["diaSource"]["visit"]
        current_time = time.time()

        visit_data[visit]["count"] += 1
        visit_data[visit]["last_seen"] = current_time
        total_messages += 1

        # Check if it's time for periodic summary
        if current_time - last_summary_time >= SUMMARY_INTERVAL:
            print_summary()
            last_summary_time = current_time

except Exception as e:
    print(f"\n[ERROR] Exception occurred: {e}")

finally:
    consumer.close()

    # Final summary on shutdown
    print("\n" + "=" * 60)
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] FINAL SUMMARY")
    print("=" * 60)
    print(f"Total messages processed: {total_messages}")
    print(f"Total unique visits seen: {len(visit_data)}")

    if visit_data:
        for visit_id in sorted(visit_data.keys()):
            count = visit_data[visit_id]["count"]
            print(
                f'{{"visit": {visit_id}, "alert_count": {count}, "day_obs": {visit_to_dayobs(visit_id)}, "seqnum": {visit_to_seqnum(visit_id)}}}'
            )
    else:
        print("\nNo visits recorded")

    print("=" * 60)
