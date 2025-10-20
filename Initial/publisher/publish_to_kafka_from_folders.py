#!/usr/bin/env python3
# Publish NDJSON files (YYYY-MM-DD/HH/bronze_*.ndjson) to Kafka topics.
# Requires: confluent_kafka (pip install confluent-kafka)

import argparse, json, time
from pathlib import Path
from confluent_kafka import Producer

FILE_TO_TOPIC = {
  "bronze_stream_session.ndjson":   "behav.stream_session",
  "bronze_engagement_event.ndjson": "behav.engagement",
  "bronze_search_event.ndjson":     "behav.search",
  "bronze_browse_event.ndjson":     "behav.browse",
  "bronze_tile_impression.ndjson":  "behav.tile_impression",
  "bronze_commerce_event.ndjson":   "behav.commerce",
  "bronze_notification_event.ndjson":"behav.notification",
}

def make_producer(bootstrap, acks="all"):
  return Producer({
    "bootstrap.servers": bootstrap,
    "enable.idempotence": True,
    "acks": acks,
    "compression.type": "lz4",
    "linger.ms": 20,
    "batch.size": 131072,
    "retries": 10,
    "request.timeout.ms": 30000,
  })

def key_for(record, pref):
  if pref == "none":
    return None
  val = record.get(pref)
  return None if val is None else str(val).encode("utf-8")

def publish_dir(producer, root: Path, partition_key: str, max_per_sec: float):
  files = sorted(root.rglob("bronze_*.ndjson"))
  sent_total = 0
  t0 = time.time()
  for f in files:
    topic = FILE_TO_TOPIC.get(f.name)
    if not topic:
      continue
    with f.open("r", encoding="utf-8") as fh:
      for line in fh:
        line = line.strip()
        if not line:
          continue
        try:
          rec = json.loads(line)
        except json.JSONDecodeError:
          continue
        key = key_for(rec, partition_key)
        producer.produce(topic, value=line.encode("utf-8"), key=key)
        sent_total += 1
        if max_per_sec > 0:
          expected = t0 + sent_total / max_per_sec
          now = time.time()
          if now < expected:
            time.sleep(expected - now)
    producer.flush()
    print(f"✓ {topic}: {f} published")
  producer.flush()
  print(f"Done. Records sent: {sent_total}")

def main():
  ap = argparse.ArgumentParser(description="Publish NDJSON hour folders to Kafka topics")
  ap.add_argument("--root", required=True, help="Root folder with YYYY-MM-DD/HH/bronze_*.ndjson")
  ap.add_argument("--brokers", required=True, help="Kafka bootstrap servers, e.g. localhost:9092")
  ap.add_argument("--partition-key", default="session_id", choices=["session_id","user_id","content_id","none"])
  ap.add_argument("--rate", type=float, default=0.0, help="Max messages/sec (0 = as fast as possible)")
  ap.add_argument("--acks", default="all")
  args = ap.parse_args()

  prod = make_producer(args.brokers, args.acks)
  try:
    publish_dir(prod, Path(args.root), args.partition_key, args.rate)
  finally:
    prod.flush()

if __name__ == "__main__":
  main()
