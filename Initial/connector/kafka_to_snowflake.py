#!/usr/bin/env python3
"""
Kafka -> Snowflake Bronze loader (append-only)

- Subscribes to 7 topics.
- Creates Bronze tables if missing.
- Optionally truncates Bronze once at startup (for clean tests).
- Batches inserts using VALUES .. SELECT PARSE_JSON(column1).

Requirements:
  pip install kafka-python snowflake-connector-python
  # If your Kafka topics are lz4-compressed, also:
  pip install lz4
"""

from kafka import KafkaConsumer
import snowflake.connector
import json
import time
from collections import defaultdict
from typing import Dict, List, Tuple

# -----------------------------
# Configuration (edit these)
# -----------------------------
BROKERS   = ["localhost:9092"]
GROUP_ID  = f"snowflake-demo-{int(time.time())}"  # fresh group each run (so we read from earliest)
AUTO_OFFSET_RESET = "earliest"

SNOWFLAKE = dict(
    account   = "SFEDU02-GVB78310",
    user      = "GOPHER",
    password  = "Kup0511012001#",            
    role      = "TRAINING_ROLE",
    warehouse = "GOPHER_WH",
    database  = "GOPHER_DB",
    schema    = "DEMO_BRONZE",
)

CLEAN_BRONZE_ON_START = False

# Throughput tuning
BATCH_SIZE     = 10_000        # rows per table per flush
BATCH_TIMEOUTS = 2             # seconds between time-based flushes
CHUNK_SIZE     = 1_000         # rows per INSERT statement

TOPIC_TO_TABLE: Dict[str, str] = {
    "behav.stream_session":   "STREAM_SESSION_RAW",
    "behav.engagement":       "ENGAGEMENT_RAW",
    "behav.search":           "SEARCH_RAW",
    "behav.browse":           "BROWSE_RAW",
    "behav.tile_impression":  "TILE_IMPR_RAW",
    "behav.commerce":         "COMMERCE_RAW",
    "behav.notification":     "NOTIFICATION_RAW",
}

# -----------------------------
# Snowflake helpers
# -----------------------------
BRONZE_TABLES = list(TOPIC_TO_TABLE.values())

DDL_CREATE_CANONICAL = """
CREATE TABLE IF NOT EXISTS {tbl} (
  DATA           VARIANT,
  INGESTED_AT    TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
  SRC_TOPIC      STRING,
  SRC_PARTITION  INTEGER,
  SRC_OFFSET     NUMBER
)
"""

def ensure_bronze_tables(cur):
    for t in BRONZE_TABLES:
        cur.execute(DDL_CREATE_CANONICAL.format(tbl=t))
        # Ensure DATA is VARIANT in case LIKE lost type
        

def truncate_bronze(cur):
    for t in BRONZE_TABLES:
        cur.execute(f"TRUNCATE TABLE IF EXISTS {t}")

def insert_batch(cur, table: str, rows: List[Tuple[str, str, int, int]]):
    """
    rows: list of tuples (json_str, topic, partition, offset)
    Uses chunking to avoid very large SQL statements.
    """
    if not rows:
        return
    for i in range(0, len(rows), CHUNK_SIZE):
        chunk = rows[i:i+CHUNK_SIZE]
        # VALUES (%s,%s,%s,%s), ... then parse column1 into VARIANT
        values_clause = ",".join(["(%s,%s,%s,%s)"] * len(chunk))
        sql = f"""
        INSERT INTO {table} (DATA, SRC_TOPIC, SRC_PARTITION, SRC_OFFSET)
        SELECT PARSE_JSON(column1), column2, column3, column4
        FROM VALUES {values_clause}
        """
        # Flatten params: [(json, topic, part, offset), ...] -> tuple sequence
        params: List = []
        for j, tp, prt, off in chunk:
            params.extend([j, tp, prt, off])
        cur.execute(sql, params)

# -----------------------------
# Main
# -----------------------------
def main():
    # Kafka consumer
    consumer = KafkaConsumer(
        *TOPIC_TO_TABLE.keys(),
        bootstrap_servers=BROKERS,
        auto_offset_reset=AUTO_OFFSET_RESET,
        enable_auto_commit=True,
        group_id=GROUP_ID,
        value_deserializer=lambda x: x.decode("utf-8", errors="ignore"),
        max_poll_records=5000,
        fetch_max_bytes=52_428_800,          # 50MB
        fetch_min_bytes=1_048_576,           # 1MB
        max_partition_fetch_bytes=10_485_760 # 10MB
    )

    # Snowflake connection
    conn = snowflake.connector.connect(
        account   = SNOWFLAKE["account"],
        user      = SNOWFLAKE["user"],
        password  = SNOWFLAKE["password"],
        role      = SNOWFLAKE["role"],
        warehouse = SNOWFLAKE["warehouse"],
        database  = SNOWFLAKE["database"],
        schema    = SNOWFLAKE["schema"],
        autocommit=False,
        session_parameters={'STATEMENT_TIMEOUT_IN_SECONDS': 3600}
    )
    cur = conn.cursor()

    # Prepare Bronze tables
    ensure_bronze_tables(cur)
    if CLEAN_BRONZE_ON_START:
        truncate_bronze(cur)
    conn.commit()

    # Batch buffers per table
    buffers: Dict[str, List[Tuple[str, str, int, int]]] = defaultdict(list)
    last_flush = time.time()
    total_inserted = 0
    start = time.time()

    try:
        for msg in consumer:
            table = TOPIC_TO_TABLE.get(msg.topic)
            if not table:
                continue

            val = msg.value.strip()
            if not val:
                continue

            # basic JSON validation; skip malformed lines
            try:
                json.loads(val)
            except Exception:
                continue

            buffers[table].append((val, msg.topic, msg.partition, msg.offset))

            # Size-based flush
            if len(buffers[table]) >= BATCH_SIZE:
                insert_batch(cur, table, buffers[table])
                total_inserted += len(buffers[table])
                buffers[table].clear()
                conn.commit()

            # Time-based flush (partial batches)
            now = time.time()
            if now - last_flush >= BATCH_TIMEOUTS:
                for tname, rows in list(buffers.items()):
                    if rows:
                        insert_batch(cur, tname, rows)
                        total_inserted += len(rows)
                        buffers[tname].clear()
                conn.commit()
                last_flush = now

            # Light progress line every ~15s
            if int(now - start) % 15 == 0 and (now - last_flush) < 1.0:
                elapsed = max(1.0, now - start)
                rate = int(total_inserted / elapsed)
                print(f"progress: {total_inserted:,} rows, {rate:,}/s, elapsed {int(elapsed)}s")

    except KeyboardInterrupt:
        pass
    finally:
        # Final flush
        for tname, rows in buffers.items():
            if rows:
                insert_batch(cur, tname, rows)
                total_inserted += len(rows)
        conn.commit()

        elapsed = max(1.0, time.time() - start)
        rate = int(total_inserted / elapsed)
        print(f"done: {total_inserted:,} rows, {rate:,}/s, elapsed {int(elapsed)}s")

        cur.close()
        conn.close()
        consumer.close()

if __name__ == "__main__":
    main()
