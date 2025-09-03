"""
Conversion service with Redis join

Consumes `raw-electrical-data` and `correlation-data` topics, stores raw data
until correlation data arrives, matches messages by sweeps_id (first or second),
performs temperature/humidity calculation, and publishes results to `conversion-data`.
"""

from __future__ import annotations

import asyncio
import gc
import os
import threading
from concurrent.futures import ProcessPoolExecutor

import numpy as np
from kafka import KafkaConsumer, KafkaProducer
from redis import asyncio as aioredis

from app.core.utils.kafka_helper import (
    BOOTSTRAP_SERVERS,
    TOPICS,
    get_consumer,
    get_producer,
    serialize_array,
    update_topic_partition,
)
from app.core.utils.maths import deserialize_array, points_fibers, temperature_humidity_calculation

# -------------------------
# Topic constants
# -------------------------
TOPICS_IN_RAW: str = TOPICS["raw-electrical"]
TOPICS_IN_CORR: str = TOPICS["correlation"]
TOPICS_OUT: str = TOPICS["conversion"]

# Redis settings
REDIS_URL: str = os.getenv("REDIS_URL", "redis://redis:6379/0")
REDIS_TTL: int = 60  # seconds

# -------------------------
# Kafka consumption helpers
# -------------------------
def _blocking_consumer_loop(consumer: KafkaConsumer, put_coroutine, topic_name: str) -> None:
    print(f"--- Starting blocking consumer loop for topic {topic_name} ---")
    try:
        for msg in consumer:
            try:
                print(f"[{topic_name}] Message received, size={len(msg.value)} bytes")
                arr: np.ndarray = deserialize_array(msg.value)

                sweeps_ids: list[str] = []
                if msg.headers:
                    print(f"[{topic_name}] Headers: {msg.headers}")
                    for k, v in msg.headers:
                        if k in ("first_sweeps_id", "second_sweeps_id"):
                            sweeps_ids.append(v.decode())

                if not sweeps_ids:
                    print(f"[{topic_name}] Missing sweeps_id headers, skipping")
                    continue

                print(f"[{topic_name}] Scheduling message with sweeps_ids={sweeps_ids}")
                put_coroutine((sweeps_ids, arr, topic_name))

            except Exception as e:
                print(f"[{topic_name}] Error consuming message: {e}")
    except Exception as e:
        print(f"[{topic_name}] Blocking consumer loop exited with error: {e}")
    finally:
        try:
            consumer.close()
            print(f"[{topic_name}] Consumer closed")
        except Exception:
            pass

async def consume_to_queue_in_thread(
    consumer: KafkaConsumer,
    queue: asyncio.Queue,
    topic_name: str,
    loop: asyncio.AbstractEventLoop,
) -> None:
    print(f"--- Launching consumer thread for topic {topic_name} ---")

    def schedule_put(item: tuple[list[str], np.ndarray, str]) -> None:
        asyncio.run_coroutine_threadsafe(queue.put(item), loop)
        print(f"[{topic_name}] Item scheduled to queue, sweeps_ids={item[0]}")

    thread = threading.Thread(
        target=_blocking_consumer_loop,
        args=(consumer, schedule_put, topic_name),
        daemon=True,
    )
    thread.start()
    print(f"--- Thread started for {topic_name} ---")

# -------------------------
# Redis helpers
# -------------------------
async def store_raw(redis: aioredis.Redis, sweeps_id: str, arr: np.ndarray) -> None:
    key = f"sweeps:{sweeps_id}:raw"
    await redis.set(key, serialize_array(arr), ex=REDIS_TTL)
    print(f"[Redis] Stored raw for sweeps_id={sweeps_id} under key={key}")

async def fetch_raw(redis: aioredis.Redis, sweeps_id: str) -> np.ndarray | None:
    key = f"sweeps:{sweeps_id}:raw"
    data = await redis.get(key)
    if data is None:
        print(f"[Redis] No raw found for sweeps_id={sweeps_id}")
        return None
    print(f"[Redis] Fetched raw for sweeps_id={sweeps_id}")
    return deserialize_array(data)

async def delete_raw(redis: aioredis.Redis, sweeps_id: str) -> None:
    key = f"sweeps:{sweeps_id}:raw"
    await redis.delete(key)
    print(f"[Redis] Deleted raw for sweeps_id={sweeps_id}")

# -------------------------
# Processing
# -------------------------
async def process_pair(
    sweeps_id: str,
    raw_data: np.ndarray,
    corr_data: np.ndarray,
    producer: KafkaProducer,
    loop: asyncio.AbstractEventLoop,
    process_executor: ProcessPoolExecutor,
) -> None:
    print(f"--- process_pair started for sweeps_id={sweeps_id} ---")
    try:
        points_temperature, points_humidity, temperature_sensor_per_point, humidity_sensor_per_point = points_fibers(
            min_temperature=1743,
            max_temperature=1929,
            min_humidity=1556,
            max_humidity=1742,
            x_average=0,
            check_reversed=True,
            points_sensor={0: (1000, 2000), 1: (2100, 3100)},
        )

        print(f"[{sweeps_id}] Running temperature/humidity calculation...")
        result: np.ndarray = await loop.run_in_executor(
            process_executor,
            temperature_humidity_calculation,
            corr_data,
            raw_data[0:2, :],
            0, 1.57, 0.18, 1.39,
            points_temperature,
            points_humidity,
            temperature_sensor_per_point,
            humidity_sensor_per_point,
        )
        print(f"[{sweeps_id}] Calculation complete, result shape={result.shape}")

        result_bytes: bytes = await asyncio.to_thread(serialize_array, result)
        future = producer.send(
            TOPICS_OUT,
            key=sweeps_id.encode(),
            value=result_bytes,
            headers=[("sweeps_id", sweeps_id.encode())],
        )
        record_metadata = future.get(timeout=10)
        print(f"[Kafka] Produced sweeps_id={sweeps_id} -> topic={record_metadata.topic}, "
              f"partition={record_metadata.partition}, offset={record_metadata.offset}")

        del raw_data, corr_data, result
        gc.collect()
    except Exception as e:
        print(f"[{sweeps_id}] Error in process_pair: {e}")

# -------------------------
# Main conversion service
# -------------------------
async def run_conversion_service() -> None:
    print("--- Starting Conversion Service ---")
    raw_queue: asyncio.Queue = asyncio.Queue()
    corr_queue: asyncio.Queue = asyncio.Queue()
    loop = asyncio.get_running_loop()
    process_executor = ProcessPoolExecutor(max_workers=max(1, os.cpu_count() - 1))

    raw_consumer = get_consumer(topic=TOPICS_IN_RAW, bootstrap_servers=BOOTSTRAP_SERVERS, group_id="conversion-service-group")
    corr_consumer = get_consumer(topic=TOPICS_IN_CORR, bootstrap_servers=BOOTSTRAP_SERVERS, group_id="conversion-service-group")
    
    producer = get_producer(bootstrap_servers=BOOTSTRAP_SERVERS)
    update_topic_partition(topic=TOPICS_OUT, partition=8, replication_factor=2)
    
    redis = await aioredis.from_url(REDIS_URL, decode_responses=False)

    await consume_to_queue_in_thread(raw_consumer, raw_queue, "Raw", loop)
    await consume_to_queue_in_thread(corr_consumer, corr_queue, "Correlation", loop)

    print("--- Consumers initialized and running ---")

    try:
        while True:
            print("--- Waiting for next message from queues ---")
            done, _ = await asyncio.wait(
                {asyncio.create_task(raw_queue.get()), asyncio.create_task(corr_queue.get())},
                return_when=asyncio.FIRST_COMPLETED,
            )

            done_task = list(done)[0]
            sweeps_ids, arr, topic = done_task.result()
            print(f"[Queue] Got message from topic={topic} with sweeps_ids={sweeps_ids}")

            if topic == "Raw":
                for sid in sweeps_ids:
                    await store_raw(redis, sid, arr)

            elif topic == "Correlation":
                matched_raw = None
                matched_sweeps_id = None
                for sid in sweeps_ids:
                    raw = await fetch_raw(redis, sid)
                    if raw is not None:
                        matched_raw = raw
                        matched_sweeps_id = sid
                        break
                if matched_raw is not None:
                    print(f"[{matched_sweeps_id}] Matching raw found, processing pair")
                    await delete_raw(redis, matched_sweeps_id)
                    await process_pair(matched_sweeps_id, matched_raw, arr, producer, loop, process_executor)
                else:
                    print(f"No matching raw found yet for correlation, skipping processing")

    except KeyboardInterrupt:
        print("--- Conversion service stopping ---")
    finally:
        print("--- Cleaning up resources ---")
        try:
            producer.flush()
            producer.close()
        except Exception:
            pass
        try:
            process_executor.shutdown(wait=False)
        except Exception:
            pass
        try:
            raw_consumer.close()
            corr_consumer.close()
        except Exception:
            pass
        await redis.aclose()
        gc.collect()
        print("--- Conversion service stopped ---")

if __name__ == "__main__":
    try:
        asyncio.run(run_conversion_service())
    except KeyboardInterrupt:
        print("Conversion service interrupted and stopped.")
