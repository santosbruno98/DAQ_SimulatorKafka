"""
Conversion service with Redis join

Consumes `raw-electrical-data` (only type=elec) and `correlation-data` topics,
stores raw data in Redis by sweeps_id until correlation data arrives,
then matches by sweeps_id (first_sweeps_id or second_sweeps_id),
performs temperature/humidity calculation, and publishes results to `conversion-data`.
"""

from __future__ import annotations

import asyncio
import gc
import os
import threading
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass

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
from app.core.utils.maths import (
    deserialize_array,
    points_fibers,
    temperature_humidity_calculation,
)

# Configuration
TOPICS_IN_RAW: str = TOPICS["raw-electrical"]
TOPICS_IN_CORR: str = TOPICS["correlation"]
TOPICS_OUT: str = TOPICS["conversion"]

REDIS_URL: str = os.getenv("REDIS_URL", "redis://redis:6379/0")
REDIS_TTL: int = 300
RETRY_INTERVAL: int = 3


@dataclass # The decorator automatically generates __init__, __repr__, __eq__ methods
class PendingCorrelation:
    sweeps_ids: list[str]
    corr_data: np.ndarray
    timestamp: float
    retry_count: int = 0


def _blocking_consumer_loop(
    consumer: KafkaConsumer, put_coroutine, topic_name: str
) -> None:
    """Blocking Kafka consumer."""
    print(f"[{topic_name}] Consumer loop started")
    try:
        for msg in consumer:
            try:
                sweeps_ids: list[str] = []
                msg_type: str | None = None

                if msg.headers:
                    for k, v in msg.headers:
                        if topic_name == TOPICS_IN_RAW:
                            if k == "type":
                                msg_type = v.decode()
                            if k == "sweeps_id":
                                sweeps_ids.append(v.decode())
                        elif topic_name == TOPICS_IN_CORR:
                            if k in ("first_sweeps_id", "second_sweeps_id"):
                                sweeps_ids.append(v.decode())

                # Skip non-elec for raw topic
                if topic_name == TOPICS_IN_RAW and msg_type != "elec":
                    continue

                if not sweeps_ids:
                    continue

                arr: np.ndarray = deserialize_array(msg.value)
                print(f"[{topic_name}] Processing: {sweeps_ids}")
                put_coroutine((sweeps_ids, arr, topic_name))

            except Exception as e:
                print(f"[{topic_name}] Error in message processing: {e}")

    except Exception as e:
        print(f"[{topic_name}] Consumer error: {e}")
    finally:
        try:
            consumer.close()
        except Exception:
            pass


async def consume_to_queue_in_thread(
    consumer: KafkaConsumer, queue: asyncio.Queue, topic_name: str, loop
) -> None:
    """Start consumer in thread."""

    def schedule_put(item):
        try:
            asyncio.run_coroutine_threadsafe(queue.put(item), loop)
        except Exception as e:
            print(f"[{topic_name}] Error scheduling to queue: {e}")

    thread = threading.Thread(
        target=_blocking_consumer_loop,
        args=(consumer, schedule_put, topic_name),
        daemon=True,
    )
    thread.start()
    print(f"[{topic_name}] Thread started")


# Redis operations
async def store_raw(redis: aioredis.Redis, sweeps_id: str, arr: np.ndarray) -> bool:
    """Store raw electrical data."""
    try:
        key = f"sweeps:{sweeps_id}"
        await redis.set(key, serialize_array(arr), ex=REDIS_TTL)
        print(f"[Redis] Stored {sweeps_id}")
        return True
    except Exception as e:
        print(f"[Redis] Error storing {sweeps_id}: {e}")
        return False


async def fetch_raw(redis: aioredis.Redis, sweeps_id: str) -> np.ndarray | None:
    """Fetch raw electrical data."""
    try:
        key = f"sweeps:{sweeps_id}"
        data = await redis.get(key)
        if data is None:
            return None
        return deserialize_array(data)
    except Exception as e:
        print(f"[Redis] Error fetching {sweeps_id}: {e}")
        return None


async def delete_raw(redis: aioredis.Redis, sweeps_id: str) -> None:
    """Delete raw electrical data."""
    try:
        key = f"sweeps:{sweeps_id}"
        await redis.delete(key)
    except Exception as e:
        print(f"[Redis] Error deleting {sweeps_id}: {e}")


async def check_availability(
    redis: aioredis.Redis, sweeps_ids: list[str]
) -> dict[str, bool]:
    """Check which sweeps are available."""
    availability = {}
    for sweep_id in sweeps_ids:
        try:
            key = f"sweeps:{sweep_id}"
            exists = await redis.exists(key)
            availability[sweep_id] = bool(exists)
        except Exception as e:
            print(f"[Redis] Error checking {sweep_id}: {e}")
            availability[sweep_id] = False
    return availability


async def process_correlation_pair(
    sweeps_ids: list[str],
    corr_data: np.ndarray,
    redis: aioredis.Redis,
    producer: KafkaProducer,
    loop,
    process_executor: ProcessPoolExecutor,
) -> bool:
    """Process correlation pair if all raw data is available."""
    # Check if all required raw data is available
    availability = await check_availability(redis, sweeps_ids)
    missing = [
        sweep_id for sweep_id, available in availability.items() if not available
    ]

    if missing:
        print(f"[Correlation] Missing raw data for: {missing}")
        print(
            f"[Correlation] Available: {[sweep_id for sweep_id, available in availability.items() if available]}"
        )
        return False

    print(f"[Correlation] All raw data found for {sweeps_ids}")

    # Fetch all raw arrays
    raw_arrays = []
    for sweep_id in sweeps_ids:
        raw_arr = await fetch_raw(redis, sweep_id)
        if raw_arr is None:
            print(f"[Correlation] Raw data disappeared for {sweep_id}")
            return False
        raw_arrays.append(raw_arr)

    if len(raw_arrays) > 1:
        combined_elec_data = np.vstack(raw_arrays)
        print(f"[Processing] Combined {len(raw_arrays)} arrays")
    else:
        combined_elec_data = raw_arrays[0]
    try:
        primary_sweeps_id = sweeps_ids[-1]  # Use last sweep_id as key

        (
            points_temperature,
            points_humidity,
            temperature_sensor_per_point,
            humidity_sensor_per_point,
        ) = points_fibers(
            min_temperature=1743,
            max_temperature=1929,
            min_humidity=1556,
            max_humidity=1742,
            x_average=0,
            check_reversed=True,
            points_sensor={0: (1000, 2000), 1: (2100, 3100)},
        )

        # Run calculation
        conversion: np.ndarray = await loop.run_in_executor(
            process_executor,
            temperature_humidity_calculation,
            corr_data,
            combined_elec_data,
            0,
            1.57,
            0.18,
            1.39,
            points_temperature,
            points_humidity,
            temperature_sensor_per_point,
            humidity_sensor_per_point,
        )

        print(f"[Processing] Calculation complete, result shape: {conversion.shape}")
        # Publish to conversion topic
        conversion_bytes: bytes = await asyncio.to_thread(serialize_array, conversion)
        future = producer.send(
            TOPICS_OUT,
            key=primary_sweeps_id.encode(),
            value=conversion_bytes,
            headers=[("sweeps_id", primary_sweeps_id.encode())],
        )
        record_metadata = future.get(timeout=10)
        print(
            f"[SUCCESS] Published {primary_sweeps_id}"
            + f"{record_metadata.topic}:{record_metadata.partition}:{record_metadata.offset}"
        )

        # Clean up Redis after successful processing
        for sweep_id in sweeps_ids:
            await delete_raw(redis, sweep_id)

        # Memory cleanup
        del combined_elec_data, corr_data, conversion
        gc.collect()

        return True

    except Exception as e:
        print(f"[ERROR] Processing failed for {sweeps_ids}: {e}")
        return False


async def periodic_retry_correlations(
    pending_correlations: list[PendingCorrelation],
    redis: aioredis.Redis,
    producer: KafkaProducer,
    loop,
    process_executor: ProcessPoolExecutor,
) -> None:
    """Periodically retry pending correlations."""
    while True:
        await asyncio.sleep(RETRY_INTERVAL)

        if not pending_correlations:
            continue

        print(f"[Retry] Checking {len(pending_correlations)} pending correlations")

        processed_indices = []
        current_time = loop.time()

        for i, pending in enumerate(pending_correlations):
            # Remove expired correlations
            if (current_time - pending.timestamp) > (REDIS_TTL - 60):
                print(f"[Retry] Removing expired correlation: {pending.sweeps_ids}")
                processed_indices.append(i)
                continue

            # Try processing
            if await process_correlation_pair(
                pending.sweeps_ids,
                pending.corr_data,
                redis,
                producer,
                loop,
                process_executor,
            ):
                print(
                    f"[Retry] Success on retry #{pending.retry_count}: {pending.sweeps_ids}"
                )
                processed_indices.append(i)
            else:
                pending.retry_count += 1
                if pending.retry_count % 5 == 0:
                    print(
                        f"[Retry] Still waiting: {pending.sweeps_ids} (retry #{pending.retry_count})"
                    )

        # Remove processed correlations
        for i in reversed(processed_indices):
            pending_correlations.pop(i)


async def run_conversion_service() -> None:
    """Main conversion service."""
    print("=== Starting Conversion Service ===")

    # Initialize queues and data structures
    raw_queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
    corr_queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
    pending_correlations: list[PendingCorrelation] = []

    loop = asyncio.get_running_loop()
    process_executor = ProcessPoolExecutor(max_workers=os.cpu_count())

    # Setup Kafka
    raw_consumer = get_consumer(
        TOPICS_IN_RAW, BOOTSTRAP_SERVERS, "conversion-service-group"
    )
    corr_consumer = get_consumer(
        TOPICS_IN_CORR, BOOTSTRAP_SERVERS, "conversion-service-group"
    )
    producer = get_producer(bootstrap_servers=BOOTSTRAP_SERVERS)
    update_topic_partition(topic=TOPICS_OUT, partition=50, replication_factor=2)

    # Setup Redis
    redis = await aioredis.from_url(REDIS_URL, decode_responses=False)

    # Start consumers
    await consume_to_queue_in_thread(raw_consumer, raw_queue, TOPICS_IN_RAW, loop)
    await consume_to_queue_in_thread(corr_consumer, corr_queue, TOPICS_IN_CORR, loop)
    print("Consumers started")

    # Start retry task
    retry_task = asyncio.create_task(
        periodic_retry_correlations(
            pending_correlations, redis, producer, loop, process_executor
        )
    )

    try:
        while True:
            # Check both queues with timeout
            try:
                # Try to get from raw queue first (non-blocking)
                try:
                    raw_item = raw_queue.get_nowait()
                    sweeps_ids, arr, topic = raw_item
                    print(f"[Raw Queue] Processing: {sweeps_ids}")

                    # Store raw data
                    for sweep_id in sweeps_ids:
                        await store_raw(redis, sweep_id, arr)

                    # Check if any pending correlations can now be processed
                    processed_indices = []
                    for i, pending in enumerate(pending_correlations):
                        if any(
                            sweep_id in pending.sweeps_ids for sweep_id in sweeps_ids
                        ):
                            print(
                                f"[Raw] Potential match for correlation {pending.sweeps_ids}"
                            )
                            if await process_correlation_pair(
                                pending.sweeps_ids,
                                pending.corr_data,
                                redis,
                                producer,
                                loop,
                                process_executor,
                            ):
                                processed_indices.append(i)

                    # Remove processed correlations
                    for i in reversed(processed_indices):
                        pending_correlations.pop(i)

                except asyncio.QueueEmpty:
                    pass

                # Try to get from correlation queue
                try:
                    corr_item = corr_queue.get_nowait()
                    sweeps_ids, arr, topic = corr_item
                    print(f"[Corr Queue] Processing: {sweeps_ids}")

                    # Try processing
                    if await process_correlation_pair(
                        sweeps_ids, arr, redis, producer, loop, process_executor
                    ):
                        print(f"[Correlation] Immediate success: {sweeps_ids}")
                    else:
                        pending = PendingCorrelation(
                            sweeps_ids=sweeps_ids, corr_data=arr, timestamp=loop.time()
                        )
                        pending_correlations.append(pending)
                        print(f"[Correlation] Added to pending: {sweeps_ids}")

                except asyncio.QueueEmpty:
                    pass

                await asyncio.sleep(0.01)

            except Exception as e:
                print(f"[Main Loop] Error: {e}")
                await asyncio.sleep(1)

    except KeyboardInterrupt:
        print("\n=== Shutting Down ===")
    finally:
        print("Cleaning up...")
        retry_task.cancel()
        try:
            await retry_task
        except asyncio.CancelledError:
            pass
        try:
            producer.flush(timeout=5)
            producer.close()
        except Exception:
            pass
        try:
            process_executor.shutdown(wait=True, timeout=10)
        except Exception:
            pass
        try:
            raw_consumer.close()
            corr_consumer.close()
        except Exception:
            pass
        await redis.aclose()
        gc.collect()
        print("Conversion service stopped")


if __name__ == "__main__":
    try:
        asyncio.run(run_conversion_service())
    except KeyboardInterrupt:
        print("\nService interrupted")
