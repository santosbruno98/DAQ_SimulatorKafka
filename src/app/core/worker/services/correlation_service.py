"""
Consumer 1 (Correlation Service), subscribes to 'raw-electrial-data ' topic,
publishes results to 'correlation-data' topic.
"""

import asyncio
import pickle
import zlib

import numpy as np

from app.core.utils.kafka_helper import (
    BOOTSTRAP_SERVERS,
    TOPICS,
    get_consumer,
    get_producer,
    serialize_array,
    update_topic_partition,
)

TOPICS_IN: str = TOPICS["raw-electrical"]
TOPICS_OUT: str = TOPICS["correlation"]


def deserialize_array(data: bytes) -> np.ndarray:
    """
    Deserialize bytes back to numpy array (reverse of serialize_array from kafka_helper.py)
    This matches the serialize_array function in kafka_helper.py
    """
    decompressed: bytes = zlib.decompress(data)
    arr: np.ndarray = pickle.loads(decompressed)
    return arr


accumulated_data: np.ndarray | None = None  # global or enclosing variable


async def run() -> None:
    global accumulated_data
    
    consumer = get_consumer(
        topic=TOPICS_IN,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="correlation-service-group",
    )
    print(
        "--- Correlation Service Started, listening to raw-electrical-data topic ---",
        type(consumer),
    )

    producer = get_producer(bootstrap_servers=BOOTSTRAP_SERVERS)
    print("--- Kafka Producer for Correlation Service Created ---", type(producer))
    print()

    for msg in consumer:
        try:
            raw_data_bytes: bytes = msg.value
            raw_data: np.ndarray = await asyncio.to_thread(
                deserialize_array, raw_data_bytes
            )
            if msg.headers:
                for k, v in msg.headers:
                    if k == "sweeps_id":
                        sweeps_id = v.decode()
                        break
            
            print("--- Received raw data ---", raw_data.shape, sweeps_id)  # (502,5000)

            # add time dimension -> (502, 1, 5000)
            raw_data = np.expand_dims(raw_data, axis=1)

            if accumulated_data is None:
                accumulated_data = raw_data  # first message
                first_sweeps_id = sweeps_id
                print("--- Accumulated first chunk ---", accumulated_data.shape)
                continue  # wait for next chunk

            # stack along time axis -> (502,2,5000)
            accumulated_data = np.concatenate([accumulated_data, raw_data], axis=1)
            print("--- Accumulated two chunks ---", accumulated_data.shape)

            # now accumulated_data.shape == (502,2,5000) -> ready for correlation
            # here you can compute correlation per row/point along time if needed
            # for example, simple placeholder:
            correlation_placeholder = accumulated_data[0:2, :, :]  # shape (2,2,5000)

            print('Correlation Service [First_sweeps_id]: %s [Sweeps_id]:', first_sweeps_id,sweeps_id)
            # serialize & send
            correlation_bytes: bytes = await asyncio.to_thread(
                serialize_array, correlation_placeholder
            )
            update_topic_partition(topic=TOPICS_OUT, partition=8, replication_factor=2)
            future = producer.send(
                TOPICS_OUT,
                value = correlation_bytes,
                headers=[("first_sweeps_id", first_sweeps_id.encode()), ("second_sweeps_id", sweeps_id.encode())]
                )
            try:
                record_metadata = future.get(timeout=10)
                print(
                    f"Message sent to topic {record_metadata.topic}, partition {record_metadata.partition}, "
                )
                print(
                    f"offset {record_metadata.offset}"
                )
            except Exception as e:
                print(f"Failed to send correlation data: {e}")

            # reset accumulator for next pair
            accumulated_data = None

        except Exception as e:
            print(f"Error processing message: {e}")
            import traceback

            traceback.print_exc()


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("Correlation service stopped.")
