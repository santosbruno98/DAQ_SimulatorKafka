"""
Consumer 1 (Correlation Service), subscribes to 'raw-electrial-data ' topic,
publishes results to 'correlation-data' topic.
"""

import asyncio
import os
import pickle
import zlib
from concurrent.futures import ProcessPoolExecutor

import numpy as np
from bson import ObjectId
from dotenv import load_dotenv

from app.core.db.mongo_db import MongoDB
from app.core.utils.kafka_helper import (
    BOOTSTRAP_SERVERS,
    TOPICS,
    get_consumer,
    get_producer,
    serialize_array,
    update_topic_partition,
)
from app.core.utils.maths import (
    frequency_axis_laser,
    moving_correlation_with_peak_finding,
    moving_cumulative_calculation,
)

TOPICS_IN: str = TOPICS["raw-electrical"]
TOPICS_OUT: str = TOPICS["correlation"]
load_dotenv()

LASER_METADATA_ID = os.getenv("LASER_METADATA_ID")
DB_NAME = os.getenv("DB_NAME")


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
    db = MongoDB(DB_NAME)
    loop = asyncio.get_running_loop()
    process_executor = ProcessPoolExecutor(max_workers=os.cpu_count())

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
            msg_type: str | None = None
            sweeps_id: str | None = None
            if msg.headers:
                for key, value in msg.headers:
                    if key == "type":
                        msg_type = value.decode()
                    if key == "sweeps_id":
                        sweeps_id = value.decode()

            if msg_type != "raw":
                continue
            moving_cumulative_last: int = 0
            ti: int = 0
            tf: int = 0
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
            raw_data = np.expand_dims(raw_data, axis=1)

            if accumulated_data is None:
                accumulated_data = raw_data  # first message
                print("--- Accumulated first chunk ---", accumulated_data.shape)
                continue  # wait for next chunk
            tf += np.array(accumulated_data).shape[1]
            accumulated_data = np.concatenate([accumulated_data, raw_data], axis=1)
            size = tf - ti

            print("--- Accumulated chunks ---", accumulated_data.shape)

            (
                sweeps_mode,
                sweeps_mode_initial,
                sweeps_mode_final,
                sweeps_mode_step,
                current_frequency_step_a,
                current_frequency_step_b,
                temperature_frequency_step,
                fiber_t_initial_point,
                fiber_t_final_point,
                fiber_rh_initial_point,
                fiber_rh_final_point,
            ) = await db.get_acquisition_characteristics(
                laser_metadata_id=ObjectId(LASER_METADATA_ID)
            )
            frequency_axis = await asyncio.to_thread(
                frequency_axis_laser,
                sweeps_mode,
                sweeps_mode_initial,
                sweeps_mode_final,
                sweeps_mode_step,
                current_frequency_step_a,
                current_frequency_step_b,
                temperature_frequency_step,
            )

            moving_cumulative: np.ndarray = np.empty(
                shape=(1, size, accumulated_data.shape[2]),
                dtype=np.float32,
            )
            moving_data: np.ndarray = np.empty(
                (2, size, accumulated_data.shape[2]),
                dtype=np.float32,
            )

            moving_frequency_shift_peaks: np.ndarray = (
                moving_correlation_with_peak_finding(
                    data=accumulated_data,
                    frequency_axis=frequency_axis,
                    smooth_window=5,
                )
            )

            moving_cumulative = await loop.run_in_executor(
                process_executor,
                moving_cumulative_calculation,
                moving_frequency_shift_peaks,
                moving_cumulative,
                moving_cumulative_last,
                fiber_t_initial_point,
                fiber_t_final_point,
                fiber_rh_initial_point,
                fiber_rh_final_point,
                1,
            )
            moving_cumulative_last: np.ndarray = moving_cumulative[
                :, moving_cumulative.shape[1] - 1, :
            ]

            moving_data[0, :, :]: np.ndarray = moving_cumulative
            moving_data[1, :, :]: np.ndarray = moving_frequency_shift_peaks

            print(
                "Correlation Service [Sweeps_id]:",
                sweeps_id,
            )
            # serialize & send
            correlation_bytes: bytes = await asyncio.to_thread(
                serialize_array, moving_data
            )
            del moving_data
            update_topic_partition(topic=TOPICS_OUT, partition=50, replication_factor=2)
            future = producer.send(
                TOPICS_OUT,
                value=correlation_bytes,
                headers=[
                    ("second_sweeps_id", sweeps_id.encode()),
                ],
            )
            try:
                record_metadata = future.get(timeout=10)
                print(
                    f"Message sent to topic {record_metadata.topic}, partition {record_metadata.partition}, "
                )
                print(f"offset {record_metadata.offset}")
            except Exception as e:
                print(f"Failed to send correlation data: {e}")

        except Exception as e:
            print(f"Error processing message: {e}")
            import traceback

            traceback.print_exc()


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("Correlation service stopped.")
