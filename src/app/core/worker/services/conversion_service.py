"""
Conversion service

Consumes `raw-electrical-data` and `correlation-data` topics, performs
temperature/humidity calculation and publishes results to `conversion-data`.
"""

from __future__ import annotations
import asyncio
import gc
import os
import pickle
import threading
import zlib
from concurrent.futures import ProcessPoolExecutor
from typing import Any, Dict, List, Tuple

import numpy as np
from kafka import KafkaConsumer, KafkaProducer
from asyncio import Queue

# Adjust import path to your helper module location
from app.core.utils.kafka_helper import (
    get_consumer,
    get_producer,
    serialize_array,
    TOPICS,
    BOOTSTRAP_SERVERS,
)

# Topic names (from your helper TOPICS dict)
TOPICS_IN_RAW: str = TOPICS["raw-electrical"]
TOPICS_IN_CORR: str = TOPICS["correlation"]
TOPICS_OUT: str = TOPICS["conversion"]


# -------------------------
# Utility / Domain methods
# -------------------------


def points_fibers(
    min_temperature: int,
    max_temperature: int,
    min_humidity: int,
    max_humidity: int,
    x_average: int,
    check_reversed: bool,
    points_sensor: Dict[int, Tuple[int, int]],
) -> Tuple[List[int], List[int], List[int], List[int]]:
    """
    Compute grid points for temperature / humidity sampling and map sensors to points.

    Returns:
        (points_temperature, points_humidity, temperature_sensor_per_point, humidity_sensor_per_point)
    """
    step = 2 * x_average if x_average != 0 else 1

    if check_reversed and min_humidity != min_temperature:
        if min_humidity < min_temperature:
            points_humidity = list(
                range(min_humidity + x_average, max_humidity - x_average + 1, step)
            )
            points_temperature = list(
                reversed(
                    range(
                        min_temperature + (max_humidity - points_humidity[-1]),
                        max_temperature - x_average + 1,
                        step,
                    )
                )
            )
            humidity_sensor_per_point = list(
                map(
                    lambda point_humidity: list(
                        filter(
                            lambda sensor_index: points_sensor[sensor_index][0]
                            <= point_humidity
                            <= points_sensor[sensor_index][1],
                            list(points_sensor.keys()),
                        )
                    )[0],
                    points_humidity,
                )
            )
            temperature_sensor_per_point = humidity_sensor_per_point
        else:  # min_temperature < min_humidity
            points_temperature = list(
                range(min_temperature + x_average, max_temperature - x_average + 1, step)
            )
            points_humidity = list(
                reversed(
                    range(
                        min_humidity + (max_temperature - points_temperature[-1]),
                        max_humidity - x_average + 1,
                        step,
                    )
                )
            )
            temperature_sensor_per_point = list(
                map(
                    lambda point_temperature: list(
                        filter(
                            lambda sensor_index: points_sensor[sensor_index][0]
                            <= point_temperature
                            <= points_sensor[sensor_index][1],
                            list(points_sensor.keys()),
                        )
                    )[0],
                    points_temperature,
                )
            )
            humidity_sensor_per_point = temperature_sensor_per_point
    else:
        points_temperature = list(
            range(min_temperature + x_average, max_temperature - x_average + 1, step)
        )
        points_humidity = list(
            range(
                min_humidity + (max_temperature - points_temperature[-1]),
                max_humidity - x_average + 1,
                step,
            )
        )
        temperature_sensor_per_point = list(
            map(
                lambda point_temperature: list(
                    filter(
                        lambda sensor_index: points_sensor[sensor_index][0]
                        <= point_temperature
                        <= points_sensor[sensor_index][1],
                        list(points_sensor.keys()),
                    )
                )[0],
                points_temperature,
            )
        )
        humidity_sensor_per_point = list(
            map(
                lambda point_humidity: list(
                    filter(
                        lambda sensor_index: points_sensor[sensor_index][0]
                        <= point_humidity
                        <= points_sensor[sensor_index][1],
                        list(points_sensor.keys()),
                    )
                )[0],
                points_humidity,
            )
        )

    return (
        points_temperature,
        points_humidity,
        temperature_sensor_per_point,
        humidity_sensor_per_point,
    )


def temperature_humidity_calculation(
    data: np.ndarray,
    electrical_data: np.ndarray,
    distance_average: int,
    slope_temperature: float,
    slope_humidity: float,
    slope_temperature_fiber_rh: float,
    points_temperature: List[int],
    points_humidity: List[int],
    temperature_sensor_per_point: List[int],
    humidity_sensor_per_point: List[int],
) -> np.ndarray:
    """
    Temperature and humidity calculation based on correlation `data` and `electrical_data`.

    Args:
        data: correlation data shape (n_sweeps?, n_timestamps?, n_distance_points?) or similar
        electrical_data: electrical array with shape (2, n_distance_points) (two channels)
        distance_average: window half-width for averaging
        slope_temperature: calibration slope for temperature
        slope_humidity: calibration slope for humidity
        slope_temperature_fiber_rh: cross-term slope
        points_temperature/humidity: indices to evaluate
        temperature_sensor_per_point/humidity_sensor_per_point: mapping

    Returns:
        fibers_data: np.ndarray shape (2, electrical_channels, n_points)
    """
    # compute frequency shifts
    if distance_average != 0:
        frequency_shift_temperature = np.transpose(
            np.array(
                list(
                    map(
                        lambda point_temperature: np.average(
                            data[
                                0,
                                :,
                                point_temperature
                                - distance_average : point_temperature
                                + distance_average
                                + 1,
                            ],
                            axis=1,
                        ),
                        points_temperature,
                    )
                )
            )
        )
        frequency_shift_humidity = np.transpose(
            np.array(
                list(
                    map(
                        lambda point_humidity: np.average(
                            data[
                                0,
                                :,
                                point_humidity
                                - distance_average : point_humidity
                                + distance_average
                                + 1,
                            ],
                            axis=1,
                        ),
                        points_humidity,
                    )
                )
            )
        )
    else:
        frequency_shift_temperature = np.transpose(
            np.array(
                list(
                    map(
                        lambda point_temperature: data[0, :, point_temperature],
                        points_temperature,
                    )
                )
            )
        )
        frequency_shift_humidity = np.transpose(
            np.array(
                list(
                    map(
                        lambda point_humidity: data[0, :, point_humidity],
                        points_humidity,
                    )
                )
            )
        )
    temperature_fiber = np.transpose(
        np.array(
            list(
                map(
                    lambda point: (
                        frequency_shift_temperature[:, point]
                        / slope_temperature
                    )
                    + electrical_data[
                        0, int(temperature_sensor_per_point[point])
                    ],
                    range(frequency_shift_temperature.shape[1]),
                )
            )
        )
    )

    humidity_fiber = np.transpose(
        np.array(
            list(
                map(
                    lambda point: (
                        (
                            frequency_shift_humidity[:, point]
                            - (
                                (slope_temperature_fiber_rh / slope_temperature)
                                * frequency_shift_temperature[:, point]
                            )
                        )
                        / slope_humidity
                    )
                    + electrical_data[1, int(humidity_sensor_per_point[point])],
                    range(frequency_shift_humidity.shape[1]),
                )
            )
        )
    )
    fibers_data = np.empty(
        (2, humidity_fiber.shape[0], humidity_fiber.shape[1]), dtype=np.float32
    )
    fibers_data[0, :, :] = temperature_fiber
    fibers_data[1, :, :] = humidity_fiber
    return fibers_data


def deserialize_array(data: bytes) -> np.ndarray:
    """
    Decompress and unpickle a bytes message into a NumPy array.
    """
    decompressed: bytes = zlib.decompress(data)
    arr: np.ndarray = pickle.loads(decompressed)
    return arr


# -------------------------
# Kafka consumption helpers
# -------------------------


def _blocking_consumer_loop(consumer: KafkaConsumer, put_coroutine, topic_name: str) -> None:
    """
    Blocking loop meant to run in a background thread. For each message,
    schedule `put_coroutine(msg_array)` on the asyncio loop.
    `put_coroutine` must be a callable that accepts a single argument (the array).
    """
    try:
        for msg in consumer:
            try:
                data_bytes: bytes = msg.value
                arr: np.ndarray = deserialize_array(data_bytes)
                # put_coroutine is a coroutine function; schedule it on the loop
                put_coroutine(arr)
            except Exception as e:
                print(f"Error consuming {topic_name} message: {e}")
    except Exception as e:
        print(f"Blocking consumer loop for {topic_name} exited with error: {e}")
    finally:
        # ensure consumer closed if thread ends
        try:
            consumer.close()
        except Exception:
            pass


async def consume_to_queue_in_thread(
    consumer: KafkaConsumer, queue: Queue, topic_name: str, loop: asyncio.AbstractEventLoop
) -> None:
    """
    Start a background thread that consumes from blocking KafkaConsumer and
    pushes messages into an asyncio.Queue.

    This function returns immediately after thread startup.
    """
    # Define how to schedule queue.put on the event loop from the thread
    def schedule_put(arr: np.ndarray) -> None:
        # asyncio.run_coroutine_threadsafe returns a concurrent.futures.Future
        asyncio.run_coroutine_threadsafe(queue.put(arr), loop)

    thread = threading.Thread(
        target=_blocking_consumer_loop,
        args=(consumer, schedule_put, topic_name),
        daemon=True,
    )
    thread.start()
    # We return immediately; the thread will keep running and pushing into queue


# -------------------------
# Main conversion service
# -------------------------


async def run_conversion_service() -> None:
    """
    Main conversion service coroutine.

    - Starts background consumer threads for raw and correlation topics.
    - Waits for a correlation message, then takes the most recent raw message,
      runs heavy CPU calculation in a ProcessPoolExecutor and publishes conversion result.
    """
    raw_queue: Queue = Queue()
    corr_queue: Queue = Queue()

    loop = asyncio.get_running_loop()
    process_executor: ProcessPoolExecutor = ProcessPoolExecutor(max_workers=max(1, os.cpu_count() - 1))

    raw_consumer: KafkaConsumer = get_consumer(
        topic=TOPICS_IN_RAW, bootstrap_servers=BOOTSTRAP_SERVERS, group_id="conversion-service-group"
    )
    corr_consumer: KafkaConsumer = get_consumer(
        topic=TOPICS_IN_CORR, bootstrap_servers=BOOTSTRAP_SERVERS, group_id="conversion-service-group"
    )
    producer: KafkaProducer = get_producer(bootstrap_servers=BOOTSTRAP_SERVERS)

    print("--- Conversion Service Started ---")

    # Start background consumer threads that feed asyncio queues
    await consume_to_queue_in_thread(raw_consumer, raw_queue, "Raw", loop)
    await consume_to_queue_in_thread(corr_consumer, corr_queue, "Correlation", loop)

    print("--- Consumer tasks started, waiting for data ---")

    try:
        while True:
            # Wait for correlation data first (it's the bottleneck)
            print("--- Waiting for correlation data ---")
            latest_corr_data: np.ndarray = await corr_queue.get()
            print(f"--- Got correlation data: {latest_corr_data.shape} ---")

            # Get the most recent raw data (drain old data if multiple are queued)
            latest_raw_data: np.ndarray | None = None
            try:
                # Get at least one raw data
                latest_raw_data = await asyncio.wait_for(raw_queue.get(), timeout=5.0)
                print(f"--- Got raw data: {latest_raw_data.shape} ---")

                # Drain queue to keep only the most recent raw data
                while not raw_queue.empty():
                    try:
                        old_raw_data = latest_raw_data
                        latest_raw_data = raw_queue.get_nowait()
                        print(f"--- Updated to newer raw data: {latest_raw_data.shape} ---")
                        del old_raw_data
                    except asyncio.QueueEmpty:
                        break

            except asyncio.TimeoutError:
                print("--- Timeout waiting for raw data, skipping this correlation data ---")
                continue

            try:
                # Compute points mapping (example values; keep them as in original)
                points_temperature, points_humidity, temperature_sensor_per_point, humidity_sensor_per_point = points_fibers(
                    min_temperature=1743,
                    max_temperature=1929,
                    min_humidity=1556,
                    max_humidity=1742,
                    x_average=0,
                    check_reversed=True,
                    points_sensor={0: (1000, 2000), 1: (2100, 3100)},
                )
                

                print("--- Starting temperature/humidity calculation ---")

                # Offload heavy calculation to a process pool
                conversion_placeholder: np.ndarray = await loop.run_in_executor(
                    process_executor,
                    temperature_humidity_calculation,
                    data=latest_corr_data,
                    electrical_data= latest_raw_data[0:2, :],
                    distance_average= 0,
                    slope_temperature=1.57,
                    slope_humidity=0.18,
                    slope_temperature_fiber_rh=1.39,
                    points_temperature=points_temperature,
                    points_humidity=points_humidity,
                    temperature_sensor_per_point=temperature_sensor_per_point,
                    humidity_sensor_per_point=humidity_sensor_per_point,
                )

                print(f"--- Calculation complete, result shape: {conversion_placeholder.shape} ---")

                # Serialize (blocking) in separate thread to avoid blocking loop
                conversion_bytes: bytes = await asyncio.to_thread(serialize_array, conversion_placeholder)

                # Send via producer (kafka-python send returns a Future)
                future = producer.send(TOPICS_OUT, value=conversion_bytes)
                try:
                    record_metadata = future.get(timeout=10)
                    print(f"Message sent to topic {record_metadata.topic}, partition {record_metadata.partition}, offset {record_metadata.offset}")
                except Exception as e:
                    print(f"Failed to send conversion data: {e}")

                print("----- Produced conversion data to Kafka -----\n")

                # Clean up
                del latest_raw_data, latest_corr_data, conversion_placeholder
                gc.collect()

            except Exception as e:
                print(f"Error processing data: {e}")
                import traceback
                traceback.print_exc()
                # Cleanup
                if "latest_raw_data" in locals():
                    del latest_raw_data
                if "latest_corr_data" in locals():
                    del latest_corr_data
                gc.collect()

    except KeyboardInterrupt:
        print("--- Conversion service stopping ---")
    except Exception as e:
        print(f"Fatal error in conversion service: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Shutdown producer and executor and attempt to close consumers
        try:
            producer.flush()
            producer.close()
        except Exception:
            pass

        try:
            process_executor.shutdown(wait=False)
        except Exception:
            pass

        # Consumers are closed inside the blocking threads when they exit;
        # if they are still open, try to close them:
        try:
            raw_consumer.close()
        except Exception:
            pass
        try:
            corr_consumer.close()
        except Exception:
            pass

        gc.collect()


if __name__ == "__main__":
    try:
        asyncio.run(run_conversion_service())
    except KeyboardInterrupt:
        print("Conversion service stopped.")
