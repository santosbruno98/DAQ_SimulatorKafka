"""Gets get_producer from app.core.utils.kafka and uses it to create a Kafka producer instance."""

from __future__ import annotations

# get data from the the daq_simulation script -> it turns a json file into a stream of data
#   later is suppose to be replaced by the actual waveform comming from the real daq
import asyncio

from app.core.utils.kafka_helper import (
    BOOTSTRAP_SERVERS,
    TOPICS,
    get_producer,
    serialize_array,
    update_topic_partition,
)

from .produce_daq_data import stream_raw_data


async def produce_raw() -> None:
    """
    Consume from stream_floats generator and produce messages to Kafka.

    Uses async iteration and sends serialized arrays to the configured topic.
    """
    update_topic_partition(topic=TOPICS["raw-electrical"], partition= 8, replication_factor= 2)
    
    
    producer = get_producer(bootstrap_servers=BOOTSTRAP_SERVERS)
    async for data in stream_raw_data():
        try:
            print(type(data), "Data received from DAQ stream")
            serialized_data: bytes = serialize_array(data)
            print(type(serialized_data), "Data serialized for Kafka")
            print("\n--Sending to Kafka topic:", TOPICS["raw-electrical"])
            print()
            future = producer.send(TOPICS["raw-electrical"], value=serialized_data)
            try:
                record_metadata = future.get(timeout=10)
                print(
                    f"Message sent to topic {record_metadata.topic},"
                )
                print(
                    f"partition {record_metadata.partition}, offset {record_metadata.offset}"
                )
            except Exception as e:
                print(f"Failed to send message: {e}")

            await asyncio.sleep(0.1)
        except Exception as e:
            print(f"Error producing data: {e}")


if __name__ == "__main__":
    try:
        asyncio.run(produce_raw())  # run the produce_data coroutine
    except KeyboardInterrupt:
        print("Kafka producer interrupted and shutting down.")
