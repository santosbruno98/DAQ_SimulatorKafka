''' Gets get_producer from app.core.utils.kafka and uses it to create a Kafka producer instance.'''

from __future__ import annotations


from .produce_daq_data import stream_raw_data
from app.core.utils.kafka_helper import get_producer, serialize_array, TOPICS, BOOTSTRAP_SERVERS

# get data from the the daq_simulation script -> it turns a json file into a stream of data -> later is suppose to be replaced by the actual waveform comming from the real daq

import asyncio
import pickle
import zlib
from typing import List
import numpy as np

async def produce_raw() -> None:
    """
    Consume from stream_floats generator and produce messages to Kafka.

    Uses async iteration and sends serialized arrays to the configured topic.
    """
    producer = get_producer(bootstrap_servers=BOOTSTRAP_SERVERS)
    async for data in stream_raw_data():
        try:
            print(type(data), "Data received from DAQ stream")
            serialized_data: bytes = serialize_array(data)
            print(type(serialized_data), "Data serialized for Kafka")
            print('\n--Sending to Kafka topic:', TOPICS['raw-electrical'])
            print()
            future = producer.send(TOPICS['raw-electrical'],value=serialized_data)
            try:
                record_metadata = future.get(timeout=10)
                print(f"Message sent to topic {record_metadata.topic}, partition {record_metadata.partition}, offset {record_metadata.offset}")
            except Exception as e:
                print(f"Failed to send message: {e}")
            
            await asyncio.sleep(0.1)
        except Exception as e:
            print(f"Error producing data: {e}")

if __name__=="__main__":
    try:
        asyncio.run(produce_raw()) # run the produce_data coroutine
    except KeyboardInterrupt:
        print("Kafka producer interrupted and shutting down.")