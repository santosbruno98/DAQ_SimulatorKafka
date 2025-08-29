'''
Consumer 1 (Correlation Service), subscribes to 'raw-electrial-data ' topic,
publishes results to 'correlation-data' topic.
'''

from app.core.utils.kafka_helper import get_consumer, get_producer, serialize_array, TOPICS, BOOTSTRAP_SERVERS
import numpy as np
import asyncio
import pickle
import zlib
from typing import List

TOPICS_IN: str = TOPICS['raw-electrical']
TOPICS_OUT: str = TOPICS['correlation']

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
        group_id='correlation-service-group',
    )
    print('--- Correlation Service Started, listening to raw-electrical-data topic ---', type(consumer))
    
    producer = get_producer(bootstrap_servers=BOOTSTRAP_SERVERS)
    print('--- Kafka Producer for Correlation Service Created ---', type(producer))
    print()
    
    for msg in consumer:
        try:
            raw_data_bytes: bytes = msg.value
            raw_data: np.ndarray = await asyncio.to_thread(deserialize_array, raw_data_bytes)
            print('--- Received raw data ---', raw_data.shape)  # (502,5000)
            
            # add time dimension -> (502, 1, 5000)
            raw_data = np.expand_dims(raw_data, axis=1)
            
            if accumulated_data is None:
                accumulated_data = raw_data  # first message
                print('--- Accumulated first chunk ---', accumulated_data.shape)
                continue  # wait for next chunk
            
            # stack along time axis -> (502,2,5000)
            accumulated_data = np.concatenate([accumulated_data, raw_data], axis=1)
            print('--- Accumulated two chunks ---', accumulated_data.shape)
            
            # now accumulated_data.shape == (502,2,5000) -> ready for correlation
            # here you can compute correlation per row/point along time if needed
            # for example, simple placeholder:
            correlation_placeholder = accumulated_data  # shape (502,2,5000)
            
            # serialize & send
            correlation_bytes: bytes = await asyncio.to_thread(serialize_array, correlation_placeholder)
            future = producer.send(TOPICS_OUT, correlation_bytes)
            try:
                record_metadata = future.get(timeout=10)
                print(f"Message sent to topic {record_metadata.topic}, partition {record_metadata.partition}, offset {record_metadata.offset}")
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