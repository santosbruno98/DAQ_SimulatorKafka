import asyncio
import numpy as np
import gc
import zlib
import pickle
from kafka import KafkaConsumer, KafkaProducer
from app.core.utils.kafka_helper import get_consumer, get_producer, serialize_array, TOPICS, BOOTSTRAP_SERVERS

TOPICS_IN_RAW: str = TOPICS['raw-electrical']
TOPICS_IN_CORR: str = TOPICS['correlation']
TOPICS_OUT: str = TOPICS['conversion']

def deserialize_array(data: bytes) -> np.ndarray:
    decompressed: bytes = zlib.decompress(data)
    arr: np.ndarray = pickle.loads(decompressed)
    return arr

async def consume_to_queue(consumer: KafkaConsumer, queue: asyncio.Queue, label: str):
    """Consume messages from Kafka and put them into an asyncio queue."""
    for msg in consumer:
        try:
            data_bytes: bytes = msg.value
            data: np.ndarray = await asyncio.to_thread(deserialize_array, data_bytes)
            print(f'--- {label} data received ---', data.shape)
            await queue.put(data)
        except Exception as e:
            print(f"Error consuming {label} message: {e}")

async def run_conversion_service():
    """Main conversion service coroutine."""
    raw_queue: asyncio.Queue = asyncio.Queue()
    corr_queue: asyncio.Queue = asyncio.Queue()

    raw_consumer: KafkaConsumer = get_consumer(
        topic=TOPICS_IN_RAW,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id='conversion-service-group'
    )
    corr_consumer: KafkaConsumer = get_consumer(
        topic=TOPICS_IN_CORR,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id='conversion-service-group'
    )
    producer: KafkaProducer = get_producer(bootstrap_servers=BOOTSTRAP_SERVERS)

    print('--- Conversion Service Started ---')

    # Start consuming in parallel
    raw_task = asyncio.create_task(consume_to_queue(raw_consumer, raw_queue, "Raw"))
    corr_task = asyncio.create_task(consume_to_queue(corr_consumer, corr_queue, "Correlation"))

    while True:
        # Wait until we have at least one item in both queues
        latest_raw_data = await raw_queue.get()
        latest_corr_data = await corr_queue.get()

        try:
            # Example conversion: simple addition (replace with your real logic)
            conversion_placeholder: np.ndarray = latest_raw_data + latest_corr_data
            conversion_bytes: bytes = await asyncio.to_thread(serialize_array, conversion_placeholder)

            future = producer.send(TOPICS_OUT, value=conversion_bytes)
            try:
                record_metadata = future.get(timeout=10)
                print(f"Message sent to topic {record_metadata.topic}, partition {record_metadata.partition}, offset {record_metadata.offset}")
            except Exception as e:
                print(f"Failed to send conversion data: {e}")

            print('----- Produced conversion data to Kafka -----\n', type(conversion_bytes))

            # Clean up
            del latest_raw_data, latest_corr_data
            gc.collect()

        except Exception as e:
            print(f"Error producing conversion data: {e}")
            del latest_raw_data, latest_corr_data
            gc.collect()

if __name__ == "__main__":
    try:
        asyncio.run(run_conversion_service())
    except KeyboardInterrupt:
        print("Conversion service stopped.")
