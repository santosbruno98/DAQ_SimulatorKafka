"""
Writer service
Consumes `raw-electrical-data`, `correlation-data`
and `conversion-data` topics, writes to MongoDB, bucket S3.
"""

import asyncio
import os
import pickle
import tempfile
import threading
import traceback
import zlib
from asyncio import Queue
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

import aiofiles
import aiohttp
import numpy as np
from bson import ObjectId
from dotenv import load_dotenv
from kafka import KafkaConsumer

from app.core.db.mongo_db import MongoDB
from app.core.utils.kafka_helper import BOOTSTRAP_SERVERS, TOPICS, get_consumer

load_dotenv()
DB_NAME = os.getenv("DB_NAME")
BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
LASER_METADATA_ID = os.getenv("LASER_METADATA_ID")

# Global upload queue and executor for background uploads
upload_executor = ThreadPoolExecutor(max_workers=3, thread_name_prefix="s3-upload")


# -------------------------
# Deserialize helper
# -------------------------
def deserialize_array(data: bytes):
    """Decompress and unpickle bytes to numpy array."""
    decompressed = zlib.decompress(data)
    return pickle.loads(decompressed)


# -------------------------
# Blocking consumer in thread
# -------------------------
def _blocking_consumer_loop(
    consumer: KafkaConsumer,
    loop: asyncio.AbstractEventLoop,
    queue: Queue,
    topic_name: str,
):
    """Blocking loop that runs in a background thread, pushes messages to asyncio.Queue."""
    try:
        for msg in consumer:
            try:
                headers = {k: v.decode() for k, v in msg.headers} if msg.headers else {}
                arr = deserialize_array(msg.value)
                # Put tuple (arr, headers) so writer_task knows what to do
                future = asyncio.run_coroutine_threadsafe(
                    queue.put((arr, headers)), loop
                )
                future.result(timeout=5.0)
            except Exception as e:
                print(f"Error consuming {topic_name} message: {e}")
    except Exception as e:
        print(f"Consumer loop for {topic_name} exited with error: {e}")
    finally:
        try:
            consumer.close()
        except Exception:
            pass


async def consume_to_queue_in_thread(
    consumer: KafkaConsumer, queue: Queue, topic_name: str
):
    """Start a background thread that consumes from Kafka and pushes messages into asyncio.Queue."""
    loop = asyncio.get_running_loop()
    thread = threading.Thread(
        target=_blocking_consumer_loop,
        args=(consumer, loop, queue, topic_name),
        daemon=True,
    )
    thread.start()
    print(f"--- Started consumer thread for {topic_name} ---")


# -------------------------
# Fast S3 upload functions
# -------------------------
def _create_file_sync(
    laser_metadata_id: ObjectId, sweeps_id: ObjectId, data: np.ndarray
) -> tuple[str, str]:
    """Synchronously create the file (fast operation) - runs in thread pool."""
    timestamp = int(ObjectId(sweeps_id).generation_time.timestamp())
    local_tz = datetime.now().astimezone().tzinfo
    formatted_time = (
        datetime.fromtimestamp(timestamp, tz=UTC)
        .astimezone(local_tz)
        .strftime("%d-%m-%Y_%H-%M-%S")
    )

    folder = "sim-data/acquisitions"
    filename = f"{folder}/raw_data_{formatted_time}_{str(sweeps_id)}.json"

    temp_dir = tempfile.gettempdir()
    full_dir_path = os.path.join(temp_dir, folder)
    os.makedirs(full_dir_path, exist_ok=True)

    file_path = os.path.join(temp_dir, filename)

    # Prepare JSON-compatible data
    acquisition_document = {
        "laser_metadata_id": str(laser_metadata_id),
        "sweeps_id": str(sweeps_id),
        "data": data.tolist(),
    }
    serialized_doc = pickle.dumps(acquisition_document)

    with open(file_path, "wb") as f:
        f.write(serialized_doc)

    print(f"Created file: {filename} ({len(serialized_doc)} bytes)")
    return file_path, filename


async def _upload_file_async(file_path: str, filename: str) -> None:
    """Asynchronously upload file to S3 via API call."""
    try:
        async with aiohttp.ClientSession() as session:
            url = f"http://localhost:8000/api/aws/{BUCKET_NAME}?file_path={filename}"

            async with aiofiles.open(file_path, "rb") as f:
                file_data = await f.read()

            data = aiohttp.FormData()
            data.add_field("file", file_data, filename=os.path.basename(filename))

            async with session.post(url, data=data, timeout=300) as response:
                text = await response.text()
                if response.status in (200, 201):
                    result = await response.json()
                    print(f"Successfully uploaded {filename} to S3: {result}")
                else:
                    print(
                        f"Failed to upload {filename}: HTTP {response.status} : {text}"
                    )
    except Exception as e:
        print(f"Error uploading {filename} to S3: {e}")
        import traceback

        traceback.print_exc()


async def upload_json_fast(
    laser_metadata_id: ObjectId, sweeps_id: ObjectId, data: np.ndarray
) -> None:
    """Fast file creation + background S3 upload."""
    loop = asyncio.get_running_loop()
    file_path, filename = await loop.run_in_executor(
        upload_executor, _create_file_sync, laser_metadata_id, sweeps_id, data
    )
    asyncio.create_task(_upload_file_async(file_path, filename))
    print(f"Scheduled S3 upload for {filename}")


# -------------------------
# Async tasks writing to DB and S3
# -------------------------
async def writer_task(queue: Queue, mongo_db: MongoDB, topic_name: str):
    """Async task: consume from queue and write to MongoDB/S3."""
    print(f"--- Started writer task for {topic_name} ---")
    while True:
        try:
            data, headers = await queue.get()

            if topic_name == "Raw":
                sweeps_id = ObjectId(headers["sweeps_id"])
                if headers["type"] == "raw":
                    await upload_json_fast(ObjectId(LASER_METADATA_ID), sweeps_id, data)
                elif headers["type"] == "elec":
                    await mongo_db.insert_elec_data(
                        sweeps_id=sweeps_id,
                        data=data,
                        laser_metadata_id=ObjectId(LASER_METADATA_ID),
                        electrical_data=data,
                    )

            elif topic_name == "Correlation":
                sweeps_id = ObjectId(headers["second_sweeps_id"])
                await mongo_db.insert_correlation_data(sweeps_id=sweeps_id, data=data)

            elif topic_name == "Conversion":
                sweeps_id = ObjectId(headers["sweeps_id"])
                await mongo_db.insert_conversion_data(sweeps_id=sweeps_id, data=data)

            print(f"--- {topic_name} data processed for sweeps_id={headers} ---")

        except Exception as e:
            print(f"Error processing {topic_name} data: {e}")


# -------------------------
# Main service
# -------------------------
async def run_writer_service():
    """Main writer service coroutine."""
    mongo_db = MongoDB(db_name=DB_NAME)

    raw_queue = Queue()
    corr_queue = Queue()
    conv_queue = Queue()

    print("--- Writer Service Starting ---")

    raw_consumer = get_consumer(
        TOPICS["raw-electrical"], BOOTSTRAP_SERVERS, "writer-service-group"
    )
    corr_consumer = get_consumer(
        TOPICS["correlation"], BOOTSTRAP_SERVERS, "writer-service-group"
    )
    conv_consumer = get_consumer(
        TOPICS["conversion"], BOOTSTRAP_SERVERS, "writer-service-group"
    )

    await consume_to_queue_in_thread(raw_consumer, raw_queue, "Raw")
    await consume_to_queue_in_thread(corr_consumer, corr_queue, "Correlation")
    await consume_to_queue_in_thread(conv_consumer, conv_queue, "Conversion")

    print("--- Writer Service Started ---")

    try:
        await asyncio.gather(
            writer_task(raw_queue, mongo_db, "Raw"),
            writer_task(corr_queue, mongo_db, "Correlation"),
            writer_task(conv_queue, mongo_db, "Conversion"),
        )
    except KeyboardInterrupt:
        print("--- Writer service stopping ---")
    except Exception as e:
        print(f"Fatal error in writer service: {e}")
        traceback.print_exc()
    finally:
        upload_executor.shutdown(wait=False)
        print("--- Writer service cleanup complete ---")


if __name__ == "__main__":
    try:
        asyncio.run(run_writer_service())
    except KeyboardInterrupt:
        print("Writer service stopped.")
