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
import time
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
upload_queue = Queue()
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
    """
    Blocking loop that runs in a background thread.
    Consumes from Kafka and schedules queue.put() on the asyncio loop.
    """
    try:
        for msg in consumer:
            try:
                arr = deserialize_array(msg.value)
                # Schedule the coroutine on the event loop from this thread
                future = asyncio.run_coroutine_threadsafe(queue.put(arr), loop)
                # Wait for the put operation to complete (optional, but safer)
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
    """
    Start a background thread that consumes from blocking KafkaConsumer and
    pushes messages into an asyncio.Queue.
    """
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
def _create_file_sync(sweeps_id: ObjectId, data: np.ndarray) -> tuple[str, str]:
    """
    Synchronously create the file (fast operation) - runs in thread pool.
    Returns (file_path, filename) tuple.
    """
    try:
        # Generate timestamp from ObjectId
        timestamp = int(ObjectId(sweeps_id).generation_time.timestamp())
        local_tz = datetime.now().astimezone().tzinfo
        formatted_time = (
            datetime.fromtimestamp(timestamp, tz=UTC)
            .astimezone(local_tz)
            .strftime("%d-%m-%Y_%H-%M-%S")  # Use underscores for filename safety
        )

        folder = "sim-data/acquisitions"
        filename = f"{folder}/raw_data_{formatted_time}_{str(sweeps_id)}.json"

        # Ensure local directories exist
        temp_dir = tempfile.gettempdir()
        full_dir_path = os.path.join(temp_dir, folder)
        os.makedirs(full_dir_path, exist_ok=True)  # <-- create folders if missing

        file_path = os.path.join(temp_dir, filename)

        # Prepare data efficiently
        compressed_waveform = zlib.compress(pickle.dumps(data))
        acquisition_document = {
            "sweeps_id": sweeps_id,
            "data": compressed_waveform,
            "timestamp": formatted_time,
            "shape": data.shape,
            "dtype": str(data.dtype),
        }

        serialized_doc = pickle.dumps(acquisition_document)

        with open(file_path, "wb") as f:
            f.write(serialized_doc)

        print(f"Created file: {filename} ({len(serialized_doc)} bytes)")
        return file_path, filename

    except Exception as e:
        print(f"Error creating file: {e}")
        raise


async def _upload_file_async(file_path: str, filename: str) -> None:
    """
    Asynchronously upload file to S3 via API call.
    """
    try:
        async with aiohttp.ClientSession() as session:
            url = f"http://localhost:8000/api/aws/{BUCKET_NAME}?file_path={filename}"

            # Use multipart form data for file upload
            async with aiofiles.open(file_path, "rb") as f:
                file_data = await f.read()

            data = aiohttp.FormData()
            data.add_field("file", file_data, filename=os.path.basename(filename))

            async with session.post(url, data=data, timeout=30) as response:
                text = await response.text()
                if response.status == 200 or response.status == 201:
                    result = await response.json()
                    print(f"Successfully uploaded {filename} to S3: {result}")
                else:
                    print(
                        f"Failed to upload {filename}: HTTP {response.status} : TEXT {text}"
                    )

    except Exception as e:
        print(f"Error uploading {filename} to S3: {e}")
    finally:
        # Clean up temp file after a day
        try:
            if os.path.isfile(file_path) and time.time() - os.path.getmtime(file_path) > (60 * 60 * 24):
                os.remove(file_path)
                print(f"Cleaned up temp file: {file_path}")
            pass
        except Exception as e:
            print(f"Error cleaning up temp file {file_path}: {e}")


async def upload_json_fast(sweeps_id: ObjectId, data: np.ndarray) -> None:
    """
    Fast file creation + background S3 upload.
    This function returns quickly after scheduling the upload.
    """
    try:
        # Create file in thread pool (fast, non-blocking)
        loop = asyncio.get_running_loop()
        file_path, filename = await loop.run_in_executor(
            upload_executor, _create_file_sync, sweeps_id, data
        )

        # Schedule upload in background (non-blocking)
        asyncio.create_task(_upload_file_async(file_path, filename))
        print(f"Scheduled S3 upload for {filename}")

    except Exception as e:
        print(f"Error in upload_json_fast: {e}")


# -------------------------
# Async tasks writing to DB and S3
# -------------------------
async def writer_task(queue: Queue, func, topic_name: str):
    """Async task: consume from queue and write to MongoDB/S3 immediately."""
    print(f"--- Started writer task for {topic_name} ---")
    while True:
        try:
            data = await queue.get()
            sweeps_id = ObjectId()  # generate new id for each message
            if func.__name__ == "insert_acquisition_data":
                await func(
                    sweeps_id=sweeps_id,
                    data=data,
                    laser_metadata_id = ObjectId(LASER_METADATA_ID),
                    electrical_data = data[0 : 2, 0 : 2]
                )
            else:
                await func(sweeps_id=sweeps_id, data=data)
            print(f"--- {topic_name} data processed --- {data.shape}")
        except Exception as e:
            print(f"Error processing {topic_name} data: {e}")


async def s3_writer_task(queue: Queue, topic_name: str):
    """Specialized writer task for S3 uploads."""
    print(f"--- Started S3 writer task for {topic_name} ---")
    while True:
        try:
            data = await queue.get()
            sweeps_id = ObjectId()
            # This returns quickly after scheduling the upload
            await upload_json_fast(sweeps_id, data)
            print(f"--- {topic_name} S3 upload scheduled --- {data.shape}")
        except Exception as e:
            print(f"Error scheduling S3 upload for {topic_name}: {e}")

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

    # Create Kafka consumers
    raw_consumer = get_consumer(
        TOPICS["raw-electrical"], BOOTSTRAP_SERVERS, "writer-service-group"
    )
    corr_consumer = get_consumer(
        TOPICS["correlation"], BOOTSTRAP_SERVERS, "writer-service-group"
    )
    conv_consumer = get_consumer(
        TOPICS["conversion"], BOOTSTRAP_SERVERS, "writer-service-group"
    )

    # Start background consumer threads
    await consume_to_queue_in_thread(raw_consumer, raw_queue, "Raw")
    await consume_to_queue_in_thread(corr_consumer, corr_queue, "Correlation")
    await consume_to_queue_in_thread(conv_consumer, conv_queue, "Conversion")

    print("--- Writer Service Started ---")

    try:
        # Start async tasks
        await asyncio.gather(
            s3_writer_task(raw_queue, "Raw-S3"),
            writer_task(raw_queue, mongo_db.insert_acquisition_data, "Raw-Mongo"),
            writer_task(corr_queue, mongo_db.insert_correlation_data, "Correlation"),
            writer_task(conv_queue, mongo_db.insert_conversion_data, "Conversion"),
        )
    except KeyboardInterrupt:
        print("--- Writer service stopping ---")
    except Exception as e:
        print(f"Fatal error in writer service: {e}")
        traceback.print_exc()
    finally:
        # Cleanup
        upload_executor.shutdown(wait=False)
        print("--- Writer service cleanup complete ---")


if __name__ == "__main__":
    try:
        asyncio.run(run_writer_service())
    except KeyboardInterrupt:
        print("Writer service stopped.")
