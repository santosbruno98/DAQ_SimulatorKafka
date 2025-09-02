"""Script to connect to Kafka server and get producer and consumer topics."""

import numpy as np
import pickle
import zlib
from kafka import KafkaProducer, KafkaConsumer

BOOTSTRAP_SERVERS: list[str] = ["kafka1:9092", "kafka2:9093"]
TOPICS: dict[str, str] = {
    "raw-electrical": "raw-electrical-data",
    "correlation": "correlation-data",
    "conversion": "conversion-data",
}


# C:\Users\santo\Documents\DevOps\DAQ_SIMULATION\
    # test-serverless-upload-s3\FastAPI-boilerplate\src\app\core\utils\kafka.py
def get_producer(bootstrap_servers: list[str] = BOOTSTRAP_SERVERS) -> KafkaProducer:
    """
    Create a Kafka producer connected to the cluster.

    Args:
        bootstrap_servers (tuple[str, str]): List of broker addresses (host:port).

    Returns:
        KafkaProducer: Configured producer instance.
    """
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        acks="all",
        retries=3,
        linger_ms=5,
        max_request_size=50 * 1024 * 1024,  # 50MB
        value_serializer=lambda v: (
            v if isinstance(v, bytes) else str(v).encode("utf-8")
        ),
    )
    return producer


def get_consumer(
    topic: str,
    bootstrap_servers: list[str] = BOOTSTRAP_SERVERS,
    group_id: str = "my-group",
    auto_offset_reset: str = "earliest",
    enable_auto_commit: bool = True,
) -> KafkaConsumer:
    """
    Create a Kafka consumer connected to the cluster.

    Args:
        topic (str): Topic name to subscribe to.
        bootstrap_servers (Tuple[str, str]): List of broker addresses.
        group_id (str): Consumer group ID.
        auto_offset_reset (str): What to do when there is no initial offset
                                 ("earliest" or "latest").
        enable_auto_commit (bool): Whether to auto commit offsets.

    Returns:
        KafkaConsumer: Configured consumer instance.
    """
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,  # start from earliest if no offset exists
        fetch_max_bytes=50 * 1024 * 1024,  # 50MB
        enable_auto_commit=enable_auto_commit,
        value_deserializer=lambda v: v,
    )
    return consumer


def serialize_array(
    arr: np.ndarray,
) -> bytes:  # for kafka, we need to put the waveform into a byte string
    """
    Serialize a NumPy array to a compressed byte string.

    Args:
        arr (np.ndarray): NumPy array to serialize.

    Returns:
        bytes: Compressed serialized byte string.
    """
    raw: bytes = pickle.dumps(arr)
    compressed: bytes = zlib.compress(raw)
    print(f"Serialized array of shape {arr.shape} to {len(compressed)} bytes")
    return compressed
