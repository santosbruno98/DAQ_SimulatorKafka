"""Script to simulate streaming DAQ data by reading from a local JSON file."""

from __future__ import annotations

import asyncio
import base64
import json
import pickle
import zlib
from collections.abc import Iterator
from typing import Any

import numpy as np

BUCKET_NAME: str = "daqrawdata"
API_GATEWAY_URL: str = "https://plyb1o6d1j.execute-api.eu-west-3.amazonaws.com/dev/"
CHUNK_INTERVAL: float = 60  # seconds between uploads
SOURCE_FILE_1: str = "/code/data/acquisition_characteristics.json"
SOURCE_FILE_2: str = "/code/data/electrical_sensors.json"


def decompress_to_floats(b64_string: str) -> np.ndarray:
    """
    Decompress a base64-encoded string to a NumPy array of floats.

    Args:
        b64_string (str): Base64-encoded compressed pickled NumPy array.

    Returns:
        np.ndarray: Decompressed NumPy array of floats.

    Raises:
        TypeError: If the decompressed object is not a NumPy array.
    """
    raw: bytes = base64.b64decode(b64_string)
    arr: Any = pickle.loads(zlib.decompress(raw))
    if isinstance(arr, np.ndarray):
        return np.ascontiguousarray(arr, dtype=np.float64)
    else:
        raise TypeError(f"Unexpected decompressed type: {type(arr)}")


async def stream_raw_data() -> Iterator[np.ndarray, None]:
    """
    Async generator that streams decompressed float arrays from a JSON source file.

    Yields:
        np.ndarray: Decompressed NumPy array of floats for each JSON line.

    Notes:
        - Reads continuously in an infinite loop.
        - Uses asyncio.to_thread to offload blocking decompression.
        - Sleeps CHUNK_INTERVAL seconds between chunks.
    """
    while True:
        try:
            with (
                open(SOURCE_FILE_1, encoding="utf-8") as f1,
                open(SOURCE_FILE_2, encoding="utf-8") as f2,
            ):
                for line_num, (line1, line2) in enumerate(zip(f1, f2), 1):
                    if not line1.strip() or not line2.strip():
                        continue
                    try:
                        doc1 = json.loads(line1)
                        b64_data: str = doc1["data"]["$binary"]["base64"]
                        data: np.ndarray = await asyncio.to_thread(
                            decompress_to_floats, b64_data
                        )

                        doc2 = json.loads(line2)
                        b64_elec_data: str = doc2["data"]["$binary"]["base64"]
                        elec_data: np.ndarray = await asyncio.to_thread(
                            decompress_to_floats, b64_elec_data
                        )

                        yield data, elec_data

                    except Exception as e:
                        print(f"Warning line {line_num}: {e}")
                        continue

                    await asyncio.sleep(CHUNK_INTERVAL)
        except KeyboardInterrupt:
            print("\nStopping...")
            break
        except Exception as e:
            print(f"Error: {e}")
            break
