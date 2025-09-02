"""Script to simulate streaming DAQ data by reading from a local JSON file."""

from __future__ import annotations

import asyncio
import base64
import json
import pickle
import time
import zlib
from collections.abc import Iterator
from typing import Any

import numpy as np

BUCKET_NAME: str = "daqrawdata"
API_GATEWAY_URL: str = "https://plyb1o6d1j.execute-api.eu-west-3.amazonaws.com/dev/"
CHUNK_INTERVAL: float = 0.1  # seconds between uploads
SOURCE_FILE: str = "/code/data/acquisition_characteristics.json"

# TODO: make method to query mongodb documents, now is just gonna read a exported query from compass


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
            with open(SOURCE_FILE, encoding="utf-8") as f: # mode (r)eading is default
                for line_num, line in enumerate(f, 1):
                    if not line.strip():
                        continue

                    try:
                        doc = json.loads(line)
                        b64_data: str = doc["data"]["$binary"]["base64"]
                        data: np.ndarray = await asyncio.to_thread(
                            decompress_to_floats, b64_data
                        )

                        # TODO: ALSO YIELD ELECTRICAL_DATA LOL
                        yield data  # , electrical_data
                    except Exception as e:
                        print(f"Warning line {line_num}: {e}")
                        continue

                    print(f"Waiting {CHUNK_INTERVAL}s before next upload...")
                    time.sleep(CHUNK_INTERVAL)

        except KeyboardInterrupt:
            print("\nStopping...")
            break
        except Exception as e:
            print(f"Error: {e}")
            break
