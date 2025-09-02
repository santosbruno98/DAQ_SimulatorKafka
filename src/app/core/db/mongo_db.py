"""
Scripts to connect to MongoDB database,
Uses AsyncioMotorClient for async operations.
"""

import os
import pickle
import zlib
from typing import Any

import numpy as np
from bson import ObjectId
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import PyMongoError, WriteError
from pymongo.server_api import ServerApi

load_dotenv()
ACQ_COLLECTION = os.getenv("ACQ_COLLECTION")
CORR_COLLECTION = os.getenv("CORR_COLLECTION")
CONV_COLLECTION = os.getenv("CONV_COLLECTION")
TRACE_COLLECTION = os.getenv("TRACE_COLLECTION")
ELEC_COLLECTION = os.getenv("ELEC_COLLECTION")
URI = os.getenv("MONGODB_CONNECTION_STRING")


class MongoDB:
    """
    Asynchronous MongoDB client wrapper using Motor.

    This class initializes an asynchronous connection to a MongoDB database
    with configurable URI and database name. It supports connection pooling
    and zlib compression.

    Attributes:
        client (AsyncIOMotorClient): The Motor client instance for connecting to MongoDB.
        db_name (str): Name of the database to connect to.
        db (AsyncIOMotorDatabase): The database instance for performing operations.

    Args:
        uri (str): MongoDB connection URI.
        db_name (str): Name of the database to use.
    """

    def __init__(self, db_name: str) -> None:
        self.client = AsyncIOMotorClient(
            URI,
            maxPoolSize=500,
            compressors="zlib",
            server_api=ServerApi("1"),
        )
        self.db_name = db_name
        self.db = self.client[self.db_name]

    async def insert_document(self, collection_name: str, document: dict) -> None:
        """Insert a document into a specified collection."""
        try:
            await self.db[collection_name].insert_one(document)
        except WriteError as e:
            if getattr(e, "code", None) == 8000:
                print(
                    f"[Atlas Full] Cannot insert into {collection_name}:"
                )
                print(
                    f" {e.details.get('errmsg') if hasattr(e, 'details') else str(e)}"
                    )
                print("Method is leaving without inserting document.")
                return
            else:
                print(f"Error inserting document into {collection_name}: {str(e)}")
                raise
        except PyMongoError as e:
            print(f"Error inserting document into {collection_name}: {str(e)}")
            return

    async def insert_acquisition_data(
        self, sweeps_id: ObjectId, data: np.ndarray, electrical_data: np.ndarray = None
    ):
        """Insert acquisition data into MongoDB collections."""
        try:
            compressed_trace: bytes = zlib.compress(pickle.dumps(data[0, :]))
            trace_document: dict[str, Any] = {
                "sweeps_id": sweeps_id,
                "data": compressed_trace,
            }
            await self.insert_document(TRACE_COLLECTION, trace_document)

            if electrical_data is not None:
                compresses_electrical_data = zlib.compress(
                    pickle.dumps(np.array(electrical_data))
                )

                electrical_document = {
                    "sweeps_id": sweeps_id,
                    "data": compresses_electrical_data,
                }
                await self.insert_document(ELEC_COLLECTION, electrical_document)

        except PyMongoError as e:
            print(f"Error inserting acquisition data: {str(e)}")
            return

    async def insert_correlation_data(
        self, sweeps_id: ObjectId, data: np.ndarray
    ) -> None:
        """Insert correlation data into MongoDB."""
        try:
            compressed_data = zlib.compress(pickle.dumps(data))
            correlation_document: dict[str, bytes] = {
                "sweeps_id": sweeps_id,
                "data": compressed_data,
            }
            await self.insert_document(CORR_COLLECTION, correlation_document)
        except PyMongoError as e:
            print(f"Error inserting correlation data: {str(e)}")
            return

    async def insert_conversion_data(
        self, sweeps_id: ObjectId, data: np.ndarray
    ) -> None:
        """Insert conversion data into MongoDB."""
        try:
            compressed_data = zlib.compress(pickle.dumps(data))
            conversion_document: dict[str, bytes] = {
                "sweeps_id": sweeps_id,
                "data": compressed_data,
            }
            await self.insert_document(CONV_COLLECTION, conversion_document)
        except PyMongoError as e:
            print(f"Error inserting conversion data: {str(e)}")
            return
