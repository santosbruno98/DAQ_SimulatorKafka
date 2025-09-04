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
LASER_COLLECTION = os.getenv("LASER_COLLECTION")
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
                print(f"[Atlas Full] Cannot insert into {collection_name}:")
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

    async def insert_elec_data(
        self,
        sweeps_id: ObjectId,
        data: np.ndarray,
        laser_metadata_id: ObjectId,
        electrical_data: np.ndarray = None,
    ):
        """Insert acquisition data into MongoDB collections."""
        try:
            laser_document = {
                "laser_metadata_id": laser_metadata_id,
                "sweeps_id": sweeps_id,
                "fiber_t_initial_point": 1000,
                "fiber_t_final_point": 1400,
                "fiber_rh_initial_point": 1500,
                "fiber_rh_final_point": 1900,
                "points_sensor": {"0": (1000, 2000), "1": (2100, 3100)},
                "check_reversed": True,
                "slope_temperature": 1.57,
                "slope_humidity": 0.18,
                "slope_temperature_fiber_rh": 1.39,
                "sweeps_mode": "temperature",
                "sweeps_mode_initial": 20,
                "sweeps_mode_final": 30,
                "sweeps_mode_step": 0.02,
                "current_frequency_step_a": -0.0000223825268541589,
                "current_frequency_step_b": 0.0345228281003642,
                "temperature_frequency_step": -9.85270358584453,
            }
            nr_docs = await self.db[LASER_COLLECTION].count_documents(
                {"laser_metadata_id": laser_metadata_id}
            )

            print(nr_docs, laser_metadata_id, sweeps_id)
            if nr_docs == 0:
                await self.insert_document(LASER_COLLECTION, laser_document)
            else:
                print(
                    f"Laser metadata with sweeps_id {sweeps_id} already exists. Skipping insertion in {LASER_COLLECTION}."
                )
            compressed_trace: bytes = zlib.compress(pickle.dumps(data[0, :]))
            trace_document: dict[str, Any] = {
                "laser_metadata_id": laser_metadata_id,
                "sweeps_id": sweeps_id,
                "data": compressed_trace,
            }
            await self.insert_document(TRACE_COLLECTION, trace_document)

            if electrical_data is not None:
                compresses_electrical_data = zlib.compress(
                    pickle.dumps(np.array(electrical_data))
                )

                electrical_document = {
                    "laser_metadata_id": laser_metadata_id,
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

    async def get_acquisition_characteristics(
        self,
        laser_metadata_id: ObjectId,
    ) -> tuple[int, str, float, float, float, float, float, float, int, int, int, int]:
        query = {"laser_metadata_id": ObjectId(str(laser_metadata_id))}
        find_db = self.db[LASER_COLLECTION].find(query)
        all_laser_metadata = []
        async for doc in find_db:
            all_laser_metadata.append(doc)
            if all_laser_metadata:
                laser_metadata = all_laser_metadata[0]
                return tuple(
                    map(
                        laser_metadata.get,
                        [
                            "sweeps_mode",
                            "sweeps_mode_initial",
                            "sweeps_mode_final",
                            "sweeps_mode_step",
                            "current_frequency_step_a",
                            "current_frequency_step_b",
                            "temperature_frequency_step",
                            "fiber_t_initial_point",
                            "fiber_t_final_point",
                            "fiber_rh_initial_point",
                            "fiber_rh_final_point",
                        ],
                    )
                )
