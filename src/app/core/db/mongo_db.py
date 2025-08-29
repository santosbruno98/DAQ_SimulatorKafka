'''
Scripts to connect to MongoDB database, 
Uses AsyncioMotorClient for async operations.
'''
from pymongo.errors import PyMongoError
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.server_api import ServerApi
import os
from dotenv import load_dotenv, dotenv_values

load_dotenv() 
ACQ_COLLECTION = os.getenv('ACQ_COLLECTION')
TRACE_COLLECTION = os.getenv('TRACE_COLLECTION')
ELEC_COLLECTION = os.getenv('ELEC_COLLECTION')

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
    def __init__(self, uri : str, db_name : str) -> None:
        self.client = AsyncIOMotorClient(
            uri = uri,
            maxPoolSize = 500,
            compressors = 'zlib',
            server_api=ServerApi('1'),
            
        )
        self.db_name = db_name
        self.db = self.client[self.db_name]
       
    async def insert_acquisition_data(
        self, waveform, sweeps_id, laser_metadata_id, electrical_data=None
    ):
        try:
            compressed_waveform = zlib.compress(pickle.dumps(waveform))
            acquisition_document = {
                "sweeps_id": sweeps_id,
                "laser_metadata_id": laser_metadata_id,
                "data": compressed_waveform,
            }
            await self.db[ACQ_COLLECTION].insert_one(acquisition_document)
            compressed_trace = zlib.compress(pickle.dumps(waveform[0, :]))
            trace_document = {
                "sweeps_id": sweeps_id,
                "laser_metadata_id": laser_metadata_id,
                "data": compressed_trace,
            }
            await self.db[TRACE_COLLECTION].insert_one(trace_document)

            if electrical_data is not None:
                compresses_electrical_data = zlib.compress(
                    pickle.dumps(np.array(electrical_data))
                )

                electrical_document = {
                    "laser_metadata_id": laser_metadata_id,
                    "sweeps_id": sweeps_id,
                    "data": compresses_electrical_data,
                }
                await self.db[ELEC_COLLECTION].insert_one(electrical_document)

        except PyMongoError as e:
            self.logger.error(f"Error inserting acquisition data: {str(e)}")
            raise