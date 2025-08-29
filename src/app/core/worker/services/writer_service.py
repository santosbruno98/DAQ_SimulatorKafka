'''
Consumer 3 (Writer Service), subcribes to 'raw-electrial-data', 
'correlation-data' and 'conversion-data' topics, writes to MongoDB.
'''

from app.core.utils.kafka_helper import get_consumer, TOPICS , BOOTSTRAP_SERVERS
from app.core.db.mongo_db import MongoDB
import numpy as np
from dotenv import load_dotenv
import os
import zlib, pickle

def deserialize_array(data: bytes) -> np.ndarray:
    """
    Deserialize bytes back to numpy array (reverse of serialize_array from kafka_helper.py)
    This matches the serialize_array function in kafka_helper.py
    """
    decompressed: bytes = zlib.decompress(data)
    arr: np.ndarray = pickle.loads(decompressed)
    return arr


async def run() -> None:
    load_dotenv()
    URI = os.getenv("MONGODB_CONNECTION_STRING")
    DB_NAME = os.getenv("DB_NAME")
    mongo_db = MongoDB(uri=URI, db_name=DB_NAME)
    db = mongo_db.db
    
    raw_consumer = get_consumer(
        topic=TOPICS['raw-electrical'],
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id='writer-service-group'
        )

    corr_consumer = get_consumer(
        topic=TOPICS['correlation'],
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id='writer-service-group'
        )
    
    conv_consumer = get_consumer(
        topic=TOPICS['conversion'],
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id='writer-service-group'
        )
    
    
    for msg in raw_consumer:
        try:
            raw_data_bytes : bytes = msg.value
            
            raw_document : dict[str, bytes] = {
                'raw_data':  raw_data_bytes,
            }
            print('--- Writing raw data to MongoDB ---\n ', raw_document)
            await db[ACQ_COLLECTION].insert_one(raw_document)
            
        except Exception as e:
            print(f'Error inserting raw data message: {e}')
    
    for msg in corr_consumer:
        try:
            corr_data_bytes : bytes = msg.value
            
            corr_document : dict[str, bytes] = {
                'corr_data':  corr_data_bytes,
            }
            print('--- Writing corr data to MongoDB ---\n ', corr_document)
            await db[CORR_COLLECTION].insert_one(corr_document)
            
        except Exception as e:
            print(f'Error inserting raw data message: {e}')
    
    for msg in conv_consumer:
        try:
            conv_data_bytes : bytes = msg.value
            
            conv_document : dict[str, bytes] = {
                'conv_data':  conv_data_bytes,
            }
            print('--- Writing t-rh data to MongoDB ---\n ', conv_document)
            await db[CONV_COLLECTION].insert_one(conv_document)
            
        except Exception as e:
            print(f'Error inserting raw data message: {e}')
        