from pymongo import MongoClient

from pandahub.api.internal.settings import PANDAHUB_GLOBAL_DB_CLIENT, MONGODB_URL, MONGODB_USER, MONGODB_PASSWORD

def _get_mongo_client(connection_url: str = MONGODB_URL, connection_user: str = MONGODB_USER,
                     connection_password: str = MONGODB_PASSWORD) -> MongoClient:
    mongo_client_args = {
        "host": connection_url,
        "uuidRepresentation": "standard",
        "connect": False,
    }
    if connection_user:
        mongo_client_args |= {
            "username": connection_user,
            "password": connection_password,
        }
    return MongoClient(**mongo_client_args)

if PANDAHUB_GLOBAL_DB_CLIENT:
    __global_mongo_client = _get_mongo_client(connection_url=MONGODB_URL, connection_user = MONGODB_USER,connection_password = MONGODB_PASSWORD)
else:
    __global_mongo_client = None
