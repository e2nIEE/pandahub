from contextlib import contextmanager
from typing import Iterator

from pymongo import MongoClient
from pymongo.synchronous.collection import Collection
from pymongo.synchronous.database import Database

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
    _global_mongo_client = _get_mongo_client(connection_url=MONGODB_URL, connection_user = MONGODB_USER, connection_password = MONGODB_PASSWORD)
else:
    _global_mongo_client = None


def get_mongo_client(database: str | None = None, collection: str | None = None, connection_url: str = MONGODB_URL, connection_user: str = MONGODB_USER,
                     connection_password: str = MONGODB_PASSWORD) -> Iterator[MongoClient | Database | Collection]:
    if collection is not None and database is None:
        raise ValueError("Must specify database to access a collection!")
    if _global_mongo_client is None:
        client = _get_mongo_client(connection_url, connection_user, connection_password)
    else:
        client = _global_mongo_client
    return _get_db_or_coll(client, database, collection)


@contextmanager
def mongo_client(database: str | None = None, collection: str | None = None, connection_url: str = MONGODB_URL,
                 connection_user: str = MONGODB_USER, connection_password: str = MONGODB_PASSWORD) -> Iterator[MongoClient | Database | Collection]:
    """Contextmanager for pymongo MongoClient / Database / Collection with close after use.

    Parameters
    ----------
    database: str or None
        The database to connect to
    collection: str or None
        The collection to connect to
    connection_url: str or None
        Defaults to MONGODB_URL env var
    connection_user: str or None
        Defaults to MONGODB_USER env var
    connection_password: str or None
        Defaults to MONGODB_PASSWORD env var

    Returns
    -------
        Contextmanager yielding MongoClient / Database / Collection
    """
    if collection is not None and database is None:
        raise ValueError("Must specify database to access a collection!")
    client = _get_mongo_client(connection_url, connection_user, connection_password)
    try:
        yield _get_db_or_coll(client, database, collection)
    finally:
        client.close()


def _get_db_or_coll(client: MongoClient, database: str | None = None, collection: str | None = None) -> Iterator[MongoClient | Database | Collection]:
    if database is not None and collection is not None:
        return client[database][collection]
    elif database is not None:
        return client[database]
    else:
        return client
