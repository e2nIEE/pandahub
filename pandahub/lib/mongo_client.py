from contextlib import contextmanager
from typing import Generator, overload

from pymongo import MongoClient
from pymongo.synchronous.collection import Collection
from pymongo.synchronous.database import Database

from pandahub.lib.settings import pandahub_settings as settings


def _get_mongo_client(
    connection_url: str = settings.mongodb_url,
    connection_user: str = settings.mongodb_user,
    connection_password: str = settings.mongodb_password,
    background_connect: bool = True,
) -> MongoClient:
    mongo_client_args = {
        "host": connection_url,
        "uuidRepresentation": "standard",
        "connect": background_connect,
    }
    if connection_user:
        mongo_client_args |= {
            "username": connection_user,
            "password": connection_password,
        }
    return MongoClient(**mongo_client_args)

if settings.pandahub_global_db_client:
    _global_mongo_client = _get_mongo_client(connection_url=settings.mongodb_url, connection_user = settings.mongodb_user,
                                             connection_password = settings.mongodb_password, background_connect=False)
else:
    _global_mongo_client = None




@overload
def get_mongo_client(database: None = None, collection: None = None,
                     connection_url: str = ..., connection_user: str = ..., connection_password: str = ...) -> MongoClient: ...
@overload
def get_mongo_client(database: str, collection: None=None,
                     connection_url: str = ..., connection_user: str = ..., connection_password: str = ...) -> Database: ...
@overload
def get_mongo_client(database: str, collection: str,
                     connection_url: str = ..., connection_user: str = ..., connection_password: str = ...) -> Collection: ...

def get_mongo_client(
    database: str | None = None,
    collection: str | None = None,
    connection_url: str = settings.mongodb_url,
    connection_user: str = settings.mongodb_user,
    connection_password: str = settings.mongodb_password,
) -> MongoClient | Database | Collection:
    if collection is not None and database is None:
        raise ValueError("Must specify database to access a collection!")
    if _global_mongo_client is None:
        client = _get_mongo_client(connection_url, connection_user, connection_password)
    else:
        client = _global_mongo_client
    return _get_db_or_coll(client, database, collection)


@contextmanager
def mongo_client(
    database: str | None = None,
    collection: str | None = None,
    connection_url: str = settings.mongodb_url,
    connection_user: str = settings.mongodb_user,
    connection_password: str = settings.mongodb_password,
) -> Generator[MongoClient | Database | Collection, None, None]:
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


def _get_db_or_coll(client: MongoClient, database: str | None = None, collection: str | None = None) -> MongoClient | Database | Collection:
    if database is not None and collection is not None:
        return client[database][collection]
    elif database is not None:
        return client[database]
    else:
        return client
