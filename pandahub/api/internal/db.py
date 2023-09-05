import asyncio

import motor.motor_asyncio
from fastapi_users.db import MongoDBUserDatabase
from fastapi_users_db_mongodb.access_token import MongoDBAccessTokenDatabase

from pandahub.api.internal import settings
from pandahub.api.internal.models import AccessToken, UserDB

mongo_client_args = {"host": settings.MONGODB_URL, "uuidRepresentation": "standard", "connect": False}
if settings.MONGODB_USER:
    mongo_client_args |= {"username": settings.MONGODB_USER, "password": settings.MONGODB_PASSWORD}

client = motor.motor_asyncio.AsyncIOMotorClient(**mongo_client_args)

client.get_io_loop = asyncio.get_event_loop

db = client["user_management"]
collection = db["users"]
access_tokens_collection = db["access_tokens"]


async def get_user_db():
    yield MongoDBUserDatabaseCosmos(UserDB, collection)


async def get_access_token_db():
    yield MongoDBAccessTokenDatabase(AccessToken, access_tokens_collection)
