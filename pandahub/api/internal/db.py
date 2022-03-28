import asyncio

import motor.motor_asyncio
from fastapi_users.db import MongoDBUserDatabase
from fastapi_users_db_mongodb.access_token import MongoDBAccessTokenDatabase

from pandahub.api.internal import settings
from pandahub.api.internal.models import AccessToken, UserDB

client = motor.motor_asyncio.AsyncIOMotorClient(
    settings.MONGODB_URL, uuidRepresentation="standard"
)

client.get_io_loop = asyncio.get_event_loop

db = client["user_management"]
collection = db["users"]
access_tokens_collection = db["access_tokens"]


async def get_user_db():
    yield MongoDBUserDatabase(UserDB, collection)


async def get_access_token_db():
    yield MongoDBAccessTokenDatabase(AccessToken, access_tokens_collection)
