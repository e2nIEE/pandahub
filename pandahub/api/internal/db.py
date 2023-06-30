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
    if settings.COSMOSDB_COMPAT:
        yield MongoDBUserDatabaseCosmos(UserDB, collection)
    else:
        yield MongoDBUserDatabase(UserDB, collection)


async def get_access_token_db():
    yield MongoDBAccessTokenDatabase(AccessToken, access_tokens_collection)

class MongoDBUserDatabaseCosmos(MongoDBUserDatabase):
    from typing import Optional
    from fastapi_users.models import UD
    async def get_by_email(self, email: str) -> Optional[UD]:
        await self._initialize()

        user = await self.collection.find_one(
            {"email": email}
        )
        return self.user_db_model(**user) if user else None

    async def _initialize(self):
        if not self.initialized:
            if "email_1" not in await self.collection.index_information():
                await self.collection.create_index("id", unique=True)
                await self.collection.create_index("email", unique=True)
        self.initialized = True
