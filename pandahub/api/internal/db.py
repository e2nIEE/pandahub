import asyncio
import uuid

import motor.motor_asyncio
from beanie import Document
from fastapi_users.db import BeanieBaseUser, BeanieUserDatabase
from fastapi_users_db_beanie.access_token import (
    BeanieAccessTokenDatabase,
    BeanieBaseAccessToken,
)

from pandahub.api.internal.settings import REGISTRATION_ADMIN_APPROVAL, MONGODB_URL, MONGODB_USER, MONGODB_PASSWORD
from pydantic import Field

mongo_client_args = {"host": MONGODB_URL, "uuidRepresentation": "standard", "connect": False}
if MONGODB_USER:
    mongo_client_args |= {"username": MONGODB_USER, "password": MONGODB_PASSWORD}

client = motor.motor_asyncio.AsyncIOMotorClient(**mongo_client_args)
client.get_io_loop = asyncio.get_event_loop
db = client["user_management"]

class User(BeanieBaseUser, Document):
    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    is_active: bool = not REGISTRATION_ADMIN_APPROVAL
    class Settings(BeanieBaseUser.Settings):
        name = "users"


class AccessToken(BeanieBaseAccessToken, Document):
    user_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    class Settings(BeanieBaseAccessToken.Settings):
        name = "access_tokens"

async def get_user_db():
    yield BeanieUserDatabase(User)

async def get_access_token_db():
    yield BeanieAccessTokenDatabase(AccessToken)
