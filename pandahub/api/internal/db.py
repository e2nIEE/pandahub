import asyncio
import uuid

import motor.motor_asyncio
from beanie import Document
from fastapi_users.db import BeanieBaseUser, BeanieUserDatabase
from fastapi_users_db_beanie.access_token import (
    BeanieAccessTokenDatabase,
    BeanieBaseAccessToken,
)

from .. import pandahub_app_settings as ph_settings
from pydantic import Field

mongo_client_args = {"host": ph_settings.mongodb_url, "uuidRepresentation": "standard", "connect": False}
if ph_settings.mongodb_user:
    mongo_client_args |= {"username": ph_settings.mongodb_user, "password": ph_settings.mongodb_password}

client = motor.motor_asyncio.AsyncIOMotorClient(**mongo_client_args)
client.get_io_loop = asyncio.get_event_loop
db = client["user_management"]

class User(BeanieBaseUser, Document):
    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    is_active: bool = not ph_settings.registration_admin_approval
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
