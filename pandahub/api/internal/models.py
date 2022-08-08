from fastapi_users import models
from fastapi_users.authentication.strategy.db import BaseAccessToken

from pandahub.api.internal.settings import REGISTRATION_ADMIN_APPROVAL


class User(models.BaseUser):
    is_active: bool = not REGISTRATION_ADMIN_APPROVAL


class UserCreate(models.BaseUserCreate):
    pass


class UserUpdate(models.BaseUserUpdate):
    pass


class UserDB(User, models.BaseUserDB):
    pass


class AccessToken(BaseAccessToken):
    pass
