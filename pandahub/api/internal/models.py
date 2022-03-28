from fastapi_users import models
from fastapi_users.authentication.strategy.db import BaseAccessToken


class User(models.BaseUser):
    pass


class UserCreate(models.BaseUserCreate):
    pass


class UserUpdate(models.BaseUserUpdate):
    pass


class UserDB(User, models.BaseUserDB):
    pass


class AccessToken(BaseAccessToken):
    pass
