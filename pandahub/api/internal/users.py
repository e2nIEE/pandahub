import uuid

from typing import Optional

from fastapi import Depends, Request
from fastapi_users import BaseUserManager, FastAPIUsers, UUIDIDMixin
from fastapi_users.authentication import AuthenticationBackend, BearerTransport
from fastapi_users.authentication.strategy.db import (
    AccessTokenDatabase,
    DatabaseStrategy,
)
from fastapi_users.db import BeanieUserDatabase

from ..internal import settings
from ..internal.db import get_user_db, get_access_token_db, User, AccessToken
from ..internal.toolbox import send_password_reset_mail, send_verification_email


class UserManager(UUIDIDMixin, BaseUserManager[User, uuid.UUID]):
    if settings.SECRET is None:
        raise UserWarning(
            "You must specify a SECRET in the environment variables or .env file"
        )
    reset_password_token_secret = settings.SECRET
    verification_token_secret = settings.SECRET

    async def on_after_register(self, user: User, request: Optional[Request] = None):
        if settings.EMAIL_VERIFICATION_REQUIRED:
            await self.request_verify(user)
        print(f"User {user.id} has registered.")

    async def on_after_forgot_password(
        self, user: User, token: str, request: Optional[Request] = None
    ):
        await send_password_reset_mail(user, token)

    #         print(f"User {user.id} has forgot their password. Reset token: {token}")

    async def on_after_request_verify(
        self, user: User, token: str, request: Optional[Request] = None
    ):
        await send_verification_email(user, token)


#         print(f"Verification requested for user {user.id}. Verification token: {token}")


async def get_user_manager(user_db:BeanieUserDatabase = Depends(get_user_db)):
    yield UserManager(user_db)


bearer_transport = BearerTransport(tokenUrl="auth/login")


def get_database_strategy(
    access_token_db: AccessTokenDatabase[AccessToken] = Depends(get_access_token_db),
) -> DatabaseStrategy:
    return DatabaseStrategy(access_token_db)


auth_backend = AuthenticationBackend(
    name="jwt",
    transport=bearer_transport,
    get_strategy=get_database_strategy,
)
fastapi_users = FastAPIUsers(get_user_manager, [auth_backend])
