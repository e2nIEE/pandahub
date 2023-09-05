from fastapi import APIRouter

from pandahub.api.internal.users import auth_backend, fastapi_users
from pandahub.api.internal.schemas import UserCreate, UserRead
from pandahub.api.internal.settings import REGISTRATION_ENABLED

router = APIRouter(prefix="/auth", tags=["auth"])

router.include_router(fastapi_users.get_auth_router(auth_backend))

router.include_router(fastapi_users.get_reset_password_router())

if REGISTRATION_ENABLED:
    router.include_router(
        fastapi_users.get_register_router(UserRead, UserCreate),
    )

    router.include_router(
        fastapi_users.get_verify_router(UserRead),
    )
