from fastapi import APIRouter

from pandahub.api.internal.users import auth_backend, fastapi_users
from pandahub.api.internal.schemas import UserCreate, UserRead
from .. import pandahub_app_settings as ph_settings

router = APIRouter(prefix="/auth", tags=["auth"])

router.include_router(fastapi_users.get_auth_router(auth_backend))

if ph_settings.registration_enabled:
    router.include_router(
        fastapi_users.get_register_router(UserRead, UserCreate),
    )

    router.include_router(
        fastapi_users.get_verify_router(UserRead),
    )
