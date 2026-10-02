import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import uvicorn
from beanie import init_beanie
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from pandahub.api.internal.db import AccessToken, User, db
from pandahub.api.routers import auth, net, projects, timeseries, users, variants
from pandahub.lib.PandaHub import PandaHubError

from . import pandahub_app_settings as ph_settings


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    """Initialise Beanie ODM on startup."""
    await init_beanie(
        database=db,
        document_models=[User, AccessToken],
    )
    yield


app = FastAPI(lifespan=lifespan)

origins = [
    "http://localhost:8080",
]

if ph_settings.debug:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

app.include_router(net.router)
app.include_router(projects.router)
app.include_router(timeseries.router)
app.include_router(users.router)
app.include_router(auth.router)
app.include_router(variants.router)


@app.exception_handler(PandaHubError)
async def pandahub_exception_handler(_request: Request, exc: PandaHubError) -> JSONResponse:
    """Convert PandaHubError exceptions into JSON HTTP responses."""
    return JSONResponse(
        status_code=exc.status_code,
        content=str(exc),
    )


@app.get("/")
async def ready():
    """Return a liveness check response."""
    if ph_settings.debug:
        return os.environ
    return "Hello World!"


if __name__ == "__main__":
    uvicorn.run(
        "pandahub.api.main:app",
        host=ph_settings.pandahub_server_url,
        port=ph_settings.pandahub_server_port,
        log_level="info",
        reload=True,
        workers=ph_settings.workers,
    )
