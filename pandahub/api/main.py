from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from pandahub.lib.PandaHub import PandaHubError
from pandahub.api.routers import net, projects, timeseries, users, auth, variants
from pandahub.api.internal.db import User, db, AccessToken
from . import pandahub_app_settings as ph_settings
from beanie import init_beanie

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_beanie(
        database=db,
        document_models=[
            User,
            AccessToken
        ],
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
async def pandahub_exception_handler(request: Request, exc: PandaHubError):
    return JSONResponse(
        status_code=exc.status_code,
        content=str(exc),
    )

@app.get("/")
async def ready():
    if ph_settings.debug:
        import os
        return os.environ
    return "Hello World!"


if __name__ == "__main__":
    uvicorn.run("pandahub.api.main:app",
                host=ph_settings.pandahub_server_url,
                port=ph_settings.pandahub_server_port,
                log_level="info",
                reload=True,
                workers=ph_settings.workers)
