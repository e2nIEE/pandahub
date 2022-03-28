import uvicorn
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from pandahub.lib.PandaHub import PandaHubError
from pandahub.api.routers import net, projects, timeseries, users, auth

app = FastAPI()

origins = [
    "http://localhost:8080",
]

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


@app.exception_handler(PandaHubError)
async def pandahub_exception_handler(request: Request, exc: PandaHubError):
    return JSONResponse(
        status_code=exc.status_code,
        content=str(exc),
    )


if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8002, log_level="info", reload=True)
