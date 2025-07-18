FROM python:3.12-slim
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy PANDAHUB_GLOBAL_DB_CLIENT="true" PATH="/app/.venv/bin:$PATH"
COPY --from=ghcr.io/astral-sh/uv:0.7.15 /uv /uvx /bin/
WORKDIR /app
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --no-dev
COPY . /app
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --extra rest-api
ENV PATH="/app/.venv/bin:$PATH"
CMD ["fastapi", "run", "pandahub/api/main.py"]
