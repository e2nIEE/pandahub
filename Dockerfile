# dev stage
FROM python:3.11 AS dev
ENV PYTHONUNBUFFERED=1
ENV PATH="/opt/venv/bin:$PATH"
WORKDIR /src
COPY requirements.txt .
RUN python -m venv /opt/venv && \
    python -m pip install -r requirements.txt
CMD ["python",  "-m", "pandahub.api.main"]

# production stage
FROM python:3.11-slim AS production-stage
ENV PATH="/opt/venv/bin:$PATH"
WORKDIR /src
# copy files from build stage
COPY --from=dev /opt/venv /opt/venv
COPY . .
CMD ["uvicorn", "--host", "127.0.0.1", "--port", "8080", "pandahub.api.main:app"]
