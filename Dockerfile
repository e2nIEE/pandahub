FROM python:3.10 as dev

ENV PYTHONUNBUFFERED 1
ENV PYTHONPATH /code/pandahub

COPY ./ /code/pandahub/
WORKDIR /code/pandahub

RUN python -m pip install --upgrade pip
RUN python -m pip install .["all"]
RUN python -m pip install watchdog pyyaml

CMD uvicorn --host "0.0.0.0" --port "8002" "pandahub.api.main:app" --reload
