FROM python:3.9 as dev
ENV PYTHONUNBUFFERED 1
ENV PYTHONPATH /code/
WORKDIR /code
COPY ./requirements.txt .
RUN pip install -r requirements.txt
RUN pip install watchdog pyyaml
CMD uvicorn --host "0.0.0.0" --port "8002" "pandahub.api.main:app" --reload
