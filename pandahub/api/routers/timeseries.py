"""FastAPI router for timeseries read/write operations."""

import json

import pandas as pd
from fastapi import APIRouter
from pydantic import BaseModel

from pandahub.api.dependencies import PandahubDep

router = APIRouter(prefix="/timeseries", tags=["timeseries"])


# -------------------------------
#  ROUTES
# -------------------------------


class GetTimeSeriesModel(BaseModel):
    """Request body for retrieving a single timeseries from the database."""

    filter_document: dict | None = {}
    global_database: bool | None = False
    project_id: str | None = None
    timestamp_range: tuple | None = None
    exclude_timestamp_range: tuple | None = None
    collection_name: str | None = "timeseries"


@router.post("/get_timeseries_from_db")
def get_timeseries_from_db(data: GetTimeSeriesModel, ph: PandahubDep) -> str:
    """Return a single timeseries matching the filter as an ISO JSON string."""
    if data.timestamp_range is not None:
        data.timestamp_range = [pd.Timestamp(t) for t in data.timestamp_range]
    ts = ph.get_timeseries_from_db(**data.model_dump())
    return ts.to_json(date_format="iso")


class MultiGetTimeSeriesModel(BaseModel):
    """Request body for retrieving multiple timeseries from the database."""

    filter_document: dict | None = {}
    global_database: bool | None = False
    project_id: str | None = None
    timestamp_range: tuple | None = None
    exclude_timestamp_range: tuple | None = None
    collection_name: str | None = "timeseries"


@router.post("/multi_get_timeseries_from_db")
def multi_get_timeseries_from_db(data: MultiGetTimeSeriesModel, ph: PandahubDep) -> list:
    """Return multiple timeseries matching the filter, each with ISO-serialised data."""
    if data.timestamp_range is not None:
        data.timestamp_range = [pd.Timestamp(t) for t in data.timestamp_range]
    ts = ph.multi_get_timeseries_from_db(**data.model_dump(), include_metadata=True)
    for i, data in enumerate(ts):
        ts[i]["timeseries_data"] = data["timeseries_data"].to_json(date_format="iso")
    return ts


class GetTimeseriesMetadataModel(BaseModel):
    """Request body for retrieving timeseries metadata."""

    project_id: str
    filter_document: dict | None = {}
    global_database: bool | None = False
    collection_name: str | None = "timeseries"


@router.post("/get_timeseries_metadata")
def get_timeseries_metadata(data: GetTimeseriesMetadataModel, ph: PandahubDep) -> dict:
    """Return timeseries metadata matching the filter as a JSON-serialisable dict."""
    ph.set_active_project_by_id(data.project_id)
    ts = ph.get_timeseries_metadata(
        filter_document=data.filter_document,
        global_database=data.global_database,
        collection_name=data.collection_name,
    )
    return json.loads(ts.to_json(orient="index"))


class WriteTimeSeriesModel(BaseModel):
    """Request body for writing a timeseries to the database."""

    timeseries: str
    project_id: str | None = None
    data_type: str | None = None
    element_type: str | None = None
    netname: str | None = None
    element_index: int | None = None
    global_database: bool | None = False
    collection_name: str | None = "timeseries"
    name: str | None = None


@router.post("/write_timeseries_to_db")
def write_timeseries_to_db(data: WriteTimeSeriesModel, ph: PandahubDep) -> bool:
    """Write a timeseries (provided as JSON string) to the database and return True."""
    data.timeseries = pd.Series(json.loads(data.timeseries))
    data.timeseries.index = pd.to_datetime(data.timeseries.index)
    ph.write_timeseries_to_db(**data.model_dump())
    return True
