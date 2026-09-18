import json

import pandas as pd
from fastapi import APIRouter, Depends
from pydantic import BaseModel

from pandahub.api.dependencies import pandahub

router = APIRouter(prefix="/timeseries", tags=["timeseries"])


# -------------------------------
#  ROUTES
# -------------------------------


class GetTimeSeriesModel(BaseModel):
    filter_document: dict | None = {}
    global_database: bool | None = False
    project_id: str | None = None
    timestamp_range: tuple | None = None
    exclude_timestamp_range: tuple | None = None
    collection_name: str | None = "timeseries"


@router.post("/get_timeseries_from_db")
def get_timeseries_from_db(data: GetTimeSeriesModel, ph=Depends(pandahub)):
    if data.timestamp_range is not None:
        data.timestamp_range = [pd.Timestamp(t) for t in data.timestamp_range]
    ts = ph.get_timeseries_from_db(**data.model_dump())
    return ts.to_json(date_format="iso")


class MultiGetTimeSeriesModel(BaseModel):
    filter_document: dict | None = {}
    global_database: bool | None = False
    project_id: str | None = None
    timestamp_range: tuple | None = None
    exclude_timestamp_range: tuple | None = None
    collection_name: str | None = "timeseries"


@router.post("/multi_get_timeseries_from_db")
def multi_get_timeseries_from_db(data: MultiGetTimeSeriesModel, ph=Depends(pandahub)):
    if data.timestamp_range is not None:
        data.timestamp_range = [pd.Timestamp(t) for t in data.timestamp_range]
    ts = ph.multi_get_timeseries_from_db(**data.model_dump(), include_metadata=True)
    for i, data in enumerate(ts):
        ts[i]["timeseries_data"] = data["timeseries_data"].to_json(date_format="iso")
    return ts


class GetTimeseriesMetadataModel(BaseModel):
    project_id: str
    filter_document: dict | None = {}
    global_database: bool | None = False
    collection_name: str | None = "timeseries"


@router.post("/get_timeseries_metadata")
def get_timeseries_metadata(data: GetTimeseriesMetadataModel, ph=Depends(pandahub)):
    ph.set_active_project_by_id(data.project_id)
    ts = ph.get_timeseries_metadata(
        filter_document=data.filter_document,
        global_database=data.global_database,
        collection_name=data.collection_name,
    )
    ts = json.loads(ts.to_json(orient="index"))
    return ts


class WriteTimeSeriesModel(BaseModel):
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
def write_timeseries_to_db(data: WriteTimeSeriesModel, ph=Depends(pandahub)):
    data.timeseries = pd.Series(json.loads(data.timeseries))
    data.timeseries.index = pd.to_datetime(data.timeseries.index)
    ph.write_timeseries_to_db(**data.model_dump())
    return True
