import json

import pandas as pd
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from typing import Optional

from pandahub.api.dependencies import pandahub

router = APIRouter(prefix="/timeseries", tags=["timeseries"])


# -------------------------------
#  ROUTES
# -------------------------------


class GetTimeSeriesModel(BaseModel):
    filter_document: Optional[dict] = {}
    global_database: Optional[bool] = False
    project_id: Optional[str] = None
    timestamp_range: Optional[tuple] = None
    exclude_timestamp_range: Optional[tuple] = None
    collection_name: Optional[str] = "timeseries"


@router.post("/get_timeseries_from_db")
def get_timeseries_from_db(data: GetTimeSeriesModel, ph=Depends(pandahub)):
    if data.timestamp_range is not None:
        data.timestamp_range = [pd.Timestamp(t) for t in data.timestamp_range]
    ts = ph.get_timeseries_from_db(**data.model_dump())
    return ts.to_json(date_format="iso")


class MultiGetTimeSeriesModel(BaseModel):
    filter_document: Optional[dict] = {}
    global_database: Optional[bool] = False
    project_id: Optional[str] = None
    timestamp_range: Optional[tuple] = None
    exclude_timestamp_range: Optional[tuple] = None
    collection_name: Optional[str] = "timeseries"


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
    filter_document: Optional[dict] = {}
    global_database: Optional[bool] = False
    collection_name: Optional[str] = "timeseries"


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
    project_id: Optional[str] = None
    data_type: Optional[str] = None
    element_type: Optional[str] = None
    netname: Optional[str] = None
    element_index: Optional[int] = None
    global_database: Optional[bool] = False
    collection_name: Optional[str] = "timeseries"
    name: Optional[str] = None


@router.post("/write_timeseries_to_db")
def write_timeseries_to_db(data: WriteTimeSeriesModel, ph=Depends(pandahub)):
    data.timeseries = pd.Series(json.loads(data.timeseries))
    data.timeseries.index = pd.to_datetime(data.timeseries.index)
    ph.write_timeseries_to_db(**data.model_dump())
    return True
