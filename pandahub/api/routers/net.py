from typing import Optional, Any

import pandapower as pp
from fastapi import APIRouter, Depends
from pydantic import BaseModel

from pandahub.api.dependencies import pandahub

router = APIRouter(
    prefix="/net",
    tags=["net"]
)


# -------------------------
# Net handling
# -------------------------

class GetNetFromDB(BaseModel):
    project_id: str
    name: str
    include_results: bool
    only_tables: Optional[list] = None


@router.post("/get_net_from_db")
def get_net_from_db(data: GetNetFromDB, ph=Depends(pandahub)):
    net = ph.get_net_from_db(**data.dict())
    return pp.to_json(net)


class WriteNetwork(BaseModel):
    project_id: str
    net: str
    name: str
    overwrite: Optional[bool] = True


@router.post("/write_network_to_db")
def write_network_to_db(data: WriteNetwork, ph=Depends(pandahub)):
    print("WRITING NET", data)
    params = data.dict()
    params["net"] = pp.from_json_string(params["net"])
    ph.write_network_to_db(**params)


# -------------------------
# Element handling
# -------------------------

class GetNetValueModel(BaseModel):
    project_id: str
    net_name: str
    element: str
    element_index: int
    parameter: str

@router.post("/get_net_value_from_db")
def get_net_value_from_db(data: GetNetValueModel, ph=Depends(pandahub)):
    return ph.get_net_value_from_db(**data.dict())

class SetNetValueModel(BaseModel):
    project_id: str
    net_name: str
    element: str
    element_index: int
    parameter: str
    value: Any

@router.post("/set_net_value_in_db")
def set_net_value_in_db(data: SetNetValueModel, ph=Depends(pandahub)):
    return ph.set_net_value_in_db(**data.dict())

class CreateElementModel(BaseModel):
    project_id: str
    net_name: str
    element: str
    element_index: int
    data: dict

@router.post("/create_element_in_db")
def create_element_in_db(data: CreateElementModel, ph=Depends(pandahub)):
    return ph.create_element_in_db(**data.dict())

class CreateElementsModel(BaseModel):
    project_id: str
    net_name: str
    element_type: str
    elements_data: list[dict]

@router.post("/create_elements_in_db")
def create_elements_in_db(data: CreateElementsModel, ph=Depends(pandahub)):
    return ph.create_elements_in_db(**data.dict())

class DeleteElementModel(BaseModel):
    project_id: str
    net_name: str
    element: str
    element_index: int

@router.post("/delete_net_element")
def delete_net_element(data: DeleteElementModel, ph=Depends(pandahub)):
    return ph.delete_net_element(**data.dict())
