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
    net = ph.get_net_from_db(**data.model_dump())
    return pp.to_json(net)


class WriteNetwork(BaseModel):
    project_id: str
    net: str
    name: str
    overwrite: Optional[bool] = True


@router.post("/write_network_to_db")
def write_network_to_db(data: WriteNetwork, ph=Depends(pandahub)):
    params = data.model_dump()
    params["net"] = pp.from_json_string(params["net"])
    ph.write_network_to_db(**params)


# -------------------------
# Element CRUD
# -------------------------

class BaseCRUDModel(BaseModel):
    project_id: str
    net: str
    element_type: str

class GetNetValueModel(BaseCRUDModel):
    element_index: int
    parameter: str

@router.post("/get_net_value_from_db")
def get_net_value_from_db(data: GetNetValueModel, ph=Depends(pandahub)):
    return ph.get_net_value_from_db(**data.model_dump())

class SetNetValueModel(BaseCRUDModel):
    element_index: int
    parameter: str
    value: Any = None

@router.post("/set_net_value_in_db")
def set_net_value_in_db(data: SetNetValueModel, ph=Depends(pandahub)):
    return ph.set_net_value_in_db(**data.model_dump())

class CreateElementModel(BaseCRUDModel):
    element_index: int
    element_data: dict

@router.post("/create_element")
def create_element_in_db(data: CreateElementModel, ph=Depends(pandahub)):
    return ph.create_element(**data.model_dump())

class CreateElementsModel(BaseCRUDModel):
    elements_data: list[dict[str,Any]]

@router.post("/create_elements")
def create_elements_in_db(data: CreateElementsModel, ph=Depends(pandahub)):
    return ph.create_elements(**data.model_dump())

class DeleteElementModel(BaseCRUDModel):
    element_index: int

@router.post("/delete_element")
def delete_net_element(data: DeleteElementModel, ph=Depends(pandahub)):
    return ph.delete_element(**data.model_dump())

class DeleteElementsModel(BaseCRUDModel):
    element_indexes: list[int]

@router.post("/delete_elements")
def delete_net_elements(data: DeleteElementsModel, ph=Depends(pandahub)):
    return ph.delete_elements(**data.model_dump())

### deprecated routes
@router.post("/create_element_in_db")
def create_element_in_db(*args, **kwargs):
    raise RuntimeError("create_element_in_db was deprecated - use create_element instead!")

@router.post("/create_elements_in_db")
def create_elements_in_db(*args, **kwargs):
    raise RuntimeError("create_elements_in_db was deprecated - use create_elements instead!")


@router.post("/delete_net_element")
def delete_net_element(*args, **kwargs):
    raise RuntimeError("delete_net_element was deprecated - use delete_element instead!")
