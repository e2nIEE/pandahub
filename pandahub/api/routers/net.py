from typing import Optional

import pandapower as pp
from fastapi import APIRouter, Depends
from pydantic import BaseModel

from pandahub.api.dependencies import pandahub

router = APIRouter(
    prefix="/net",
    tags=["net"]
)


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
    ph.write_network_to_db(**data.dict())
