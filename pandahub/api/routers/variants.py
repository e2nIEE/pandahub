import json

import pandas as pd
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from pydantic.typing import Optional

from pandahub.api.dependencies import pandahub

router = APIRouter(
    prefix="/variants",
    tags=["variants"]
)


# -------------------------------
#  ROUTES
# -------------------------------

class GetVariantsModel(BaseModel):
    project_id: str


@router.post("/get_variants")
def get_variants(data: GetVariantsModel, ph=Depends(pandahub)):
    project_id = data.project_id
    ph.set_active_project_by_id(project_id)
    db = ph._get_project_database()

    variants = db["net_variant"].find({}, projection={"_id": 0})
    response = {}
    for var in variants:
        response[var.pop("index")] = var
    return response

