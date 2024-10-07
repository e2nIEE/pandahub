import json

from fastapi import APIRouter, Depends
from pydantic import BaseModel

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
    net_id: int | str

@router.post("/get_variants")
def get_variants(data: GetVariantsModel, ph=Depends(pandahub)):
    ph.set_active_project_by_id(data.project_id)
    variants_collection = ph.get_project_database("variant")

    variants = variants_collection.find({"net_id": data.net_id}, projection={"_id": 0})
    return {variant.pop("index"): variant for variant in variants}


class CreateVariantModel(BaseModel):
    project_id: str
    net_id: int
    name: str | None = None
    default_name: str | None = None


class CreateVariantResponseModel(BaseModel):
    net_id: int
    index: int
    name: str | None = None
    date_created: int
    date_changed: int


@router.post("/create_variant")
def create_variant(data: CreateVariantModel, ph=Depends(pandahub)) -> CreateVariantResponseModel:
    ph.set_active_project_by_id(data.project_id)
    return ph.create_variant(net_id=data.net_id, name=data.name, default_name=data.default_name)

class DeleteVariantModel(BaseModel):
    project_id: str
    net_id: int | str
    index: int

@router.post("/delete_variant")
def delete_variant(data: DeleteVariantModel, ph=Depends(pandahub)):
    project_id = data.project_id
    ph.set_active_project_by_id(project_id)
    return ph.delete_variant(data.net_id, data.index)

class UpdateVariantModel(BaseModel):
    project_id: str
    net_id: int | str
    index: int
    data: dict

@router.post("/update_variant")
def update_variant(data: UpdateVariantModel, ph=Depends(pandahub)):
    project_id = data.project_id
    ph.set_active_project_by_id(project_id)
    return ph.update_variant(data.net_id, data.index, data.data)
