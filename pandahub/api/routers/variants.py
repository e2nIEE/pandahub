"""FastAPI router for network variant management."""

from fastapi import APIRouter
from pydantic import BaseModel

from pandahub.api.dependencies import PandahubDep

router = APIRouter(prefix="/variants", tags=["variants"])


# -------------------------------
#  ROUTES
# -------------------------------


class GetVariantsModel(BaseModel):
    """Request body for retrieving all variants of a network."""

    project_id: str
    net_id: int | str


@router.post("/get_variants")
def get_variants(data: GetVariantsModel, ph: PandahubDep) -> dict:
    """Return all variants of the specified network, keyed by variant index."""
    ph.set_active_project_by_id(data.project_id)
    variants_collection = ph.get_project_collection("variant")

    variants = variants_collection.find({"net_id": data.net_id}, projection={"_id": 0})
    return {variant.pop("index"): variant for variant in variants}


class CreateVariantModel(BaseModel):
    """Request body for creating a new network variant."""

    project_id: str
    net_id: int
    name: str | None = None
    default_name: str | None = None


class CreateVariantResponseModel(BaseModel):
    """Response body returned after creating a variant."""

    net_id: int
    index: int
    name: str | None = None
    date_created: int
    date_changed: int


@router.post("/create_variant")
def create_variant(data: CreateVariantModel, ph: PandahubDep) -> CreateVariantResponseModel:
    """Create a new variant for the given network and return its metadata."""
    ph.set_active_project_by_id(data.project_id)
    return ph.create_variant(net_id=data.net_id, name=data.name, default_name=data.default_name)


class DeleteVariantModel(BaseModel):
    """Request body for deleting a network variant."""

    project_id: str
    net_id: int | str
    index: int


@router.post("/delete_variant")
def delete_variant(data: DeleteVariantModel, ph: PandahubDep) -> None:
    """Delete a network variant and all its element changes/additions."""
    ph.set_active_project_by_id(data.project_id)
    return ph.delete_variant(data.net_id, data.index)


class UpdateVariantModel(BaseModel):
    """Request body for updating variant metadata fields."""

    project_id: str
    net_id: int | str
    index: int
    data: dict


@router.post("/update_variant")
def update_variant(data: UpdateVariantModel, ph: PandahubDep) -> None:
    """Update fields on an existing network variant."""
    ph.set_active_project_by_id(data.project_id)
    return ph.update_variant(data.net_id, data.index, data.data)
