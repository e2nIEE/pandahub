"""FastAPI router for pandapower/pandapipes network CRUD operations."""

from typing import Any

import pandapower as pp
from fastapi import APIRouter
from pydantic import BaseModel

from pandahub.api.dependencies import PandahubDep

router = APIRouter(prefix="/net", tags=["net"])


# -------------------------
# Net handling
# -------------------------


class GetNetFromDB(BaseModel):
    """Request body for retrieving a network by name."""

    project_id: str
    name: str
    include_results: bool
    only_tables: list | None = None


@router.post("/get_net_from_db")
def get_net_from_db(data: GetNetFromDB, ph: PandahubDep) -> str:
    """Return a network from the database serialised as a pandapower JSON string."""
    net = ph.get_network_by_name(**data.model_dump())
    return pp.to_json(net)


class WriteNetwork(BaseModel):
    """Request body for writing a network to the database."""

    project_id: str
    net: str
    name: str
    overwrite: bool | None = True


@router.post("/write_network_to_db")
def write_network_to_db(data: WriteNetwork, ph: PandahubDep) -> None:
    """Write a pandapower network (JSON string) to the database."""
    params = data.model_dump()
    params["net"] = pp.from_json_string(params["net"])
    ph.write_network_to_db(**params)


# -------------------------
# Element CRUD
# -------------------------


class BaseCRUDModel(BaseModel):
    """Base fields shared by all element CRUD request bodies."""

    project_id: str
    net_id: int | str
    element_type: str


class GetNetValueModel(BaseCRUDModel):
    """Request body for reading a single element field value."""

    element_index: int
    parameter: str


@router.post("/get_net_value_from_db")
def get_net_value_from_db(data: GetNetValueModel, ph: PandahubDep) -> Any:  # noqa: ANN401
    """Return the value of a single field from one network element."""
    return ph.get_net_value_from_db(**data.model_dump())


class SetNetValueModel(BaseCRUDModel):
    """Request body for updating a single element field value."""

    element_index: int
    parameter: str
    value: Any = None


@router.post("/set_net_value_in_db")
def set_net_value_in_db(data: SetNetValueModel, ph: PandahubDep) -> dict | None:
    """Set a single field value on one network element."""
    return ph.set_net_value_in_db(**data.model_dump())


class CreateElementModel(BaseCRUDModel):
    """Request body for creating a single network element."""

    element_index: int
    element_data: dict


@router.post("/create_element")
def create_element_in_db(data: CreateElementModel, ph: PandahubDep) -> dict:
    """Create a single network element in the database."""
    return ph.create_element(**data.model_dump())


class CreateElementsModel(BaseCRUDModel):
    """Request body for creating multiple network elements of the same type."""

    elements_data: list[dict[str, Any]]


@router.post("/create_elements")
def create_elements_in_db(data: CreateElementsModel, ph: PandahubDep) -> list[dict]:
    """Create multiple network elements of the same type in the database."""
    return ph.create_elements(**data.model_dump())


class DeleteElementModel(BaseCRUDModel):
    """Request body for deleting a single network element."""

    element_index: int


@router.post("/delete_element")
def delete_net_element(data: DeleteElementModel, ph: PandahubDep) -> dict:
    """Delete a single network element from the database."""
    return ph.delete_element(**data.model_dump())


class DeleteElementsModel(BaseCRUDModel):
    """Request body for deleting multiple network elements."""

    element_indexes: list[int]


@router.post("/delete_elements")
def delete_net_elements(data: DeleteElementsModel, ph: PandahubDep) -> list[dict]:
    """Delete multiple network elements of the same type from the database."""
    return ph.delete_elements(**data.model_dump())
