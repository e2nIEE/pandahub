"""FastAPI router for project management operations."""

from typing import Any

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from pandahub import PandaHub
from pandahub.api.dependencies import pandahub

router = APIRouter(prefix="/projects", tags=["projects"])


# -------------------------
# Projects
# -------------------------


class CreateProject(BaseModel):
    """Request body for creating a new project."""

    name: str
    settings: dict | None = None


@router.post("/create_project")
def create_project(data: CreateProject, ph: PandaHub = Depends(pandahub)) -> dict:
    """Create a new project and return a confirmation message."""
    ph.create_project(**data.model_dump(), realm=ph.user_id)
    return {"message": f"Project {data.name} created !"}


class DeleteProject(BaseModel):
    """Request body for deleting a project."""

    project_id: str
    i_know_this_action_is_final: bool


@router.post("/delete_project")
def delete_project(data: DeleteProject, ph: PandaHub = Depends(pandahub)) -> bool:
    """Delete a project permanently and return True on success."""
    ph.delete_project(**data.model_dump())
    return True


@router.post("/get_projects")
def get_projects(ph: PandaHub = Depends(pandahub)) -> list:
    """Return all projects the current user has access to."""
    return ph.get_projects()


class Project(BaseModel):
    """Request body for checking project existence."""

    name: str


@router.post("/project_exists")
def project_exists(data: Project, ph: PandaHub = Depends(pandahub)) -> bool:
    """Return True if a project with the given name exists in the user's realm."""
    return ph.project_exists(**data.model_dump(), realm=ph.user_id)


class SetActiveProjectModel(BaseModel):
    """Request body for activating a project by name."""

    project_name: str


@router.post("/set_active_project")
def set_active_project(data: SetActiveProjectModel, ph: PandaHub = Depends(pandahub)) -> str:
    """Activate a project by name and return its id as a string."""
    ph.set_active_project(**data.model_dump())
    return str(ph.active_project["_id"])


# -------------------------
# Settings
# -------------------------


class GetProjectSettingsModel(BaseModel):
    """Request body for retrieving project settings."""

    project_id: str


@router.post("/get_project_settings")
def get_project_settings(data: GetProjectSettingsModel, ph: PandaHub = Depends(pandahub)) -> dict:
    """Return the settings dict for the specified project."""
    settings = ph.get_project_settings(**data.model_dump())
    return settings


class SetProjectSettingsModel(BaseModel):
    """Request body for updating project settings."""

    project_id: str
    settings: dict


@router.post("/set_project_settings")
def set_project_settings(data: SetProjectSettingsModel, ph: PandaHub = Depends(pandahub)) -> None:
    """Merge the provided settings dict into the project's existing settings."""
    ph.set_project_settings(**data.model_dump())


class SetProjectSettingsValueModel(BaseModel):
    """Request body for setting a single project setting value."""

    project_id: str
    parameter: str
    value: Any = None


@router.post("/set_project_settings_value")
def set_project_settings_value(data: SetProjectSettingsValueModel, ph: PandaHub = Depends(pandahub)) -> None:
    """Set a single project setting by dot-notation parameter name."""
    ph.set_project_settings_value(**data.model_dump())


# -------------------------
# Metadata
# -------------------------


class GetProjectMetadataModel(BaseModel):
    """Request body for retrieving project metadata."""

    project_id: str


@router.post("/get_project_metadata")
def get_project_metadata(data: GetProjectMetadataModel, ph: PandaHub = Depends(pandahub)) -> dict:
    """Return the metadata dict for the specified project."""
    metadata = ph.get_project_metadata(**data.model_dump())
    return metadata


class SetProjectMetadataModel(BaseModel):
    """Request body for updating project metadata."""

    project_id: str
    metadata: dict


@router.post("/set_project_metadata")
def set_project_metadata(data: SetProjectMetadataModel, ph: PandaHub = Depends(pandahub)) -> None:
    """Merge the provided metadata dict into the project's existing metadata."""
    return ph.set_project_metadata(**data.model_dump())
