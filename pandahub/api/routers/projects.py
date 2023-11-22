from typing import Optional, Any

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from pandahub.api.dependencies import pandahub

router = APIRouter(
    prefix="/projects",
    tags=["projects"]
)


# -------------------------
# Projects
# -------------------------

class CreateProject(BaseModel):
    name: str
    settings: Optional[dict] = None

@router.post("/create_project")
def create_project(data: CreateProject, ph=Depends(pandahub)):
    ph.create_project(**data.model_dump(), realm=ph.user_id)
    return {"message": f"Project {data.name} created !"}

class DeleteProject(BaseModel):
    project_id: str
    i_know_this_action_is_final: bool

@router.post("/delete_project")
def delete_project(data: DeleteProject, ph=Depends(pandahub)):
    ph.delete_project(**data.model_dump())
    return True

@router.post("/get_projects")
def get_projects(ph=Depends(pandahub)):
    return ph.get_projects()


class Project(BaseModel):
    name: str

@router.post("/project_exists")
def project_exists(data: Project, ph=Depends(pandahub)):
    return ph.project_exists(**data.model_dump(), realm=ph.user_id)


class SetActiveProjectModel(BaseModel):
    project_name: str

@router.post("/set_active_project")
def set_active_project(data: SetActiveProjectModel, ph=Depends(pandahub)):
    ph.set_active_project(**data.model_dump())
    return str(ph.active_project["_id"])


# -------------------------
# Settings
# -------------------------

class GetProjectSettingsModel(BaseModel):
    project_id: str

@router.post("/get_project_settings")
def get_project_settings(data: GetProjectSettingsModel, ph=Depends(pandahub)):
    settings = ph.get_project_settings(**data.model_dump())
    return settings

class SetProjectSettingsModel(BaseModel):
    project_id: str
    settings: dict

@router.post("/set_project_settings")
def set_project_settings(data: SetProjectSettingsModel, ph=Depends(pandahub)):
    ph.set_project_settings(**data.model_dump())

class SetProjectSettingsValueModel(BaseModel):
    project_id: str
    parameter: str
    value: Any = None

@router.post("/set_project_settings_value")
def set_project_settings_value(data: SetProjectSettingsValueModel, ph=Depends(pandahub)):
    ph.set_project_settings_value(**data.model_dump())

# -------------------------
# Metadata
# -------------------------

class GetProjectMetadataModel(BaseModel):
    project_id: str

@router.post("/get_project_metadata")
def get_project_metadata(data: GetProjectMetadataModel, ph=Depends(pandahub)):
    metadata = ph.get_project_metadata(**data.model_dump())
    return metadata

class SetProjectMetadataModel(BaseModel):
    project_id: str
    metadata: dict

@router.post("/set_project_metadata")
def set_project_metadata(data: SetProjectMetadataModel, ph=Depends(pandahub)):
    return ph.set_project_metadata(**data.model_dump())
