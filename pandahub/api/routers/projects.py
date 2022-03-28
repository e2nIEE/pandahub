from typing import Optional

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from pandahub.api.dependencies import pandahub

router = APIRouter(
    prefix="/projects",
    tags=["projects"]
)


class CreateProject(BaseModel):
    name: str
    settings: Optional[dict] = None


@router.post("/create_project")
def create_project(data: CreateProject, ph=Depends(pandahub)):
    ph.create_project(**data.dict(), realm=ph.user_id)
    return {"message": f"Project {data.name} created !"}


class DeleteProject(BaseModel):
    project_id: str
    i_know_this_action_is_final: bool


@router.post("/delete_project")
def delete_project(data: DeleteProject, ph=Depends(pandahub)):
    ph.delete_project(**data.dict())
    return True


@router.post("/get_projects")
def get_projects(ph=Depends(pandahub)):
    return ph.get_projects()


class Project(BaseModel):
    name: str


@router.post("/project_exists")
def project_exists(data: Project, ph=Depends(pandahub)):
    return ph.project_exists(**data.dict(), realm=ph.user_id)


class SetActiveProjectModel(BaseModel):
    project_name: str


@router.post("/set_active_project")
def set_active_project(data: SetActiveProjectModel, ph=Depends(pandahub)):
    ph.set_active_project(**data.dict())
    print("ACTIVATED PROJECT", ph.active_project)
    return str(ph.active_project["_id"])
