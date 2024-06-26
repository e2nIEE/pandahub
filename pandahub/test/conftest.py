import pytest
from pandahub import PandaHub
from pandahub import PandaHubClient
from pandahub.api.internal import settings

@pytest.fixture(scope="session")
def ph():
    ph = PandaHub(connection_url=settings.MONGODB_URL)

    project_name = "pytest"

    if ph.project_exists(project_name):
        ph.set_active_project(project_name)
        ph.delete_project(i_know_this_action_is_final=True)

    ph.create_project(name=project_name, activate=False)
    ph.set_active_project(project_name)

    yield ph

    ph.delete_project(i_know_this_action_is_final=True)


@pytest.fixture(scope="session")
def phc():
    url = settings.PANDAHUB_SERVER_URL

    if url == "0.0.0.0":
        url = "127.0.0.1"

    phc = PandaHubClient(
        config={
            "url": f"http://{url}:{settings.PANDAHUB_SERVER_PORT}",
            "token": settings.SECRET
        }
    )

    project_name = "pandahubclienttest"

    #if phc.project_exists(project_name):
    #    phc.set_active_project(project_name)
    #    phc.delete_project(i_know_this_action_is_final=True)

    phc.create_project(name=project_name)
    phc.set_active_project(project_name)

    yield phc

    phc.delete_project(i_know_this_action_is_final=True)
