import pytest
from pandahub import PandaHub
from pandahub import PandaHubClient
from pandahub.client.user_management import _login

@pytest.fixture(scope="session")
def ph():
    ph = PandaHub(connection_url="mongodb://localhost:27017")

    project_name = "pytest"

    if ph.project_exists(project_name):
        ph.set_active_project(project_name)
        ph.delete_project(i_know_this_action_is_final=True)

    ph.create_project(project_name)
    ph.set_active_project(project_name)

    yield ph

    ph.delete_project(i_know_this_action_is_final=True)


@pytest.fixture(scope="session")
def phc():
    phc = PandaHubClient()

    project_name = "pandahubclienttest"

    #if phc.project_exists(project_name):
    #    phc.set_active_project(project_name)
    #    phc.delete_project(i_know_this_action_is_final=True)

    phc.create_project(project_name)
    phc.set_active_project(project_name)

    yield phc

    phc.delete_project(i_know_this_action_is_final=True)
