import pytest
from pandahub import PandaHub
from pandahub.api.internal import settings

@pytest.fixture(scope="session")
def ph():
    ph = PandaHub(connection_url=settings.MONGODB_URL)

    project_name = "pytest"

    if ph.project_exists(project_name):
        ph.set_active_project(project_name)
        ph.delete_project(i_know_this_action_is_final=True)

    ph.create_project(name=project_name)

    yield ph

    ph.delete_project(i_know_this_action_is_final=True)


