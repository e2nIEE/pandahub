

def test_project_management(ph):
    project = "pytest2"
    if not ph.project_exists(project):
        ph.create_project(project)
    assert ph.project_exists(project)
    ph.set_active_project(project)
    ph.delete_project(i_know_this_action_is_final=True)
    assert not ph.project_exists(project)
