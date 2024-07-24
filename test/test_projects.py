import pandapower.networks as nw
import pandahub
import pandapower as pp
from pandahub.api.internal import settings
from pandahub.lib.database_toolbox import convert_dataframes_to_dicts
from pymongo import DESCENDING
from packaging import version


def test_project_management(ph):
    project = "pytest2"
    if not ph.project_exists(project):
        ph.create_project(project)
    assert ph.project_exists(project)
    ph.set_active_project(project)
    ph.delete_project(i_know_this_action_is_final=True)
    assert not ph.project_exists(project)


def test_upgrade_project():
    class PandaHubV0_2_2(pandahub.PandaHub):
        def create_project(self, name, settings=None, realm=None, metadata=None, project_id=None):
            # if project_id:
            #     self.set_active_project_by_id(project_id)
            if self.project_exists(name, realm):
                raise pandahub.PandaHubError("Project already exists")
            if settings is None:
                 settings = {}
            if metadata is None:
                metadata = {}
            project_data = {"name": name,
                            "realm": realm,
                            "settings": settings,
                            "metadata": metadata}
            if project_id:
                project_data["_id"] = project_id
            if self.user_id is not None:
                 project_data["users"] = {self.user_id: "owner"}
            self.mongo_client["user_management"]["projects"].insert_one(project_data)
            self.set_active_project(name, realm)
            return project_data

        def write_network_to_db(self, net, name, overwrite=True, project_id=None):
            if project_id:
                self.set_active_project_by_id(project_id)
            self.check_permission("write")
            db = self._get_project_database()
            if isinstance(net, pp.pandapowerNet):
                net_type = "power"
            elif isinstance(net, pp.pandapipesNet):
                net_type = "pipe"
            else:
                raise pandahub.PandaHubError("net must be a pandapower or pandapipes object")
            if self._network_with_name_exists(name, db):
                if overwrite:
                    self.delete_net_from_db(name)
                else:
                    raise pandahub.PandaHubError("Network name already exists")
            max_id_network = db["_networks"].find_one(sort=[("_id", -1)])
            _id = 0 if max_id_network is None else max_id_network["_id"] + 1
            dataframes, other_parameters, types = convert_dataframes_to_dicts(net, _id, version.parse("0.2.1"))
            self._write_net_collections_to_db(db, dataframes)

            net_dict = {"_id": _id, "name": name, "dtypes": types,
                        "net_type": net_type,
                        "data": other_parameters}
            db["_networks"].insert_one(net_dict)

        def _write_net_collections_to_db(self, db, collections):
            for key, item in collections.items():
                if len(item) > 0:
                    try:
                        db[key].insert_many(item, ordered= True)
                        db[key].create_index([("net_id", DESCENDING)])
                    except:
                        print("FAILED TO WRITE TABLE", key)
    # we use the implemetation of 0.2.2 to write a net
    oldph = PandaHubV0_2_2(connection_url=settings.MONGODB_URL)

    if oldph.project_exists("pytest"):
        oldph.set_active_project("pytest")
        oldph.delete_project(i_know_this_action_is_final=True)

    oldph.create_project("pytest")
    oldph.set_active_project("pytest")
    net = nw.simple_four_bus_system()
    net.bus.zone = "1"
    net.ext_grid.name = "Slack"
    oldph.write_network_to_db(net, "simple_network")
    # convert the db to latest version
    ph = pandahub.PandaHub(connection_url=settings.MONGODB_URL)
    ph.set_active_project("pytest")
    ph.upgrade_project_to_latest_version()
    # and test if everything went fine
    net2 = ph.get_net_from_db("simple_network")
    assert pp.nets_equal(net, net2, check_dtype=False)


def reset_project(db):
    for cname in db.list_collection_names():
        db.drop_collection(cname)


if __name__ == '__main__':
    test_upgrade_project()







