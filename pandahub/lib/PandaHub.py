# -*- coding: utf-8 -*-

import pandas as pd
import pandapower as pp
import pandapipes as pps
from pandahub.lib.database_toolbox import create_timeseries_document, convert_timeseries_to_subdocuments, convert_dataframes_to_dicts
from pandahub.api.internal import settings
from pymongo import MongoClient, ReplaceOne, DESCENDING
from pandapower.io_utils import JSONSerializableClass
from bson.objectid import ObjectId
from pydantic.types import UUID4
from typing import Optional
import logging
logger = logging.getLogger(__name__)

# -------------------------
# Exceptions
# -------------------------

class PandaHubError(Exception):
    def __init__(self, message, status_code=400):
        self.status_code = status_code
        super().__init__(message)


# -------------------------
# PandaHub
# -------------------------

class PandaHub:
    permissions = {
        "read": ["owner", "developer", "guest"],
        "write": ["owner", "developer"],
        "user_management": ["owner"]
    }

    # -------------------------
    # Initialization
    # -------------------------

    def __init__(self, connection_url=None, check_server_available=False, user_id=None):
        if connection_url is None:
            connection_url = settings.MONGODB_URL
        if not connection_url.startswith('mongodb://'):
            raise PandaHubError("Connection URL needs to point to a mongodb instance: 'mongodb://..'")
        self.mongo_client = MongoClient(host=connection_url, uuidRepresentation="standard")
        self.active_project = None
        self.user_id = user_id
        if check_server_available:
            self.server_is_available()


    # -------------------------
    # Database connection checks
    # -------------------------

    def server_is_available(self):
        """
        Check if the MongoDB server is available
        """
        try:
            self.mongo_client.server_info()
            logger.debug("connected to mongoDB server %s" % self.get_masked_mongodb_url())
            return True
        except ServerSelectionTimeoutError:
            logger.error("could not connect to mongoDB server %s" % self.get_masked_mongodb_url())
            return False

    def check_connection_status(self):
        """
        Checks if the database is accessible
        """
        try:
            status = self.find({}, collection_name="__connection_test_collection")
            if status == []:
                return "ok"
        except (ServerSelectionTimeoutError, timeout) as e:
            return "connection timeout"


    # -------------------------
    # Permission check
    # -------------------------

    def check_permission(self, permission):
        if self.active_project is None:
            raise PandaHubError("No project is activated")
        if not self.has_permission(permission):
            raise PandaHubError("You don't have {} rights on this project".format(permission), 403)

    def has_permission(self, permission):
        if not "users" in self.active_project:
            return True

        user = self._get_user()

        if user is None:
            return False

        if user["is_superuser"]:
            return True

        users = self.active_project["users"]
        if self.user_id not in users:
            return False
        role = users[self.user_id]
        return role in self.permissions[permission]

    def get_permissions_by_role(self, role):
        permissions = []
        for perm, roles in self.permissions.items():
            if role in roles:
                permissions.append(perm)
        return permissions


    # -------------------------
    # User handling
    # -------------------------

    def get_user_by_email(self, email):
        user_mgmnt_db = self.mongo_client["user_management"]
        user = user_mgmnt_db["users"].find_one({"email": email})
        if user is None:
            return None
        if str(user["id"]) != self.user_id:
            self.check_permission("user_management")
        return user

    def _get_user(self):
        user_mgmnt_db = self.mongo_client["user_management"]
        user = user_mgmnt_db["users"].find_one(
            {"id": UUID4(self.user_id)}, projection={"_id": 0, "hashed_password": 0}
        )
        return user


    # -------------------------
    # Project handling
    # -------------------------

    def create_project(self, name, settings=None, realm=None, metadata=None, project_id=None):
        if project_id:
            self.set_active_project_by_id(project_id)
        if self.project_exists(name, realm):
            raise PandaHubError("Project already exists")
        if settings is None:
             settings = {}
        if metadata is None:
            metadata = {}

        project_data = {"name": name, "realm": realm, "settings": settings, "metadata": metadata}
        if self.user_id is not None:
             project_data["users"] = {self.user_id: "owner"}
        self.mongo_client["user_management"]["projects"].insert_one(project_data)
        self.set_active_project(name, realm)
        return project_data

    def delete_project(self, i_know_this_action_is_final=False, project_id=None):
        if project_id:
            self.set_active_project_by_id(project_id)
        project_id = self.active_project["_id"]
        self.check_permission("write")
        if not i_know_this_action_is_final:
            raise PandaHubError("Calling this function will delete the whole project and all the nets stored within. It can not be reversed. Add 'i_know_this_action_is_final=True' to confirm.")
        self.mongo_client.drop_database(str(project_id))
        self.mongo_client.user_management.projects.delete_one({"_id": project_id})
        self.active_project = None

    def get_projects(self):
        if self.user_id is not None:
            user = self._get_user()
            if user["is_superuser"]:
                filter_dict = {}
            else:
                filter_dict = {"users.{}".format(self.user_id): {"$exists": True}}
        else:
            filter_dict = {"users":  {"$exists": False}}
        db = self.mongo_client["user_management"]
        projects = list(db["projects"].find(filter_dict))
        return [{
            "id": str(p["_id"]),
            "name": p["name"],
            "realm": str(p["realm"]),
            "settings": p["settings"],
            "locked": p.get("locked"),
            "locked_by": p.get("locked_by"),
            "permissions": self.get_permissions_by_role(p.get("users").get(self.user_id)) if self.user_id else None
        } for p in projects]

    def set_active_project(self, project_name, realm=None):
        projects = self.get_projects()
        active_projects = [project for project in projects if project["name"] == project_name]
        if len(active_projects) == 0:
            raise PandaHubError("Project not found!", 404)
        elif len(active_projects) > 1:
            raise PandaHubError("Multiple projects found!")
        else:
            project_id = active_projects[0]["id"]
            self.set_active_project_by_id(project_id)

    def set_active_project_by_id(self, project_id):
        self.active_project = self._get_project_document({"_id": ObjectId(project_id)})

    def rename_project(self, project_name):
        self.has_permission("write")
        project_collection = self.mongo_client["user_management"].projects
        realm = self.active_project["realm"]
        if self.project_exists(project_name, realm):
            raise PandaHubError("Can't rename - project with this name already exists")
        project_collection.find_one_and_update({"_id": self.active_project["_id"]},
                                               {"$set": {"name": project_name}})
        self.set_active_project(project_name, realm)

    def change_realm(self, realm):
        self.has_permission("write")
        project_collection = self.mongo_client["user_management"].projects
        project_name = self.active_project["name"]
        if self.project_exists(active_project_name, realm):
            raise PandaHubError("Can't change realm - project with this name already exists")
        project_collection.find_one_and_update({"_id": self.active_project["_id"]},
                                               {"$set": {"realm": realm}})
        self.set_active_project(project_name, realm)

    def lock_project(self):
        db = self.mongo_client["user_management"]["projects"]
        result = db.update_one(
            {"_id": self.active_project["_id"],},
            {"$set": {"locked": True, "locked_by": self.user_id}}
        )
        return result.acknowledged and result.modified_count > 0

    def unlock_projects(self):
        db = self.mongo_client["user_management"]["projects"]
        return db.update_many(
            {"locked_by": self.user_id}, {"$set": {"locked": False, "locked_by": None}}
        )

    def project_exists(self, project_name=None, realm=None):
        project_collection = self.mongo_client["user_management"].projects
        project = project_collection.find_one({"name": project_name, "realm": realm})
        return project is not None

    def _get_project_document(self, filter_dict: dict) -> Optional[dict]:
        project_collection = self.mongo_client["user_management"].projects
        projects = list(project_collection.find(filter_dict))
        if len(projects) == 0: #project doesn't exist
            return None
        if len(projects) > 1:
            raise PandaHubError("Duplicate Project detected. This should never happen if you create projects through the API. Remove duplicate projects manually in the database.")
        project_doc = projects[0]
        user = self._get_user()
        if "users" not in project_doc:
            return project_doc #project is not user protected
        elif not user["is_superuser"] and self.user_id not in project_doc["users"].keys():
            raise PandaHubError("You don't have rights to access this project", 403)
        elif project_doc.get("locked") and project_doc.get("locked_by") != self.user_id:
            raise PandaHubError("Project is locked by another user")
        else:
            return project_doc

    def _get_project_database(self):
        return self.mongo_client[str(self.active_project["_id"])]

    def _get_global_database(self):
        return self.mongo_client["global_data"]


    # -------------------------
    # Project settings and metadata
    # -------------------------

    def get_project_settings(self, project_id=None):
        if project_id:
            self.set_active_project_by_id(project_id)
        self.check_permission("read")
        return self.active_project["settings"]

    def set_project_settings(self, settings, project_id=None):
        if project_id:
            self.set_active_project_by_id(project_id)
        self.check_permission("write")
        _id = self.active_project["_id"]
        new_settings = {**self.active_project["settings"], **settings}
        project_collection = self.mongo_client["user_management"]["projects"]
        project_collection.find_one_and_update({"_id": _id}, {"$set": {"settings": new_settings}})
        self.active_project["settings"] = new_settings

    def get_project_metadata(self, project_id=None):
        if project_id:
            self.set_active_project_by_id(project_id)
        self.check_permission("read")
        return self.active_project.get("metadata") or dict()

    def set_project_metadata(self, metadata: dict, project_id=None):
        if project_id:
            self.set_active_project_by_id(project_id)
        self.check_permission("write")
        project_data = self.active_project
        if "metadata" in project_data.keys():
            new_metadata = {**project_data["metadata"], **metadata}
        else:
            new_metadata = metadata

        # Workaround until mongo 5.0
        update_metadata = dict()
        for key, val in new_metadata.items():
            if val:
                update_metadata[key] = val

        self.mongo_client.user_management.projects.update_one(
            {"_id": project_data['_id']},
            [
                {"$unset": "metadata"}, # deletion needed because set won't delete not existing fields
                {"$set": {"metadata": update_metadata}}
            ]
        )
        self.active_project["metadata"] = update_metadata


    # -------------------------
    # Project user management
    # -------------------------

    def get_project_users(self):
        self.check_permission("user_management")
        project_users = self.active_project["users"]
        users = self.mongo_client["user_management"]["users"].find(
            {"id": { "$in": [UUID4(user_id) for user_id in project_users.keys()] }}
        )
        enriched_users = []
        for user in users:
            enriched_users.append({
                "email": user["email"],
                "role": project_users[str(user["id"])]
            })
        return enriched_users

    def add_user_to_project(self, email, role):
        self.check_permission("user_management")
        user = self.get_user_by_email(email)
        if user is None:
            return
        user_id = user["id"]
        self.mongo_client["user_management"]["projects"].update_one(
            {"_id": self.active_project["_id"]},
            {"$set": {f"users.{user_id}": role}}
        )
        return user

    def change_project_user_role(self, email, new_role):
        self.check_permission("user_management")
        user = self.get_user_by_email(email)
        if user is None:
            return
        user_id = user["id"]
        self.mongo_client["user_management"]["projects"].update_one(
            {"_id": self.active_project["_id"]},
            {"$set": {f"users.{user_id}": new_role}}
        )

    def remove_user_from_project(self, email):
        user = self.get_user_by_email(email)
        if user is None:
            return
        # check permission only if the user tries to remove a different user to
        # allow leaving a project with just 'read' permission
        if str(user["id"]) != self.user_id:
            self.check_permission("user_management")
        user_id = user["id"]
        self.mongo_client["user_management"]["projects"].update_one(
            {"_id": self.active_project["_id"]},
            {"$unset": {f"users.{user_id}": ""}}
        )


    # -------------------------
    # Net handling
    # -------------------------

    def get_net_from_db(self, name, include_results=True, only_tables=None, project_id=None):
        if project_id:
            self.set_active_project_by_id(project_id)
        self.check_permission("read")
        db = self._get_project_database()
        _id = self._get_id_from_name(name, db)
        if _id is None:
            return None
        return self.get_net_from_db_by_id(_id, include_results, only_tables)

    def get_net_from_db_by_id(self, id, include_results=True, only_tables=None):
        self.check_permission("read")
        return self._get_net_from_db_by_id(id, include_results, only_tables)

    def get_subnet_from_db(self, name, bus_filter=None, bus_geodata_filter=None,
                           include_results=True, add_edge_branches=True):
        self.check_permission("read")
        db = self._get_project_database()
        _id = self._get_id_from_name(name, db)
        if _id is None:
            return None

        meta = self._get_network_metadata(db, _id)

        net = pp.create_empty_network()

        if bus_geodata_filter is not None:
            self._add_element_from_collection(net, db, "bus_geodata", _id, bus_geodata_filter)
            self._add_element_from_collection(net, db, "bus", _id, {"index": {"$in": net.bus_geodata.index.tolist()}})

        # Add buses with filter
        if bus_filter is not None:
            self._add_element_from_collection(net, db, "bus", _id, bus_filter)
        buses = net.bus.index.tolist()

        branch_operator = "$or" if add_edge_branches else "$and"
        # Add branch elements connected to at least one bus
        self._add_element_from_collection(net, db, "line", _id,
                                          {branch_operator: [
                                              {"from_bus": {"$in": buses}},
                                              {"to_bus": {"$in": buses}}]})
        self._add_element_from_collection(net, db,  "trafo", _id,
                                          {branch_operator: [
                                              {"hv_bus": {"$in": buses}},
                                              {"lv_bus": {"$in": buses}}]})
        self._add_element_from_collection(net, db, "trafo3w", _id,
                                          {branch_operator: [
                                              {"hv_bus": {"$in": buses}},
                                              {"mv_bus": {"$in": buses}},
                                              {"lv_bus": {"$in": buses}}]})

        self._add_element_from_collection(net, db, "switch", _id,
                                          {"$and": [
                                              {"et": "b"},
                                              {branch_operator: [
                                                  {"bus": {"$in": buses}},
                                                  {"element": {"$in": buses}}
                                               ]}
                                              ]
                                           })
        if add_edge_branches:
            # Add buses on the other side of the branches
            branch_buses = set(net.trafo.hv_bus.values) | set(net.trafo.lv_bus.values) | \
                    set(net.line.from_bus) | set(net.line.to_bus) | \
                    set(net.trafo3w.hv_bus.values) | set(net.trafo3w.mv_bus.values) | \
                    set(net.trafo3w.lv_bus.values) | set(net.switch.bus) | set(net.switch.element)
            branch_buses_outside = [int(b) for b in branch_buses - set(buses)]
            self._add_element_from_collection(net, db, "bus", _id,
                                              {"index": {"$in": branch_buses_outside}})
            buses = net.bus.index.tolist()

        switch_filter = {"$or": [
                            {"$and": [
                                 {"et": "t"},
                                 {"element": {"$in": net.trafo.index.tolist()}}
                                 ]
                                },
                            {"$and": [
                                 {"et": "l"},
                                 {"element": {"$in": net.line.index.tolist()}}
                                 ]
                                },
                            {"$and": [
                                 {"et": "t3"},
                                 {"element": {"$in": net.trafo3w.index.tolist()}}
                                 ]
                                }
                            ]
                        }
        self._add_element_from_collection(net, db,"switch", _id, switch_filter)

        #add node elements
        node_elements = ["load", "sgen", "gen", "ext_grid", "shunt", "xward", "ward", "motor", "storage"]
        branch_elements = ["trafo", "line", "trafo3w", "switch", "impedance"]
        all_elements = node_elements + branch_elements + ["bus"]

        #add all node elements that are connected to buses within the network
        for element in node_elements:
            filter = {"bus": {"$in": buses}}
            self._add_element_from_collection(net, db, element, _id,
                                              filter=filter,
                                              include_results=include_results)

        #add all other collections
        collection_names = self._get_net_collections(db)
        for collection in collection_names:
            #skip all element tables that we have already added
            if collection in all_elements:
                continue
            #for tables that share an index with an element (e.g. load->res_load) load only relevant entries
            for element in all_elements:
                if collection.startswith(element + "_") or collection.startswith("res_" + element):
                    filter = {"index": {"$in": net[element].index.tolist()}}
                    break
            else:
                #all other tables (e.g. std_types) are loaded without filter
                filter = None
            self._add_element_from_collection(net, db, collection, _id,
                                              filter=filter,
                                              include_results=include_results)
        net.update(meta["data"])
        return net

    def write_network_to_db(self, net, name, overwrite=True, project_id=None):
        if project_id:
            self.set_active_project_by_id(project_id)
        self.check_permission("write")
        db = self._get_project_database()
        if isinstance(net, pp.pandapowerNet):
            net_type = "power"
        elif isinstance(net, pps.pandapipesNet):
            net_type = "pipe"
        else:
            raise PandaHubError("net must be a pandapower or pandapipes object")
        if self._network_with_name_exists(name, db):
            if overwrite:
                self.delete_net_from_db(name)
            else:
                raise PandaHubError("Network name already exists")
        max_id_network = db["_networks"].find_one(sort=[("_id", -1)])
        _id = 0 if max_id_network is None else max_id_network["_id"] + 1
        dataframes, other_parameters, types = convert_dataframes_to_dicts(net, _id)
        self._write_net_collections_to_db(db, dataframes)

        net_dict = {"_id": _id, "name": name, "dtypes": types,
                    "net_type": net_type,
                    "data": other_parameters}
        db["_networks"].insert_one(net_dict)

    def delete_net_from_db(self, name):
        self.check_permission("write")
        db = self._get_project_database()
        _id = self._get_id_from_name(name, db)
        if _id is None:
            raise PandaHubError("Network does not exist", 404)
        collection_names = self._get_net_collections(db) #TODO
        for collection_name in collection_names:
            db[collection_name].delete_many({'net_id': _id})
        db["_networks"].delete_one({"_id": _id})

    def network_with_name_exists(self, name):
        self.check_permission("read")
        db = self._get_project_database()
        return self._network_with_name_exists(name, db)

    def load_networks_meta(self, networks, load_area=False):
        self.check_permission("read")
        db = self._get_project_database()
        fi = {"name": {"$in": networks}}
        proj = {"net": 0}
        if not load_area:
            proj["area_geojson"] = 0
        nets = pd.DataFrame(list(db.find(fi,rojection=proj)))
        return nets

    def _write_net_collections_to_db(self, db, collections):
        for key, item in collections.items():
            if len(item) > 0:
                try:
                    db[key].insert_many(item, ordered= True)
                    db[key].create_index([("net_id", DESCENDING)])
                except:
                    print("FAILED TO WRITE TABLE", key)

    def _get_metadata_from_name(self, name, db):
        return list(db["_networks"].find({"name": name}))

    def _get_id_from_name(self, name, db):
        metadata = self._get_metadata_from_name(name, db)
        if len(metadata) > 1:
            raise PandaHubError("Duplicate Network!")
        return None if len(metadata) == 0 else metadata[0]["_id"]

    def _network_with_name_exists(self, name, db):
        return self._get_id_from_name(name, db) is not None


    def _get_net_collections(self, db):
        all_collection_names = db.list_collection_names()
        return [name for name in all_collection_names if
                    not name.startswith("_") and
                    not name=="timeseries"]


    def _get_net_from_db_by_id(self, id, include_results=True, only_tables=None):
        db = self._get_project_database()
        meta = self._get_network_metadata(db, id)
        if meta["net_type"] == "power":
            net = pp.create_empty_network()
        elif meta["net_type"] == "pipe":
            net = pps.create_empty_network()
        collection_names = self._get_net_collections(db)
        for collection_name in collection_names:
            self._add_element_from_collection(net, db, collection_name, id,
                                              include_results=include_results,
                                              only_tables=only_tables)
        net.update(meta["data"])
        return net

    def _get_network_metadata(self, db, net_id):
        return db["_networks"].find_one({"_id": net_id})



    def _add_element_from_collection(self, net, db, element, net_id,
                                     filter=None, include_results=True,
                                     only_tables=None):
        if only_tables is not None and not element in only_tables:
            return
        if not include_results and element.startswith("res_"):
            return
        filter_dict = {"net_id": net_id}
        if filter is not None:
            filter_dict = {**filter_dict, **filter}
        data = list(db[element].find(filter_dict))
        if len(data) == 0:
            return
        dtypes = db["_networks"].find_one({"_id": net_id})["dtypes"]
        df = pd.DataFrame.from_records(data, index="index")
        if element in dtypes:
            df = df.astype(dtypes[element])
        df.index.name = None
        df.drop(columns=["_id", "net_id"], inplace=True)
        df.sort_index(inplace=True)
        if "object" in df.columns:
            df["object"] = df["object"].apply(lambda obj: JSONSerializableClass.from_dict(obj))
        if not element in net or net[element].empty:
            net[element] = df
        else:
            new_rows = set(df.index) - set(net[element].index)
            if new_rows:
                net[element] = pd.concat([net[element], df.loc[new_rows]])


    # -------------------------
    # Net element handling
    # -------------------------

    def get_net_value_from_db(self, net_name, element, element_index,
                              parameter, project_id=None):
        if project_id:
            self.set_active_project_by_id(project_id)
        self.check_permission("write")
        db = self._get_project_database()
        _id = self._get_id_from_name(net_name, db)
        elements = list(db[element].find({"index": element_index, "net_id": _id}))
        if len(elements) == 0:
            raise PandaHubError("Element doesn't exist", 404)
        element = elements[0]
        if parameter not in element:
            raise PandaHubError("Parameter doesn't exist", 404)
        return element[parameter]

    def delete_net_element(self, net_name, element, element_index, project_id=None):
        if project_id:
            self.set_active_project_by_id(project_id)
        self.check_permission("write")
        db = self._get_project_database()
        _id = self._get_id_from_name(net_name, db)
        db[element].delete_one({"index": element_index, "net_id": _id})


    def set_net_value_in_db(self, net_name, element, element_index,
                            parameter, value, project_id=None):
        if project_id:
            self.set_active_project_by_id(project_id)
        print("SET", net_name, element, element_index, parameter, value)
        self.check_permission("write")
        db = self._get_project_database()
        _id = self._get_id_from_name(net_name, db)
        db[element].find_one_and_update({"index": element_index, "net_id": _id},
                                        {"$set": {parameter: value}})

    def create_element_in_db(self, net_name, element, element_index, data, project_id=None):
        if project_id:
            self.set_active_project_by_id(project_id)
        self.check_permission("write")
        db = self._get_project_database()
        _id = self._get_id_from_name(net_name, db)
        element_data = {**data, **{"index": element_index, "net_id": _id}}
        db[element].insert_one(element_data)

    def create_elements_in_db(self, net_name: str, element_type: str, elements_data: list, project_id=None):
        if project_id:
            self.set_active_project_by_id(project_id)
        self.check_permission("write")
        db = self._get_project_database()
        _id = self._get_id_from_name(net_name, db)
        data = []
        for elm_data in elements_data:
            data.append({**elm_data, **{"net_id": _id}})
        db[element_type].insert_many(data)


    # -------------------------
    # Bulk operations
    # -------------------------

    def bulk_write_to_db(self, data, collection_name="tasks", global_database=True):
        """
        Writes any number of documents to the database at once. Checks, if any
        document with the same _id already exists in the database. Already existing
        _ids will be replaced with the according new document.

        Parameters
        ----------
        data : list
            List of all documents that shall be added to the database.
        project : str
            Name of the database.
        collection_name : str
            Name of the collection the new documents shall be added to.

        Returns
        -------
        None.

        """
        if global_database:
            db = self._get_global_database()
        else:
            self.check_permission("write")
            db = self._get_project_database()
        operations = [ReplaceOne(replacement=d, filter={"_id": d["_id"]},
                                 upsert=True)
                      for d in data]
        db[collection_name].bulk_write(operations)

    def bulk_update_in_db(self, data, document_ids, collection_name="tasks", global_database=False):
        """
        Updates any number of documents in the database at once, according to their
        document_ids.

        Parameters
        ----------
        data : list
            List of all documents that shall be added to the database.
        document_ids : list
            Contains the ids of the documents to be updated in the same order as they are
            in the **data** variable.
        project : str
            Name of the database that the orignial timeseries is in.
        collection_name : str
            Name of the collection that the orignial timeseries is in.

        Returns
        -------
        None.

        """
        if global_database:
            db = self._get_global_database()
        else:
            self.check_permission("write")
            db = self._get_project_database()
        operations = {}
        operations["UpdateOne"] = []
        i = 0
        for d in data:
            operations["UpdateOne"].append({
                        "filter": {"_id": document_ids[i]},
                        "update": {"$push": d},
                        "upsert": False
                    })
            i+=1

        db[collection_name].bulk_write(operations)


    # -------------------------
    # Timeseries
    # -------------------------

    def write_timeseries_to_db(self, timeseries, data_type, element_type=None,
                               netname=None, element_index=None, name=None,
                               global_database=False, collection_name="timeseries",
                               project_id=None, **kwargs):
        """
        This function can be used to write a timeseries to a MongoDB database.
        The timeseries must be provided as a pandas Series with the timestamps as
        an index. The timeseries will be represented by a dict, that contains all
        metadata on the first level. The element_type and data_type of the
        timeseries are required metadata, netname and element_index are optional.
        Additionally, arbitrary kwargs can be added. The timeseires itself can be
        accessed with the attribute 'timeseries_data'.

        By default, the document will have an _id generated based on its metadata
        using the function get_document_hash but an _id provided by the user as a
        kwarg will not be overwritten.

        If a timeseries document with the same _id already exists in the specified
        database collection, it will be replaced by the new one.

        Parameters
        ----------
        timeseries : pandas.Series
            A timeseries with the timestamps as index.
        data_type : str
            Type and unit the timeseries values' are given in. The recommended format
            is <type>_<unit> (e.g. p_mw).
        element_type : str
            Kind of element the timeseries belongs to (load/sgen).
            The default is None.
        project : str
            Name of the database. The default is None.
        collection_name : str
            Name of the collection the timeseries document shall be added to.
            The default is None.
        netname : str, optional
            Name of the network the timeseries belongs to. Is only added to the
            document if any value is specified. The default is None.
        element_index : int (could also be str, but int is recommended), optional
            The index of the element the timeseries belongs to. Is only added to the
            document if any value is specified. The default is None.
        return_id : bool
            if True returns the _id of the document that was written to the
            database. The default is False.
        **kwargs :
            Any additional metadata that shall be added to the document.

        Returns
        -------
        document _id if return_id = True.

        """
        if project_id:
            self.set_active_project_by_id(project_id)
        if global_database:
            db = self._get_global_database()
        else:
            self.check_permission("write")
            db = self._get_project_database()
        document = create_timeseries_document(timeseries=timeseries,
                                              data_type=data_type,
                                              element_type=element_type,
                                              netname=netname,
                                              element_index=element_index,
                                              name=name,
                                              **kwargs)
        db[collection_name].find_one_and_replace(
            {"_id": document["_id"]},
            document,
            upsert=True
        )
        logger.debug("document with _id {document['_id']} added to database")


    def bulk_write_timeseries_to_db(self, timeseries, data_type,
                                    element_type=None, netname=None,
                                    meta_frame=None,
                                    global_database=False,
                                    collection_name="timeseries",
                                    **kwargs):
        """
        This function can be used to write a pandas DataFrame, containing multiple
        timeseries of the same element_type and data_type at once to a MongoDB
        database (e.g. all p_mw timeseries of all loads in a grid).
        The element_type and data_type of the timeseries are required metadata,
        the netname is optional.
        Additionally, arbitrary kwargs can be added. All these attributed will be
        the same for all timeseries documents created from this dataframe!

        The element_index for each timeseries will be set to the value that is given
        by the column name of its DataFrame column.


        Parameters
        ----------
        timeseries : pandas.DataFrame
            A DataFrame of timeseries with the timestamps as index.
        data_type : str
            Type and unit the timeseries values' are given in. The recommended format
            is <type>_<unit> (e.g. p_mw).
        element_type : str
            Kind of element the timeseries belongs to (load/sgen).
        project : str
            Name of the database.
        collection_name : str
            Name of the collection the timeseries document shall be added to.
        netname : str, optional
            Name of the network the timeseries belongs to. Is only added to the
            document if any value is specified. The default is None.
        meta_frame = pandas.DataFrame
            A DataFrame of meta_data for the timeseries.
            The index must equal the columns of the timeseries DataFrame.
        return_ids : bool
            if True returns the _id fields of the documents that were written
            to the database. The default is False.
        **kwargs :
            Any additional metadata that shall be added to the document.

        Returns
        -------
        document _ids if return_id = True.

        """
        documents = []
        for col in timeseries.columns:
            if meta_frame is not None:
                args = {**kwargs, **meta_frame.loc[col]}
            else:
                args = kwargs
            doc = create_timeseries_document(timeseries[col],
                                                        element_type=element_type,
                                                        data_type=data_type,
                                                        netname=netname,
                                                        element_index=col,
                                                        **args)
            documents.append(doc)
        self.bulk_write_to_db(documents, collection_name=collection_name,
                              global_database=global_database)
        logger.debug(f"{len(documents)} documents added to database")
        return [d["_id"] for d in documents]

    def update_timeseries_in_db(self, new_ts_content, document_id, collection_name="timeseries",
                               global_database=False):

        """
        This function can be used to append a timeseries to an existing timseries
        in the MongoDB database.
        The timeseries to be added must be provided as a pandas Series with the
        timestamps as an index. A document_id must be provided, which identifies
        the existing timeseries that the new timeseries is to be added to.


        Parameters
        ----------
        new_ts_content : pandas.Series
            A timeseries with the timestamps as index.
        document_id: str
            The id of an already existing timeseries document in the database.
        project : str
            Name of the database that the orignial timeseries is in.
        collection_name : str
            Name of the collection that the orignial timeseries is in.


        Returns
        -------
        None.

        """
        if global_database:
            db = self._get_global_database()
        else:
            self.check_permission("write")
            db = self._get_project_database()
        ts_update = {"timeseries_data": {"$each": convert_timeseries_to_subdocuments(new_ts_content)}}
        db[collection_name].find_one_and_update({"_id": document_id},
                                                {"$push": ts_update},
                                                upsert=False
                                                )
        #logger.info("document updated in database")

    def bulk_update_timeseries_in_db(self, new_ts_content, document_ids, collection_name="timeseries",
                                     global_database=False):

        """
        This function can be used to append a pandas DataFrame, containing multiple
        timeseries of the same element_type and data_type at once, to an already
        existing pandas DataFrame in a MongoDB
        database (e.g. all p_mw timeseries of all loads in a grid).

        The element_index for each timeseries will be set to the value that is given
        by the column name of its DataFrame column.


        Parameters
        ----------
        new_ts_content : pandas.DataFrame
            A DataFrame of timeseries with the timestamps as index.
        document_ids : list
            Contains the ids of the documents to be updated in the same order as they
            are in the columns of **new_ts_content**.
        project : str
            Name of the database that the orignial timeseries is in.
        collection_name : str
            Name of the collection that the orignial timeseries is in.


        Returns
        -------
        None
        """
        documents = []
        for i in range(len(new_ts_content.columns)):
            col = new_ts_content.columns[i]
            document = {}
            document["timeseries_data"] = {"$each": convert_timeseries_to_subdocuments(new_ts_content[col])}
            documents.append(document)
        self.bulk_update_in_db(documents, document_ids, project=project,
                               collection_name="timeseries", global_database=global_database)

        #logger.debug(f"{len(documents)} documents added to database")


    def get_timeseries_from_db(self, filter_document={}, timestamp_range=None,
                               global_database=False, collection_name="timeseries",
                               include_metadata=False, project_id=None, **kwargs):
        """
        This function can be used to retrieve a single timeseries from a
        MongoDB database that matches the provided metadata filter_document.
        Additionally, arbitrary filter kwargs can be provided.

        The timeseries in the database is represented by a dict, that contains all
        metadata on the first level. The timeseries itself is returned as a
        pandas Series. With include_metadata = False only the timeseries itself
        is returned. With include_metadata = True, the whole document is
        returned. In this case, the timeseries is stored at "timeseries_data".

        If only a subset of timesteps is required, a tuple containing the first
        and last timestamp of the desired timestep range can be passed to the function.

        Parameters
        ----------
        filter_document : dict
            dictionary, containing all key value pairs the documents in the database
            shall be filtered for.
        project : str
            Name of the database.
        collection_name : str
            Name of the collection that shall be queried.
        netname : TYPE, optional
            DESCRIPTION. The default is None.
        element_index : int (could also be str, but int is recommended), optional
            The index of the element the timeseries belongs to. Is only added to the
            document if any value is specified. The default is None.
        timestamp_range : tuple, optional
            tuple, containing the first and last timestamp of the desired timestep
            range The default is None.
        include_metadata : bool
            If True, the whole document is returned. If False only the
            timeseries itself.
        **kwargs :
            Any additional metadata that shall be added to the filter.

        Returns
        -------
        timeseries : pandas.Series
            DESCRIPTION.

        """
        if project_id:
            self.set_active_project_by_id(project_id)
        if global_database:
            db = self._get_global_database()
        else:
            self.check_permission("read")
            db = self._get_project_database()
        filter_document = {**filter_document, **kwargs}
        pipeline = [{"$match": filter_document}]
        if timestamp_range:
            pipeline.append({"$project": {"timeseries_data": {"$filter": {"input": "$timeseries_data",
                                                                          "as": "timeseries_data",
                                                                          "cond": {"$and": [{"$gte": ["$$timeseries_data.timestamp", timestamp_range[0]]},
                                                                                            {"$lt": ["$$timeseries_data.timestamp", timestamp_range[1]]}]}}}}})
        pipeline.append({"$addFields": {"timestamps": "$timeseries_data.timestamp",
                                            "values": "$timeseries_data.value" }})
        if include_metadata:
            pipeline.append({"$project": {"timeseries_data": 0}})
        else:
            pipeline.append({"$project": {"timestamps":1,
                                          "values":1,
                                          "_id":0}})
        data = list(db[collection_name].aggregate(pipeline))
        if len(data) == 0:
            raise PandaHubError("no documents matching the provided filter found", 404)
        elif len(data) > 1:
            raise PandaHubError("multiple documents matching the provided filter found")
        else:
            data = data[0]
        timeseries_data = pd.Series(data["values"], index=data["timestamps"])
        if include_metadata:
            data["timeseries_data"] = timeseries_data
            del data["timestamps"]
            del data["values"]
            return data
        else:
            return timeseries_data

    def get_timeseries_metadata(self, filter_document, collection_name="timeseries", global_database=False):
        """
        Returns a DataFrame, containing all metadata matching the provided filter.
        A filter document has to be provided in form of a dictionary, containing
        all key value pairs the documents in the database shall be filtered for.
        It is possible to filter for multiple values at once, by passing them
        as lists (e.g. {"element_index": [0,1,2]})

        Parameters
        ----------
        filter_document : dict
            dictionary, containingall key value pairs the documents in the database
            shall be filtered for.
        project : str
            Name of the database.
        collection_name : str
            Name of the collection that shall be queried.

        Returns
        -------
        metadata : pandas.DataFrame
            DataFrame, containing all metadata matching the provided filter.

        """
        if global_database:
            db = self._get_global_database()
        else:
            self.check_permission("read")
            db = self._get_project_database()
        match_filter = []
        pipeline = []
        for key in filter_document:
            if key == "timestamp_range":
                continue
            filter_value = filter_document[key]
            if type(filter_value) == list:
                match_filter.append({key: {"$in": filter_value}})
            else:
                match_filter.append({key: filter_value})
        if match_filter:
            pipeline.append({"$match": {"$and": match_filter}})
        projection = {"$project":{"timeseries_data":0}}
        pipeline.append(projection)
        metadata = list(db[collection_name].aggregate(pipeline))
        df_metadata = pd.DataFrame(metadata)
        if len(df_metadata):
            df_metadata.set_index("_id", inplace=True)
        return df_metadata

    def add_metadata(self, filter_document, add_meta, global_database=False,
                     collection_name="timeseries"):
        if global_database:
            db = self._get_global_database()
        else:
            self.check_permission("write")
            db = self._get_project_database()

        # get metada before change
        meta_before = self.get_timeseries_metadata(filter_document, global_database=global_database,
                                                   collection_name=collection_name)
        # add the new information to the metadata dict of the existing timeseries
        if len(meta_before) > 1: #TODO is this the desired behaviour? Needs to specified
            raise PandaHubError
        meta_copy = {**meta_before.iloc[0].to_dict(), **add_meta}
        # write new metadata to mongo db
        db[collection_name].find_one_and_replace({"_id": meta_before.index[0]},
                                                 meta_copy, upsert=True)
        return meta_copy

    def multi_get_timeseries_from_db(self, filter_document={}, timestamp_range=None,
                                     exclude_timestamp_range=None,
                                     global_database=False, collection_name="timeseries",
                                     project_id=None,**kwargs):
        if project_id:
            self.set_active_project_by_id(project_id)
        if global_database:
            db = self._get_global_database()
        else:
            self.check_permission("read")
            db = self._get_project_database()
        filter_document = {**filter_document, **kwargs}
        match_filter = []
        for key in filter_document:
            filter_value = filter_document[key]
            if type(filter_value) == list:
                match_filter.append({key: {"$in": filter_value}})
            else:
                match_filter.append({key: filter_value})

        pipeline = [{"$match": {"$and": match_filter}}]
        if timestamp_range:
            projection = {"timeseries_data": {"$filter": {"input": "$timeseries_data",
                                                          "as": "timeseries_data",
                                                          "cond": {"$and": [{"$gte": ["$$timeseries_data.timestamp", timestamp_range[0]]},
                                                                            {"$lt": ["$$timeseries_data.timestamp", timestamp_range[1]]}]}}}}
            pipeline.append({"$project": projection})
        if exclude_timestamp_range:
            projection = {"timeseries_data": {"$filter": {"input": "$timeseries_data",
                                                          "as": "timeseries_data",
                                                          "cond": {"$or": [{"$lt": ["$$timeseries_data.timestamp", timestamp_range[0]]},
                                                                           {"$gte": ["$$timeseries_data.timestamp", timestamp_range[1]]}]}}}}
            pipeline.append({"$project": projection})

        timeseries = []
        for ts in db[collection_name].aggregate(pipeline):
            if len(ts["timeseries_data"]) == 0:
                continue
            df = pd.DataFrame(ts["timeseries_data"])
            df.set_index("timestamp", inplace=True)
            df.index.name = None
            ts["timeseries_data"] = df["value"]
            if exclude_timestamp_range is not None or timestamp_range is not None:
                #TODO: Second query to get the metadata, since metadata is not returned if a projection on the subfield is used
                metadata = db[collection_name].find_one({"_id": ts["_id"]}, projection={"timeseries_data": 0})
                ts.update(metadata)
            timeseries.append(ts)
        return timeseries

    def bulk_get_timeseries_from_db(self, filter_document={}, timestamp_range=None,
                                    exclude_timestamp_range=None,
                                    additional_columns=None, pivot_by_column=None,
                                    global_database=False, collection_name="timeseries",
                                    **kwargs):
        """
        This function can be used to retrieve multiple timeseries at once from a
        MongoDB database. The timeseries will be filtered by their metadata.
        A filter document has to be provided in form of a dictionary, containing
        all key value pairs the documents in the database shall be filtered for.
        It is possible to filter for multiple values at once, by passing them
        as lists (e.g. {"element_index": [0,1,2]})

        By default, the data is returned as a long-form DataFrame with the
        timestamps as an index and the values as the only columns.

        Additional columns that shall be added to the DataFrame can be provided
        as a list.

        Optionally, the data can also be returned as a wide-form DataFrame, by
        passing a column name the DataFrame shall be pivoted (aggregated) by.

        For some examples how to use this function also see the pandapower Pro
        tutorial "timeseries_in_mongodb"


        Parameters
        ----------
        filter_document : dict
            dictionary, containingall key value pairs the documents in the database
            shall be filtered for.
        project : str
            Name of the database.
        collection_name : str
            Name of the collection that shall be queried.
        additional_columns : list, optional
            List of column names, that shall be added to the returned DataFrame.
            The default is None.
        pivot_by_column : str, optional
            Name of the column, the DataFrame shall be pivoted (aggregated) by.
            The default is None.

        Returns
        -------
        timeseries : pandas.DataFrame
            DataFrame, containing all timeseries that match the provided
            filter_document.

        """
        if global_database:
            db = self._get_global_database()
        else:
            self.check_permission("read")
            db = self._get_project_database()

        filter_document = {**filter_document, **kwargs}
        match_filter = []
        for key in filter_document:
            filter_value = filter_document[key]
            if type(filter_value) == list:
                match_filter.append({key: {"$in": filter_value}})
            else:
                match_filter.append({key: filter_value})

        pipeline = [{"$match": {"$and": match_filter}}]
        custom_projection = {"_id": 0}
        if additional_columns:
            for column in additional_columns:
                custom_projection[column] = 1
        if pivot_by_column:
            custom_projection[pivot_by_column] = 1
        if timestamp_range:
            projection = {"timeseries_data": {"$filter": {"input": "$timeseries_data",
                                                          "as": "timeseries_data",
                                                          "cond": {"$and": [{"$gte": ["$$timeseries_data.timestamp", timestamp_range[0]]},
                                                                            {"$lt": ["$$timeseries_data.timestamp", timestamp_range[1]]}]}}}}
            projection = {**projection, **custom_projection}
            pipeline.append({"$project": projection})
        if exclude_timestamp_range:
            projection = {"timeseries_data": {"$filter": {"input": "$timeseries_data",
                                                          "as": "timeseries_data",
                                                          "cond": {"$or": [{"$lt": ["$$timeseries_data.timestamp", timestamp_range[0]]},
                                                                           {"$gte": ["$$timeseries_data.timestamp", timestamp_range[1]]}]}}}}
            projection = {**projection, **custom_projection}
            pipeline.append({"$project": projection})
        pipeline.append({"$unwind": "$timeseries_data"})
        projection = {"value": "$timeseries_data.value",
                      "timestamp": "$timeseries_data.timestamp"}
        projection = {**projection, **custom_projection}
        pipeline.append({"$project": projection})
        timeseries = pd.DataFrame(db[collection_name].aggregate(pipeline))
        if len(timeseries) == 0:
            logger.warning("No timeseries found matching the provided filter")
            return timeseries
        timeseries.index = timeseries["timestamp"]
        columns = list(timeseries.columns)
        columns.remove("timestamp")
        timeseries = timeseries[columns]
        if pivot_by_column:
            timeseries = timeseries.pivot(columns=pivot_by_column, values="value")
        return timeseries


    def delete_timeseries_from_db(self, element_type, data_type, netname=None,
                                  element_index=None, collection_name="timeseries",
                                   **kwargs):
        """
        This function can be used to delete a single timeseries that matches
        the provided metadata from a MongoDB database. The element_type and data_type
        of the timeseries are required, netname and element_index are optional.
        Additionally, arbitrary kwargs can be used.

        Parameters
        ----------
        element_type : str
            Kind of element the timeseries belongs to (load/sgen).
        data_type : str
            Type and unit the timeseries values' are given in. The recommended format
            is <type>_<unit> (e.g. p_mw).
        project : str
            Name of the database.
        collection_name : str
            Name of the collection that shall be queried.
        netname : TYPE, optional
            DESCRIPTION. The default is None.
        element_index : int (could also be str, but int is recommended), optional
            The index of the element the timeseries belongs to. Is only added to the
            document if any value is specified. The default is None.
        timestamp_range : tuple, optional
            tuple, containing the first and last timestamp of the desired timestep
            range The default is None.
        **kwargs :
            Any additional metadata that shall be added to the filter.

        Returns
        -------
        Success : Bool
            DESCRIPTION.

        """
        self.check_permission("write")
        db = self._get_project_database()

        filter_document = {"element_type": element_type,
                           "data_type": data_type}
        if netname is not None:
            filter_document["netname"] = netname
        if element_index is not None:
            filter_document["element_index"] = element_index
        filter_document = {**filter_document, **kwargs}
        del_res = db[collection_name].delete_one(filter_document)
        return del_res


    def bulk_del_timeseries_from_db(self, filter_document,
                                    collection_name="timeseries"):
        """
        This function can be used to delete multiple timeseries at once from a
        MongoDB database. The timeseries will be filtered by their metadata.
        A filter document has to be provided in form of a dictionary, containing
        all key value pairs the documents in the database shall be filtered for.
        It is possible to filter for multiple values at once, by passing them
        as lists (e.g. {"element_index": [0,1,2]})

        Parameters
        ----------
        filter_document : dict
            dictionary, containingall key value pairs the documents in the database
            shall be filtered for.
        project : str
            Name of the database.
        collection_name : str
            Name of the collection that shall be queried.

        Returns
        -------
        timeseries : pandas.DataFrame
            DataFrame, containing all timeseries that match the provided
            filter_document.

        """
        self.check_permission("write")
        db = self._get_project_database()
        match_filter = {}
        for key in filter_document:
            if key == "timestamp_range":
                continue
            filter_value = filter_document[key]
            if type(filter_value) == list:
                match_filter[key] = {"$in": filter_value}
            else:
                match_filter[key] = filter_value
        del_res = None
        del_res = db[collection_name].delete_many(match_filter)
        return del_res


if __name__ == '__main__':
    self = PandaHub()
    project_name = 'test_project'
    self.set_active_project(project_name)
    ts = self.multi_get_timeseries_from_db(global_database=True)
    # r = self.create_account(email, password)
    # self.login(email, password)
    # project_exists = self.project_exists(project_name)
    # if project_exists:
    # r = self.delete_project(project_name, i_know_this_action_is_final=True)
    # self.create_project(project_name)


    # r = self.delete_project(project_name, i_know_this_action_is_final=True)

    # print(r.json())
