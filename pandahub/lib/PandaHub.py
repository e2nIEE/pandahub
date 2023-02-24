# -*- coding: utf-8 -*-

import builtins
import importlib
import json
import logging
import traceback
from inspect import signature, _empty
from typing import Optional

import numpy as np
import pandas as pd
from bson.objectid import ObjectId
from pydantic.types import UUID4
from pymongo import MongoClient, ReplaceOne, DESCENDING

import pandapipes as pps
from pandapipes import from_json_string as from_json_pps
import pandapower as pp
import pandapower.io_utils as io_pp
from pandahub.api.internal import settings
from pandahub.lib.database_toolbox import create_timeseries_document, convert_timeseries_to_subdocuments, \
    convert_dataframes_to_dicts, json_to_object
from pandahub.lib.database_toolbox import decompress_timeseries_data, convert_geojsons

logger = logging.getLogger(__name__)
from pandahub import __version__
from packaging import version



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

    _datatypes = getattr(importlib.import_module(settings.DATATYPES_MODULE), "datatypes")

    # -------------------------
    # Initialization
    # -------------------------

    def __init__(self, connection_url=None, check_server_available=False, user_id=None):
        if connection_url is None:
            connection_url = settings.MONGODB_URL
        if not connection_url.startswith('mongodb://'):
            raise PandaHubError("Connection URL needs to point to a mongodb instance: 'mongodb://..'")
        self.mongo_client = MongoClient(host=connection_url, uuidRepresentation="standard", connect=False)
        self.mongo_client_global_db = None
        self.active_project = None
        self.user_id = user_id
        self.base_variant_filter = {
            "$or": [
                {"var_type": {"$exists": False}},
                {"var_type": "base"}
            ]
        }
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

    def create_project(self, name, settings=None, realm=None, metadata=None, project_id=None,
                       activate=True):
        if self.project_exists(name, realm):
            raise PandaHubError("Project already exists")
        if settings is None:
            settings = {}
        if metadata is None:
            metadata = {}
        project_data = {"name": name,
                        "realm": realm,
                        "settings": settings,
                        "metadata": metadata,
                        "version": __version__}
        if project_id:
            project_data["_id"] = project_id
        if self.user_id is not None:
            project_data["users"] = {self.user_id: "owner"}
        self.mongo_client["user_management"]["projects"].insert_one(project_data)
        if activate:
            self.set_active_project(name, realm)
        return project_data

    def delete_project(self, i_know_this_action_is_final=False, project_id=None):
        if project_id:
            self.set_active_project_by_id(project_id)
        project_id = self.active_project["_id"]
        self.check_permission("write")
        if not i_know_this_action_is_final:
            raise PandaHubError(
                "Calling this function will delete the whole project and all the nets stored within. It can not be reversed. Add 'i_know_this_action_is_final=True' to confirm.")
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
            filter_dict = {"users": {"$exists": False}}
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
        try:
            self.active_project = self._get_project_document({"_id": ObjectId(project_id)})
        except:
            self.active_project = self._get_project_document({"_id": project_id})
        self.upgrade_project_to_latest_version()

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
            {"_id": self.active_project["_id"], },
            {"$set": {"locked": True, "locked_by": self.user_id}}
        )
        return result.acknowledged and result.modified_count > 0

    def unlock_project(self):
        db = self.mongo_client["user_management"]["projects"]
        return db.update_one(
            {"_id": self.active_project["_id"], "locked_by": self.user_id},
            {"$set": {"locked": False, "locked_by": None}}
        )

    def force_unlock_project(self, project_id):
        db = self.mongo_client["user_management"]["projects"]
        project = db.find_one({"_id": ObjectId(project_id)})
        user = self._get_user()
        if project is None:
            return None
        if "users" not in project or self.user_id in project["users"].keys() or user["is_superuser"]:
            return db.update_one({"_id": ObjectId(project_id)}, {"$set": {"locked": False, "locked_by": None}})
        else:
            raise PandaHubError("You don't have rights to access this project", 403)

    def project_exists(self, project_name=None, realm=None):
        project_collection = self.mongo_client["user_management"].projects
        project = project_collection.find_one({"name": project_name, "realm": realm})
        return project is not None

    def _get_project_document(self, filter_dict: dict) -> Optional[dict]:
        project_collection = self.mongo_client["user_management"].projects
        projects = list(project_collection.find(filter_dict))
        if len(projects) == 0:  # project doesn't exist
            return None
        if len(projects) > 1:
            raise PandaHubError(
                "Duplicate Project detected. This should never happen if you create projects through the API. Remove duplicate projects manually in the database.")
        project_doc = projects[0]
        if "users" not in project_doc:
            return project_doc  # project is not user protected

        user = self._get_user()
        if not user["is_superuser"] and self.user_id not in project_doc["users"].keys():
            raise PandaHubError("You don't have rights to access this project", 403)
        elif project_doc.get("locked") and project_doc.get("locked_by") != self.user_id:
            raise PandaHubError("Project is locked by another user")
        else:
            return project_doc

    def _get_project_database(self):
        return self.mongo_client[str(self.active_project["_id"])]

    def _get_global_database(self):
        if self.mongo_client_global_db is None and not settings.MONGODB_URL_GLOBAL_DATABASE is None:
            self.mongo_client_global_db = MongoClient(
                host=settings.MONGODB_URL_GLOBAL_DATABASE, uuidRepresentation="standard"
            )
        if self.mongo_client_global_db is None:
            return self.mongo_client["global_data"]
        else:
            return self.mongo_client_global_db["global_data"]

    def get_project_version(self):
        return self.active_project.get("version", "0.2.2")

    def upgrade_project_to_latest_version(self):
        # TODO check that user has right to write user_management
        # TODO these operations should be encapsulated in a transaction in order to avoid
        #      inconsistent Database states in case of occuring errors
        def _set_project_version(ph_version):
            project_collection = self.mongo_client["user_management"].projects
            project_collection.find_one_and_update({"_id": self.active_project["_id"]},
                                                   {"$set": {"version": ph_version}})
            logger.info(f"upgraded projekt '{self.active_project['name']}' from version"
                        f" {self.get_project_version()} to version {ph_version}")
            self.active_project["version"] = ph_version

        project_version = self.get_project_version()
        # we are up-to-date - nothing to do
        if project_version == __version__:
            return

        # apply migrations sequentially:

        # upgrade from < 0.2.3 to 0.2.3
        if version.parse(project_version) < version.parse("0.2.3"):
            db = self._get_project_database()
            all_collection_names = db.list_collection_names()
            old_net_collections = [name for name in all_collection_names if
                                   not name.startswith("_") and
                                   not name == "timeseries" and not name.startswith("net_")]

            for element in old_net_collections:
                db[element].rename(self._collection_name_of_element(element))
            _set_project_version("0.2.3")

        # upgrade from 0.2.3 to 0.2.4
        if self.active_project["version"] == "0.2.3":
            db = self._get_project_database()
            # for all networks
            for d in list(db["_networks"].find({}, projection={"sector": 1, "data": 1})):
                # load old format
                if d.get("sector", "power") == "power":
                    data = dict((k, json.loads(v, cls=io_pp.PPJSONDecoder)) for k, v in d['data'].items())
                else:
                    data = dict((k, from_json_pps(v)) for k, v in d['data'].items())
                # save new format
                for key, dat in data.items():
                    try:
                        json.dumps(dat)
                    except:
                        dat = f"serialized_{json.dumps(data, cls=io_pp.PPJSONEncoder)}"
                    data[key] = dat

                db["_networks"].find_one_and_update({"_id":d["_id"]},
                                                    {"$set": {"data": data}})

        # !! when adding new migrations, explicitly add _set_project_version() on the previous migration

        # no further actions required, set project to current ph version
        _set_project_version(__version__)

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

    def set_project_settings_value(self, parameter, value, project_id=None):
        if project_id:
            self.set_active_project_by_id(project_id)
        self.check_permission("write")
        _id = self.active_project["_id"]
        project_collection = self.mongo_client["user_management"]["projects"]
        setting_string = "settings.{}".format(parameter)
        project_collection.find_one_and_update({"_id": _id}, {"$set": {setting_string: value}})
        self.active_project["settings"][parameter] = value

    def get_project_metadata(self, project_id=None):
        if project_id:
            self.set_active_project_by_id(project_id)
        self.check_permission("read")
        metadata = self.active_project.get("metadata") or dict()

        # Workaround until mongo 5.0
        def restore_empty(data):
            for key, val in data.items():
                if isinstance(val, dict):
                    restore_empty(val)
                elif isinstance(val, str):
                    if val == "_none":
                        val = None
                    elif val.startswith("_empty_"):
                        t = getattr(builtins, val.replace("_empty_", ""))
                        val = t()
                data[key] = val

        restore_empty(metadata)
        return metadata

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
        def replace_empty(updated, data):
            for key, val in data.items():
                if val is None:
                    updated[key] = "_none"
                elif hasattr(val, "__iter__") and len(val) == 0:
                    updated[key] = f"_empty_{type(val).__name__}"
                elif isinstance(val, dict):
                    sub_upd = dict()
                    replace_empty(sub_upd, val)
                    updated[key] = sub_upd
                else:
                    updated[key] = val

        update_metadata = dict()
        replace_empty(update_metadata, new_metadata)

        self.mongo_client.user_management.projects.update_one(
            {"_id": project_data['_id']},
            [
                {"$unset": "metadata"},  # deletion needed because set won't delete not existing fields
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
            {"id": {"$in": [UUID4(user_id) for user_id in project_users.keys()]}}
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

    def get_net_from_db(self, name, include_results=True, only_tables=None, project_id=None,
                        geo_mode="string", variants=[]):
        if project_id:
            self.set_active_project_by_id(project_id)
        self.check_permission("read")
        db = self._get_project_database()
        _id = self._get_id_from_name(name, db)
        if _id is None:
            return None
        return self.get_net_from_db_by_id(_id, include_results, only_tables, geo_mode=geo_mode, variants=variants)

    def get_net_from_db_by_id(self, id, include_results=True, only_tables=None, convert=True,
                              geo_mode="string", variants=[]):
        self.check_permission("read")
        return self._get_net_from_db_by_id(id, include_results, only_tables, convert=convert,
                                           geo_mode=geo_mode, variants=variants)

    def _get_net_from_db_by_id(self, id_, include_results=True, only_tables=None, convert=True,
                               geo_mode="string", variants=[]):
        db = self._get_project_database()
        meta = self._get_network_metadata(db, id_)

        package = pp if meta.get("sector", "power") == "power" else pps
        net = package.create_empty_network()

        # add all elements that are stored as dataframes
        collection_names = self._get_net_collections(db)
        for collection_name in collection_names:
            el = self._element_name_of_collection(collection_name)
            self._add_element_from_collection(net, db, el, id_, include_results=include_results,
                                              only_tables=only_tables, geo_mode=geo_mode,
                                              variants=variants)
        # add data that is not stored in dataframes
        self.deserialize_and_update_data(net, meta)

        if convert:
            package.convert_format(net)

        return net

    def deserialize_and_update_data(self, net, meta):
        if version.parse(self.get_project_version()) <= version.parse("0.2.3"):
            if meta.get("sector", "power") == "power":
                data = dict((k, json.loads(v, cls=io_pp.PPJSONDecoder)) for k, v in meta['data'].items())
                net.update(data)
            else:
                data = dict((k, from_json_pps(v)) for k, v in meta['data'].items())
                net.update(data)
        else:
            for key, value in meta["data"].items():
                if type(value) == str and value.startswith("serialized_"):
                    value = json.loads(value[11:], cls=io_pp.PPJSONDecoder)
                net[key] = value

    def get_subnet_from_db(self, name, bus_filter=None, include_results=True,
                           add_edge_branches=True, geo_mode="string", variants=[]):
        self.check_permission("read")
        db = self._get_project_database()
        _id = self._get_id_from_name(name, db)
        if _id is None:
            return None
        return self.get_subnet_from_db_by_id(_id, bus_filter=bus_filter, include_results=include_results,
                                             add_edge_branches=add_edge_branches, geo_mode=geo_mode, variants=variants)

    def get_subnet_from_db_by_id(self, net_id, bus_filter=None, include_results=True,
                           add_edge_branches=True, geo_mode="string", variants=[],
                           ignore_elements=[]):
        db = self._get_project_database()
        meta = self._get_network_metadata(db, net_id)
        dtypes = db["_networks"].find_one({"_id": net_id}, projection={"dtypes"})

        net = pp.create_empty_network()

        if db[self._collection_name_of_element("bus")].find().count() == 0:
            net["empty"] = True

        # Add buses with filter
        if bus_filter is not None:
            self._add_element_from_collection(net, db, "bus", net_id, bus_filter, geo_mode=geo_mode,
                                              variants=variants, dtypes=dtypes)
        buses = net.bus.index.tolist()

        branch_operator = "$or" if add_edge_branches else "$and"
        # Add branch elements connected to at least one bus
        self._add_element_from_collection(net, db, "line", net_id,
                                          {branch_operator: [
                                              {"from_bus": {"$in": buses}},
                                              {"to_bus": {"$in": buses}}]}, geo_mode=geo_mode,
                                          variants=variants, dtypes=dtypes)
        self._add_element_from_collection(net, db, "trafo", net_id,
                                          {branch_operator: [
                                              {"hv_bus": {"$in": buses}},
                                              {"lv_bus": {"$in": buses}}]}, geo_mode=geo_mode,
                                          variants=variants, dtypes=dtypes)
        self._add_element_from_collection(net, db, "trafo3w", net_id,
                                          {branch_operator: [
                                              {"hv_bus": {"$in": buses}},
                                              {"mv_bus": {"$in": buses}},
                                              {"lv_bus": {"$in": buses}}]}, geo_mode=geo_mode,
                                          variants=variants, dtypes=dtypes)

        self._add_element_from_collection(net, db, "switch", net_id,
                                          {"$and": [
                                              {"et": "b"},
                                              {branch_operator: [
                                                  {"bus": {"$in": buses}},
                                                  {"element": {"$in": buses}}
                                              ]}
                                          ]
                                          }, geo_mode=geo_mode, variants=variants, dtypes=dtypes)
        if add_edge_branches:
            # Add buses on the other side of the branches
            branch_buses = set(net.trafo.hv_bus.values) | set(net.trafo.lv_bus.values) | \
                            set(net.line.from_bus) | set(net.line.to_bus) | \
                            set(net.trafo3w.hv_bus.values) | set(net.trafo3w.mv_bus.values) | \
                            set(net.trafo3w.lv_bus.values) | set(net.switch.bus) | set(net.switch.element)
            branch_buses_outside = [int(b) for b in branch_buses - set(buses)]
            self._add_element_from_collection(net, db, "bus", net_id, geo_mode=geo_mode, variants=variants,
                                              filter={"index": {"$in": branch_buses_outside}},
                                              dtypes=dtypes)
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
        self._add_element_from_collection(net, db, "switch", net_id, switch_filter,
                                          geo_mode=geo_mode, variants=variants, dtypes=dtypes)

        # add node elements
        node_elements = ["load", "sgen", "gen", "ext_grid", "shunt", "xward", "ward", "motor", "storage"]
        branch_elements = ["trafo", "line", "trafo3w", "switch", "impedance"]
        all_elements = node_elements + branch_elements + ["bus"]
        all_elements = list(set(all_elements) - set(ignore_elements))

        # add all node elements that are connected to buses within the network
        for element in node_elements:
            filter = {"bus": {"$in": buses}}
            self._add_element_from_collection(net, db, element, net_id,
                                              filter=filter, geo_mode=geo_mode,
                                              include_results=include_results,
                                              variants=variants, dtypes=dtypes)

        # add all other collections
        collection_names = self._get_net_collections(db)
        for collection in collection_names:
            table_name = self._element_name_of_collection(collection)
            # skip all element tables that we have already added
            if table_name in all_elements or table_name in ignore_elements:
                continue
            # for tables that share an index with an element (e.g. load->res_load) load only relevant entries
            for element in all_elements:
                if table_name.startswith(element + "_") or table_name.startswith("net_res_" + element):
                    filter = {"index": {"$in": net[element].index.tolist()}}
                    break
            else:
                # all other tables (e.g. std_types) are loaded without filter
                filter = None
            self._add_element_from_collection(net, db, table_name, net_id,
                                              filter=filter, geo_mode=geo_mode,
                                              include_results=include_results,
                                              variants=variants, dtypes=dtypes)
        self.deserialize_and_update_data(net, meta)
        return net

    def _collection_name_of_element(self, element):
        return f"net_{element}"

    def _element_name_of_collection(self, collection):
        return collection[4:]  # remove "net_" prefix

    def write_network_to_db(self, net, name, sector="power", overwrite=True, project_id=None,
                            metadata=None):
        if project_id:
            self.set_active_project_by_id(project_id)
        self.check_permission("write")
        db = self._get_project_database()

#         if not isinstance(net, pp.pandapowerNet) and not isinstance(net, pps.pandapipesNet):
#             raise PandaHubError("net must be a pandapower or pandapipes object")

        if self._network_with_name_exists(name, db):
            if overwrite:
                self.delete_net_from_db(name)
            else:
                raise PandaHubError("Network name already exists")
        max_id_network = db["_networks"].find_one(sort=[("_id", -1)])
        _id = 0 if max_id_network is None else max_id_network["_id"] + 1

        dfs, data, types = convert_dataframes_to_dicts(net, _id,
                                                       version.parse(self.get_project_version()),
                                                       self._datatypes)
        self._write_net_collections_to_db(db, dfs)

        net_dict = {"_id": _id,
                    "name": name,
                    "sector": sector,
                    "dtypes": types,
                    "data": data}

        if metadata is not None:
            net_dict.update(metadata)
        db["_networks"].insert_one(net_dict)

    def _write_net_collections_to_db(self, db, collections):
        existing_collections = set(db.list_collection_names())

        def add_index(element, df_dict):
            columns = {"bus": ["net_id", "index"],
                       "line": ["net_id", "index", "from_bus", "to_bus"],
                       "trafo": ["net_id", "index", "hv_bus", "lv_bus"],
                       "switch": ["net_id", "index", "bus", "element", "et"],
                       "substation": ["net_id", "index"],
                       "area": ["net_id", "index", "name"]}.get(element, [])
            if element in ["load", "sgen", "gen", "ext_grid", "shunt", "xward", "ward", "motor",
                           "storage"]:
                columns = ["net_id", "bus"]
            for c in columns:
                logger.info(f"creating index on '{c}' in collection '{element}'")
                db[self._collection_name_of_element(element)].create_index([(c, DESCENDING)])

        for element, df_dict in collections.items():
            if len(df_dict) > 0:
                collection_name = self._collection_name_of_element(element)
                try:
                    db[collection_name].insert_many(df_dict, ordered=False)
                    if collection_name not in existing_collections:
                        add_index(element, df_dict)
                except:
                    traceback.print_exc()
                    print(f"\nFAILED TO WRITE TABLE '{element}' TO DATABASE! (details above)")

    def delete_net_from_db(self, name):
        self.check_permission("write")
        db = self._get_project_database()
        _id = self._get_id_from_name(name, db)
        if _id is None:
            raise PandaHubError("Network does not exist", 404)
        collection_names = self._get_net_collections(db)  # TODO
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
        nets = pd.DataFrame(list(db.find(fi, projection=proj)))
        return nets

    def _get_metadata_from_name(self, name, db):
        return list(db["_networks"].find({"name": name}))

    def _get_id_from_name(self, name, db):
        metadata = self._get_metadata_from_name(name, db)
        if len(metadata) > 1:
            raise PandaHubError("Duplicate Network!")
        return None if len(metadata) == 0 else metadata[0]["_id"]

    def _network_with_name_exists(self, name, db):
        return self._get_id_from_name(name, db) is not None

    def _get_net_collections(self, db, with_areas=True):
        if with_areas:
            collection_filter = {'name': {'$regex': '^net_'}}
        else:
            collection_filter = {'name': {'$regex': '^net_.*(?<!area)$'}}
        return db.list_collection_names(filter=collection_filter)


    def _get_network_metadata(self, db, net_id):
        return db["_networks"].find_one({"_id": net_id})

    def _add_element_from_collection(self, net, db, element, net_id,
                                     filter=None, include_results=True,
                                     only_tables=None, geo_mode="string", variants=[], dtypes=None):
        if only_tables is not None and not element in only_tables:
            return
        if not include_results and element.startswith("res_"):
            return
        variants_filter = self.get_variant_filter(variants)
        filter_dict = {"net_id": net_id, **variants_filter}
        if filter is not None:
            if "$or" in filter_dict.keys() and "$or" in filter.keys():
                # if 'or' is in both filters create 'and' with
                # both to avoid override during filter merge
                filter_and = {"$and": [{"$or": filter_dict.pop("$or")}, {"$or": filter.pop("$or")}]}
                filter_dict = {**filter_dict, **filter, **filter_and}
            else:
                filter_dict = {**filter_dict, **filter}
        data = list(db[self._collection_name_of_element(element)].find(filter_dict))
        if len(data) == 0:
            return
        if dtypes is None:
            dtypes = db["_networks"].find_one({"_id": net_id}, projection={"dtypes"})
        df = pd.DataFrame.from_records(data, index="index")
        if element in dtypes:
            dtypes_found_columns = {
                column: dtype for column, dtype in dtypes[element].items() if column in df.columns
            }
            df = df.astype(dtypes_found_columns, errors="ignore")
        df.index.name = None
        df.drop(columns=["_id", "net_id"], inplace=True)
        df.sort_index(inplace=True)
        convert_geojsons(df, geo_mode)
        if "object" in df.columns:
            df["object"] = df["object"].apply(json_to_object)
        if not element in net or net[element].empty:
            net[element] = df
        else:
            new_rows = set(df.index) - set(net[element].index)
            if new_rows:
                net[element] = pd.concat([net[element], df.loc[list(new_rows)]])

    # -------------------------
    # Net element handling
    # -------------------------

    def get_net_value_from_db(self, net_name, element, element_index,
                              parameter, variant=None, project_id=None):
        if variant is not None:
            variant = int(variant)
        if project_id:
            self.set_active_project_by_id(project_id)
        self.check_permission("write")
        db = self._get_project_database()
        _id = self._get_id_from_name(net_name, db)
        collection = self._collection_name_of_element(element)
        variant_filter = self.get_variant_filter(variant)
        elements = list(db[collection].find({"index": element_index, "net_id": _id, **variant_filter}))
        if len(elements) == 1:
            element = elements[0]
        else:
            if len(elements) == 0:
                raise PandaHubError("Element doesn't exist", 404)
            else:
                raise PandaHubError("Multiple elements found", 404)
        dtypes = self._datatypes.get(element)
        if parameter not in element:
            raise PandaHubError("Parameter doesn't exist", 404)
        if dtypes is not None and parameter in dtypes:
            return dtypes[parameter](element[parameter])
        else:
            return element[parameter]

    def delete_net_element(self, net_id, element, element_index, variant=None, project_id=None):
        if variant is not None:
            variant = int(variant)
        if project_id:
            self.set_active_project_by_id(project_id)
        self.check_permission("write")
        db = self._get_project_database()
        collection = self._collection_name_of_element(element)
        element_filter = {"index": element_index, "net_id": int(net_id), **self.get_variant_filter(variant)}

        target = db[collection].find_one(element_filter)
        if target is None:
            # element does not exist in net
            return
        if variant and target["var_type"] == "base":
            db[collection].update_one({"_id": target["_id"]},
                                      {"$addToSet": {"not_in_var": variant}})
        else:
            db[collection].delete_one({"_id": target["_id"]})
        return target

    def set_net_value_in_db(self, net_id, element, element_index,
                            parameter, value, variant=None, project_id=None):
        logger.info(f"Setting  {parameter} = {value} in {element} with index {element_index} and variant {variant}")
        if variant is not None:
            variant = int(variant)
        if project_id:
            self.set_active_project_by_id(project_id)
        self.check_permission("write")
        db = self._get_project_database()
        dtypes = self._datatypes.get(element)
        if value is not None and dtypes is not None and parameter in dtypes:
            value = dtypes[parameter](value)
        collection = self._collection_name_of_element(element)

        element_filter = {"index": element_index, "net_id": int(net_id), **self.get_variant_filter(variant)}
        document = db[collection].find_one({**element_filter})
        if not document:
            raise UserWarning(f"No element '{element}' to change with index '{element_index}' in this variant")

        old_value = document.get(parameter, None)
        if old_value == value:
            logger.warning(f'Value "{value}" for "{parameter}" identical to database element - no change applied')
            return None
        if "." in parameter:
            key, subkey = parameter.split(".")
            document[key][subkey] = value
        else:
            document[parameter] = value

        if variant is None:
            db[collection].update_one({**element_filter, **self.base_variant_filter},
                                      {"$set": {parameter: value}})
        else:
            if document["var_type"] == "base":
                base_variant_id = document.pop("_id")
                db[collection].update_one({"_id": base_variant_id},
                                          {"$addToSet": {"not_in_var": variant}})
                document.update(var_type="change", variant=variant, changed_fields=[parameter])
                insert_result = db[collection].insert_one(document)
                document["_id"] = insert_result.inserted_id
            else:
                update_dict = {"$set": {parameter: value}, "$unset": {"not_in_var": ""}}
                if document["var_type"] == "change":
                    update_dict["$addToSet"] = {"changed_fields": parameter}
                db[collection].update_one({"_id": document["_id"]},
                                          update_dict)
        return {"document": document, parameter: {"previous": old_value, "current": value}}

    def set_object_attribute(self, net_id, element, element_index,
                             parameter, value, variant=None, project_id=None):
        if project_id:
            self.set_active_project_by_id(project_id)
        self.check_permission("write")
        db = self._get_project_database()
        dtypes = self._datatypes.get(element)
        if dtypes is not None and parameter in dtypes:
            value = dtypes[parameter](value)
        collection = self._collection_name_of_element(element)
        js = list(db[collection].find({"index": element_index, "net_id": net_id}))[0]
        obj = json_to_object(js["object"])
        setattr(obj, parameter, value)
        db[collection].find_one_and_update({"index": element_index, "net_id": net_id},
                                           {"$set": {"object._object": obj.to_json()}})

        element_filter = {"index": element_index, "net_id": int(net_id)}

        if variant is None:
            document = db[collection].find_one({**element_filter, **self.base_variant_filter})
            obj = json_to_object(document["object"])
            setattr(obj, parameter, value)
            db[collection].find_one_and_update(
                {**filter, **self.base_variant_filter}, {"$set": {"object._object": obj.to_json()}}
            )
        else:
            variant = int(variant)
            element_filter = {**element_filter, **self.get_variant_filter(variant)}
            document = db[collection].find_one({**element_filter})
            if not document:
                raise UserWarning(f"No element '{element}' to change with index '{element_index}' in this variant")
            obj = json_to_object(document["object"])
            setattr(obj, parameter, value)
            if document["var_type"] == "base":
                base_variant_id = document.pop("_id")
                db[collection].update_one({"_id": base_variant_id},
                                          {"$addToSet": {"not_in_var": variant}})
                document["object"]["_object"] = obj
                document["var_type"] = "change"
                db[collection].insert_one(document)
            else:
                db[collection].update_one({"_id": document["_id"]},
                                          {"$set": {"object._object": obj}})

    def create_element_in_db(self, net_id, element, element_index, data, variant=None, project_id=None):
        logger.info(f"Creating element {element} with index {element_index} and variant {variant}, data: {data}")
        if project_id:
            self.set_active_project_by_id(project_id)
        self.check_permission("write")
        db = self._get_project_database()
        element_data = {**data, **{"index": element_index, "net_id": int(net_id)}}
        if not variant:
            element_data.update(var_type="base", not_in_var=[])
        else:
            element_data.update(var_type="addition", variant=int(variant))
        self._add_missing_defaults(db, net_id, element, element_data)
        self._ensure_dtypes(element, element_data)
        collection = self._collection_name_of_element(element)
        insert_result = db[collection].insert_one(element_data)
        element_data["_id"] = insert_result.inserted_id
        return element_data

    def create_elements_in_db(self, net_id: int, element_type: str, elements_data: list, project_id=None,
                              variant=None):
        if project_id:
            self.set_active_project_by_id(project_id)
        self.check_permission("write")
        db = self._get_project_database()
        if not variant:
            var_data = {"var_type": "base", "not_in_var": []}
        else:
            var_data = {"var_type": "addition", "variant": int(variant)}
        data = []
        for elm_data in elements_data:
            self._add_missing_defaults(db, net_id, element_type, elm_data)
            self._ensure_dtypes(element_type, elm_data)
            data.append({**elm_data, **var_data, "net_id": net_id})
        collection = self._collection_name_of_element(element_type)
        insert_result = db[collection].insert_many(data)
        return [[z[0].update(_id=z[1]) for z in zip(data, insert_result.inserted_ids)]]

    def _add_missing_defaults(self, db, net_id, element_type, element_data):
        func_str = f"create_{element_type}"
        if not hasattr(pp, func_str):
            return
        create_func = getattr(pp, func_str)
        sig = signature(create_func)
        params = sig.parameters

        for par, data in params.items():
            if par in ["net", "kwargs"]:
                continue
            if par in element_data:
                continue
            if data.default == _empty:
                continue
            element_data[par] = data.default

        if element_type in ["line", "trafo", "trafo3w"]:
            # add standard type values
            std_type = element_data["std_type"]
            net_doc = db["_networks"].find_one({"_id": net_id})
            if net_doc is not None:
#                 std_types = json.loads(net_doc["data"]["std_types"], cls=io_pp.PPJSONDecoder)[element_type]
                std_types = net_doc["data"]["std_types"]
                if std_type in std_types:
                    element_data.update(std_types[std_type])

            # add needed parameters not defined in standard type
            if element_type == "line":
                if "g_us_per_km" not in element_data:
                    element_data["g_us_per_km"] = 0

    def _ensure_dtypes(self, element, data):
        dtypes = self._datatypes.get(element)
        if dtypes is None:
            return
        for key, val in data.items():
            if not val is None and key in dtypes and not dtypes[key] == object:
                data[key] = dtypes[key](val)

    # -------------------------
    # Variants
    # -------------------------

    def create_variant(self, data):
        db = self._get_project_database()
        net_id = int(data["net_id"])
        max_index = list(db["variant"].find({"net_id": net_id},
                                            projection={"_id": 0, "index": 1}).sort("index", -1).limit(1))
        if not max_index:
            index = 1
            for coll in self._get_net_collections(db):
                update = {"$set": {"var_type": "base",
                                   "not_in_var": []}}
                db[coll].update_many({}, update)

        else:
            index = int(max_index[0]["index"]) + 1

        data["index"] = index

        if data.get("default_name") is not None and data.get("name") is None:
            data["name"] = data.pop("default_name") + " " + str(index)

        db["variant"].insert_one(data)
        del data["_id"]

        return data

    def delete_variant(self, net_id, index):
        db = self._get_project_database()
        collection_names = self._get_net_collections(db)
        for coll in collection_names:
            # remove references to deleted objects
            db[coll].update_many({"net_id": net_id, "var_type": "base", "not_in_var": index},
                                 {"$pull": {"not_in_var": index}})
            # remove changes and additions
            db[coll].delete_many({"net_id": net_id, "var_type": {"$in": ["change", "addition"]},
                                  "variant": index})
        # delete variant
        db["variant"].delete_one({"net_id": net_id, "index": index})

    def update_variant(self, net_id, index, data):
        db = self._get_project_database()
        db["variant"].update_one({"net_id": net_id, "index": index}, {"$set": data})

    def get_variant_filter(self, variants):
        """
        Creates a mongodb query filter to retrieve pandapower elements for the given variant(s).

        Parameters
        ----------
        variants : int or list of int or None
                None or an empty list represent the base variant, ints specify variant indices.

        Returns
        -------
        dict
            mongodb query filter for the given variant(s)
        """
        if type(variants) is list and variants:
            if len(variants) > 1:
                variants = [int(var) for var in variants] # make sure variants are of type int
                return {"$or": [{"var_type": "base", "not_in_var": {"$nin": variants}},
                                {"var_type": {"$in": ["change", "addition"]}, "variant": {"$in": variants}}]}
            else:
                variants = variants[0]
        if variants:
            variants = int(variants)
            return {"$or": [{"var_type": "base", "not_in_var": {"$ne": variants}},
                            {"var_type": {"$in": ["change", "addition"]}, "variant": variants}]}
        else:
            return self.base_variant_filter

    # -------------------------
    # Bulk operations
    # -------------------------

    def bulk_write_to_db(self, data, collection_name="tasks", global_database=True, project_id=None):
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
        if project_id:
            self.set_active_project_by_id(project_id)

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
            i += 1

        db[collection_name].bulk_write(operations)

    # -------------------------
    # Timeseries
    # -------------------------

    def write_timeseries_to_db(self,
                               timeseries,
                               data_type,
                               ts_format="timestamp_value",
                               compress_ts_data=False,
                               global_database=False,
                               collection_name="timeseries",
                               project_id=None,
                               **kwargs):
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
                                              ts_format=ts_format,
                                              compress_ts_data=compress_ts_data,
                                              **kwargs)
        db[collection_name].find_one_and_replace(
            {"_id": document["_id"]},
            document,
            upsert=True
        )
        logger.debug("document with _id {document['_id']} added to database")
        if kwargs.get("return_id"):
            return document["_id"]
        return None

    def bulk_write_timeseries_to_db(self, timeseries, data_type,
                                    meta_frame=None,
                                    ts_format="timestamp_value",
                                    compress_ts_data=False,
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
                                             data_type,
                                             ts_format=ts_format,
                                             compress_ts_data=compress_ts_data,
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
        # logger.info("document updated in database")

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

        # logger.debug(f"{len(documents)} documents added to database")

    def get_timeseries_from_db(self, filter_document={}, timestamp_range=None,
                               ts_format="timestamp_value",
                               compressed_ts_data=False,
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
        if not compressed_ts_data:
            if ts_format == "timestamp_value":
                if timestamp_range:
                    pipeline.append({"$project": {"timeseries_data": {"$filter": {"input": "$timeseries_data",
                                                                                  "as": "timeseries_data",
                                                                                  "cond": {"$and": [{"$gte": [
                                                                                      "$$timeseries_data.timestamp",
                                                                                      timestamp_range[0]]},
                                                                                      {"$lt": [
                                                                                          "$$timeseries_data.timestamp",
                                                                                          timestamp_range[
                                                                                              1]]}]}}}}})
                pipeline.append({"$addFields": {"timestamps": "$timeseries_data.timestamp",
                                                "values": "$timeseries_data.value"}})
                if include_metadata:
                    pipeline.append({"$project": {"timeseries_data": 0}})
                else:
                    pipeline.append({"$project": {"timestamps": 1,
                                                  "values": 1,
                                                  "_id": 0}})
            elif ts_format == "array":
                if not include_metadata:
                    pipeline.append({"$project": {"timeseries_data": 1}})
        else:
            if not include_metadata:
                pipeline.append({"$project": {"timeseries_data": 1}})
        data = list(db[collection_name].aggregate(pipeline))
        if len(data) == 0:
            raise PandaHubError("no documents matching the provided filter found", 404)
        elif len(data) > 1:
            raise PandaHubError("multiple documents matching the provided filter found")
        else:
            data = data[0]
        if compressed_ts_data:
            timeseries_data = decompress_timeseries_data(data["timeseries_data"], ts_format)
        else:
            if ts_format == "timestamp_value":
                timeseries_data = pd.Series(data["values"],
                                            index=data["timestamps"],
                                            dtype="float64")
            elif ts_format == "array":
                timeseries_data = data["timeseries_data"]
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
        projection = {"$project": {"timeseries_data": 0}}
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
        if len(meta_before) > 1:  # TODO is this the desired behaviour? Needs to specified
            raise PandaHubError
        meta_copy = {**meta_before.iloc[0].to_dict(), **add_meta}
        # write new metadata to mongo db
        db[collection_name].find_one_and_replace({"_id": meta_before.index[0]},
                                                 meta_copy, upsert=True)
        return meta_copy

    def multi_get_timeseries_from_db(self, filter_document={},
                                     timestamp_range=None,
                                     exclude_timestamp_range=None,
                                     include_metadata=False,
                                     ts_format="timestamp_value",
                                     compressed_ts_data=False,
                                     global_database=False, collection_name="timeseries",
                                     project_id=None, **kwargs):
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

        if len(match_filter) > 0:
            pipeline = [{"$match": {"$and": match_filter}}]
        else:
            pipeline = []
        if timestamp_range:
            projection = {"timeseries_data": {"$filter": {"input": "$timeseries_data",
                                                          "as": "timeseries_data",
                                                          "cond": {"$and": [{"$gte": ["$$timeseries_data.timestamp",
                                                                                      timestamp_range[0]]},
                                                                            {"$lt": ["$$timeseries_data.timestamp",
                                                                                     timestamp_range[1]]}]}}}}
            pipeline.append({"$project": projection})
        if exclude_timestamp_range:
            projection = {"timeseries_data": {"$filter": {"input": "$timeseries_data",
                                                          "as": "timeseries_data",
                                                          "cond": {"$or": [{"$lt": ["$$timeseries_data.timestamp",
                                                                                    timestamp_range[0]]},
                                                                           {"$gte": ["$$timeseries_data.timestamp",
                                                                                     timestamp_range[1]]}]}}}}
            pipeline.append({"$project": projection})
        if not include_metadata:
            pipeline.append({"$project": {"timeseries_data": 1}})

        timeseries = []
        for ts in db[collection_name].aggregate(pipeline):
            if len(ts["timeseries_data"]) == 0:
                continue
            data = ts["timeseries_data"]
            if compressed_ts_data:
                timeseries_data = decompress_timeseries_data(data, ts_format)
                ts["timeseries_data"] = timeseries_data
            else:
                if ts_format == "timestamp_value":
                    timeseries_data = pd.DataFrame(ts["timeseries_data"])
                    timeseries_data.set_index("timestamp", inplace=True)
                    timeseries_data.index.name = None
                    ts["timeseries_data"] = timeseries_data.value
            if include_metadata:
                timeseries.append(ts)
                if exclude_timestamp_range is not None or timestamp_range is not None:
                    # TODO: Second query to get the metadata, since metadata is not returned if a projection on the subfield is used
                    metadata = db[collection_name].find_one({"_id": ts["_id"]}, projection={"timeseries_data": 0})
                    ts.update(metadata)
            else:
                if ts_format == "timestamp_value":
                    timeseries.append(ts["timeseries_data"].values)
                elif ts_format == "array":
                    timeseries.append(ts["timeseries_data"])
        if include_metadata:
            return timeseries
        else:
            if ts_format == "timestamp_value":
                return pd.DataFrame(np.array(timeseries).T, index=timeseries_data.index)
            return pd.DataFrame(np.array(timeseries).T)

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
                                                          "cond": {"$and": [{"$gte": ["$$timeseries_data.timestamp",
                                                                                      timestamp_range[0]]},
                                                                            {"$lt": ["$$timeseries_data.timestamp",
                                                                                     timestamp_range[1]]}]}}}}
            projection = {**projection, **custom_projection}
            pipeline.append({"$project": projection})
        if exclude_timestamp_range:
            projection = {"timeseries_data": {"$filter": {"input": "$timeseries_data",
                                                          "as": "timeseries_data",
                                                          "cond": {"$or": [{"$lt": ["$$timeseries_data.timestamp",
                                                                                    timestamp_range[0]]},
                                                                           {"$gte": ["$$timeseries_data.timestamp",
                                                                                     timestamp_range[1]]}]}}}}
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
