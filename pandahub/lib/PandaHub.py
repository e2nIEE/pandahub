# -*- coding: utf-8 -*-
import builtins
import json
import logging
import time
import warnings
from collections.abc import Callable
from functools import reduce
from inspect import _empty, signature
from itertools import chain
from operator import getitem
from types import NoneType
from typing import Optional, Union, TypeAlias
from uuid import UUID

import numpy as np
import pandapipes as pps
import pandapower as pp
import pandapower.io_utils as io_pp
import pandas as pd
import pymongoarrow.monkey
from bson.errors import InvalidId
from bson.objectid import ObjectId
from packaging import version
from pandapipes import BranchComponent, FromSerializableRegistryPpipe
from pandapipes import from_json_string as from_json_pps
from pandapipes.component_models import NodeElementComponent
from pymongo import MongoClient, ReplaceOne, ReturnDocument
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import ConnectionFailure, DuplicateKeyError

from pandahub import __version__
from pandahub.lib import get_mongo_client
from pandahub.lib.database_toolbox import (
    convert_element_to_dict,
    convert_geojsons,
    convert_timeseries_to_subdocuments,
    create_timeseries_document,
    decompress_timeseries_data,
    get_dtypes,
    get_metadata_for_timeseries_collections,
    json_to_object,
    serialize_object_data,
)
from pandahub.lib.datatypes import DATATYPES
from pandahub.lib.mongodb_indexes import MONGODB_INDEXES
from pandahub.lib.settings import pandahub_settings as ph_settings

logger = logging.getLogger(__name__)
pymongoarrow.monkey.patch_all()

# -------------------------
# Typing
# -------------------------

ProjectID: TypeAlias = str | int | ObjectId
SettingsValue: TypeAlias = str | int | float | list | dict
PandaNet: TypeAlias = pp.pandapowerNet | pps.pandapipesNet
PandaPowerNet: TypeAlias = pp.pandapowerNet
PandaPipesNet: TypeAlias = pps.pandapipesNet

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

def validate_variant_type(variant: int | None):
    """Raise a ValueError if variant is not int | None."""
    if not isinstance(variant, (int, NoneType)):
        msg = f"variant must be int or None, but got {variant} of type {type(variant)}"
        raise ValueError(msg)


def re_arg(kwarg_map: dict[str, str]) -> Callable:
    def decorator(func: Callable) -> Callable:
        def wrapped(*args, **kwargs):
            new_kwargs = {}
            for k, v in kwargs.items():
                if k in kwarg_map:
                    print(f"DEPRECATION WARNING: keyword argument '{k}' is no longer valid. Use '{kwarg_map[k]}' instead.")
                new_kwargs[kwarg_map.get(k, k)] = v
            return func(*args, **new_kwargs)
        return wrapped
    return decorator


class PandaHub:
    permissions = {
        "read": ["owner", "developer", "guest"],
        "write": ["owner", "developer"],
        "user_management": ["owner"],
        "delete_project": ["owner"],
    }

    # -------------------------
    # Initialization
    # -------------------------

    def __init__(
        self,
        connection_url: str = ph_settings.mongodb_url,
        connection_user: str = ph_settings.mongodb_user,
        connection_password: str = ph_settings.mongodb_password,
        check_server_available: bool = False,
        user_id: str = None,
        datatypes: dict = DATATYPES,
        mongodb_indexes: dict = MONGODB_INDEXES,
        elements_without_vars: list | tuple = None,
        create_indexes_with_project: bool = ph_settings.create_indexes_with_project,
    ):
        self._datatypes = datatypes
        self.mongodb_indexes = mongodb_indexes
        self.mongo_client = get_mongo_client(
            connection_url=connection_url,
            connection_user=connection_user,
            connection_password=connection_password,
        )
        self._elements_without_vars = (
            ["variant"] if elements_without_vars is None else elements_without_vars
        )
        self.create_indexes_with_project = create_indexes_with_project

        self.mongo_client_global_db = None
        self.active_project = None
        self.user_id = user_id
        self.base_variant_filter = {
            "$or": [
                {"var_type": "base"},
                {"var_type": None},
                {"var_type": np.nan},
            ]
        }
        if check_server_available:
            self.server_is_available()


    @property
    def project_db(self) -> Database:
        """Get the database of the current active project"""
        return self.get_project_database()

    @property
    def mgmt_db(self) -> Database:
        """Get the user_management database"""
        return self.mongo_client["user_management"]

    @property
    def users_collection(self) -> Collection:
        """Get the users collection"""
        return self.mongo_client["user_management"]["users"]

    @property
    def projects_collection(self) -> Collection:
        """Get the projects collection"""
        return self.mongo_client["user_management"]["projects"]

    @property
    def active_project_id(self) -> str | None:
        """Get the active project id as string."""
        return None if self.active_project is None else str(self.active_project["_id"])

    # -------------------------
    # Database connection checks
    # -------------------------

    def server_is_available(self):
        """Check if the MongoDB server is available."""
        try:
            self.mongo_client.admin.command("ping")
            return True
        except ConnectionFailure as e:
            logger.exception(e)
            return False

    def close(self, force=False):
        """Close the database connection."""
        if ph_settings.pandahub_global_db_client and not force:
            msg = "Closing the global MongoClient instance will break all subsequent database connections. If this is intentional, use force=True"
            raise PandaHubError(msg)
        self.mongo_client.close()
    # -------------------------
    # Permission check
    # -------------------------

    def check_permission(self, permission):
        if self.active_project is None:
            raise PandaHubError("No project is activated")
        if not self.has_permission(permission):
            raise PandaHubError(
                "You don't have {} rights on this project".format(permission), 403
            )

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
        if str(user["_id"]) != self.user_id:
            self.check_permission("user_management")
        return user

    def _get_user(self):
        user_mgmnt_db = self.mongo_client["user_management"]
        user = user_mgmnt_db["users"].find_one(
            {"_id": UUID(self.user_id)}, projection={"hashed_password": 0}
        )
        return user

    # -------------------------
    # Project handling
    # -------------------------

    def create_project(
        self,
        name,
        settings=None,
        realm=None,
        metadata=None,
        project_id=None,
        activate=True,
        additional_project_data=None,
    ):
        if self.project_exists(name, realm):
            raise PandaHubError("Project already exists")
        if settings is None:
            settings = {}
        if metadata is None:
            metadata = {}
        if additional_project_data is None:
            additional_project_data = {}
        project_data = {
            "name": name,
            "realm": realm,
            "settings": settings,
            "metadata": metadata,
            "version": __version__,
            **additional_project_data,
        }
        if project_id:
            project_data["_id"] = project_id
        if self.user_id is not None:
            project_data["users"] = {self.user_id: "owner"}
        self.mongo_client["user_management"]["projects"].insert_one(project_data)
        if self.create_indexes_with_project:
            self._create_mongodb_indexes(project_data["_id"])
        if activate:
            self.set_active_project_by_id(project_data["_id"])
        return project_data

    def delete_project(self, i_know_this_action_is_final: bool = False, project_id: ProjectID | None = None):
        """Delete a project, checking the required permission."""
        if project_id is None:
            project_id = self.active_project["_id"]
        else:
            self.set_active_project_by_id(project_id)
        self.check_permission("delete_project")
        if not i_know_this_action_is_final:
            raise PandaHubError(
                "Calling this function will delete the whole project and all the nets stored within. It can not be reversed. Add 'i_know_this_action_is_final=True' to confirm."
            )
        self._delete_project(project_id)

    def _delete_project(self, project_id: ProjectID):
        """Delete project by id."""
        self.mongo_client.drop_database(str(project_id))
        self.mongo_client.user_management.projects.delete_one({"_id": project_id})
        self.active_project = None

    def get_projects(
        self, query_filter: dict | None = None, projection: dict | list | None = None, realm: str | None = None
    ) -> list[dict]:
        """Return a list of projects the active user has access to.

        If the active user is superuser, project documents for all users will be returned. The project documents additionally
        contain a field "permission" with the user's permission value, and "_id" is mapped to "id" and cast to string.

        If no user is active, only projects not under user management will be returned.

        Parameters
        ----------
        query_filter:
            Return only projects matching the given filter. User management / realm filtering is always applied additionally.
        projection:
            mongodb query projection to filter fields on the returned project documents.
        realm:
            Return only projects with matching realm.

        Returns
        -------
        list[dict]
            A list of project documents the user has access to.
        """
        projection = {} if projection is None else projection
        query_filter = {} if query_filter is None else query_filter
        if self.user_id is not None:
            user = self._get_user()
            if not user["is_superuser"]:
                query_filter |= {f"users.{self.user_id}": {"$exists": True}}
        else:
            query_filter |= {"users": {"$exists": False}}
        if realm is not None:
            query_filter["realm"] = realm
        projects = self.projects_collection.find(query_filter, projection).to_list()
        for project in projects:
            if "_id" in project:
                project["id"] = str(project.pop("_id"))
            if self.user_id and "users" in project:
                role = project["users"].get(self.user_id)
                project["permissions"] = self.get_permissions_by_role(role)
            else:
                project["permissions"] = None
        return projects

    def get_project_names(self) -> list[str]:
        """Return a list with names the current user has access to."""
        return [project["name"] for project in self.get_projects(projection={"_id": 0, "name": 1})]


    def set_active_project(self, project_name: str, realm=None):
        projects = self.get_projects(query_filter={"name":project_name},realm=realm, projection=["_id"])
        if len(projects) == 0:
            raise PandaHubError("Project not found!", 404)
        elif len(projects) > 1:
            raise PandaHubError("Multiple projects found!")
        else:
            project_id = projects[0]["id"]
            self.set_active_project_by_id(project_id)

    def set_active_project_by_id(self, project_id: ProjectID):
        try:
            project_id = ObjectId(project_id)
        except InvalidId:
            pass
        self.active_project = self._get_project_document({"_id": project_id})
        if self.active_project is None:
            raise PandaHubError("Project not found!", 404)

    def rename_project(self, project_name:str):
        self.has_permission("write")
        project_collection = self.mongo_client["user_management"].projects
        realm = self.active_project["realm"]
        if self.project_exists(project_name, realm):
            raise PandaHubError("Can't rename - project with this name already exists")
        project_collection.update_one(
            {"_id": self.active_project["_id"]}, {"$set": {"name": project_name}}
        )
        self.set_active_project(project_name, realm)

    def change_realm(self, realm):
        self.has_permission("write")
        project_collection = self.mongo_client["user_management"].projects
        project_name = self.active_project["name"]
        if self.project_exists(project_name, realm):
            raise PandaHubError(
                "Can't change realm - project with this name already exists"
            )
        project_collection.update_one(
            {"_id": self.active_project["_id"]}, {"$set": {"realm": realm}}
        )
        self.set_active_project(project_name, realm)

    def lock_project(self)-> bool:
        """Lock active project with the current user.

        Returns True if the project was successfully locked for current active user, and False if another user already held a lock on the project.
        """
        result = self.projects_collection.find_one_and_update(
            {"_id": self.active_project["_id"], "locked_by":{"$in":[self.user_id, None]}},
            {"$set": {"locked_by": self.user_id}}
        )
        return result is not None


    def unlock_project(self)-> bool:
        """Unlock active project with the current user.

        Returns True if the project was successfully unlocked with the current user, and False if the project lock is held by another user.
        """
        result = self.projects_collection.find_one_and_update(
            {"_id": self.active_project["_id"], "locked_by": {"$in": [self.user_id, None]}},
            {"$set": {"locked_by": None}},
        )
        return result is not None

    def force_unlock_project(self, project_id):
        project = self.projects_collection.find_one({"_id": ObjectId(project_id)})
        user = self._get_user()
        if project is None:
            return None
        if (
            "users" not in project
            or self.user_id in project["users"].keys()
            or user["is_superuser"]
        ):
            return self.projects_collection.update_one(
                {"_id": ObjectId(project_id)},
                {"$set": {"locked_by": None}},
            )
        else:
            raise PandaHubError("You don't have rights to access this project", 403)

    def project_exists(self, project_name:Optional[str]=None, realm=None):
        project = self.projects_collection.find_one({"name": project_name, "realm": realm})
        return project is not None

    def _get_project_document(self, filter_dict: dict) -> Optional[dict]:
        projects = self.projects_collection.find(filter_dict).to_list()
        if len(projects) == 0:  # project doesn't exist
            return None
        if len(projects) > 1:
            raise PandaHubError(
                "Duplicate Project detected. This should never happen if you create projects through the API. Remove duplicate projects manually in the database."
            )
        project_doc = projects[0]
        if "users" not in project_doc:
            return project_doc  # project is not user protected

        user = self._get_user()
        if not user["is_superuser"] and self.user_id not in project_doc["users"].keys():
            raise PandaHubError("You don't have rights to access this project", 403)
        locked_by = project_doc.get("locked_by")
        if locked_by is not None and locked_by != self.user_id:
            raise PandaHubError("Project is locked by another user")
        return project_doc

    def _get_project_database(self) -> Database:
        return self.get_project_database()

    def get_project_database(self, collection: Optional[str] = None) -> Database:
        """
        Get the database for the current active project.

        Parameters
        ----------
        collection or None
            Name of document collection (DEPRECATED)

        Returns
        -------
        pymongo.database.Database
        """
        if self.active_project is None:
            msg = "Can not access project database - no project active!"
            raise PandaHubError(msg)
        project_id = str(self.active_project["_id"])
        if collection is None:
            return self.mongo_client[project_id]
        logger.warning(f"Passing a collection to get_project_database is deprecated. Use get_project_collection instead!")
        return self.get_project_collection(collection)

    def get_project_collection(self, collection_name: str) -> Collection:
        """
        Get a MongoClient instance connected to the database for the current active project, optionally set to the given collection.

        Parameters
        ----------
        collection_name
            Name of document collection

        Returns
        -------
        pymongo.collection.Collection
        """
        return self.get_project_database()[collection_name]


    def _get_global_database(self) -> Database:
        if (
            self.mongo_client_global_db is None
            and ph_settings.mongodb_global_database_url is not None
        ):
            mongo_client_args = {
                "host": ph_settings.mongodb_global_database_url,
                "uuidRepresentation": "standard",
            }
            if ph_settings.mongodb_global_database_user:
                mongo_client_args |= {
                    "username": ph_settings.mongodb_global_database_user,
                    "password": ph_settings.mongodb_global_database_password,
                }
            self.mongo_client_global_db = MongoClient(**mongo_client_args)
        if self.mongo_client_global_db is None:
            return self.mongo_client["global_data"]
        else:
            return self.mongo_client_global_db["global_data"]

    def get_network_ids(self) -> list[int]:
        """
        Retrieve the id's of all networks in the active project.

        Returns
        -------
        list
            network ids
        """
        return self.get_project_collection("_networks").find({}, {"_id:": 1}).distinct("_id")

    def get_project_version(self):
        return self.active_project.get("version", "0.2.2")

    def upgrade_project_to_latest_version(self):
        # TODO check that user has right to write user_management
        # TODO these operations should be encapsulated in a transaction in order to avoid
        #      inconsistent Database states in case of occuring errors

        if self.active_project["version"] == __version__:
            return

        if version.parse(self.get_project_version()) < version.parse("0.2.3"):
            db = self.project_db
            all_collection_names = db.list_collection_names()
            old_net_collections = [
                name
                for name in all_collection_names
                if not name.startswith("_")
                and not name == "timeseries"
                and not name.startswith("net_")
            ]

            for element in old_net_collections:
                db[element].rename(self._collection_name_of_element(element))
            # for all networks
            for d in db["_networks"].find({}, projection={"sector": 1, "data": 1}).to_list():
                # load old format
                if d.get("sector", "power") == "power":
                    data = dict(
                        (k, json.loads(v, cls=io_pp.PPJSONDecoder))
                        for k, v in d["data"].items()
                    )
                else:
                    data = dict((k, from_json_pps(v)) for k, v in d["data"].items())
                # save new format
                for key, dat in data.items():
                    try:
                        json.dumps(dat)
                    except:
                        dat = f"serialized_{json.dumps(data, cls=io_pp.PPJSONEncoder)}"
                    data[key] = dat
                db["_networks"].update_one({"_id": d["_id"]}, {"$set": {"data": data}})

            logger.info(
                f"upgraded projekt '{self.active_project['name']}' from version"
                f" {self.get_project_version()} to version 0.2.3"
            )

        project_collection = self.mongo_client["user_management"].projects
        project_collection.update_one(
            {"_id": self.active_project["_id"]}, {"$set": {"version": __version__}}
        )
        self.active_project["version"] = __version__

    # -------------------------
    # Project settings and metadata
    # -------------------------

    def get_project_settings(self, project_id=None):
        if project_id:
            self.set_active_project_by_id(project_id)
        self.check_permission("read")
        return self.active_project["settings"]

    def get_project_setting_value(self, setting, project_id=None):
        """
        Retrieve the value of a setting.

        Parameters
        ----------
        setting: str
            The setting to retrieve - use dot notation to index into nested settings.
        project_id: str or None
            The project id to retrieve the setting from. Applies to the current active project if None.
        Returns
        -------
        Settings value or None
            The settings' value if set in the database or None if the setting is not defined.
        """
        if project_id:
            self.set_active_project_by_id(project_id)
        self.check_permission("read")
        _id = self.active_project["_id"]
        project_collection = self.mongo_client["user_management"]["projects"]
        setting_string = f"settings.{setting}"
        setting = project_collection.find_one(
            {"_id": _id}, {"_id": 0, setting_string: 1}
        )
        try:
            return reduce(getitem, setting_string.split("."), setting)
        except KeyError:
            return None

    def set_project_settings(self, settings, project_id=None):
        if project_id:
            self.set_active_project_by_id(project_id)
        self.check_permission("write")
        _id = self.active_project["_id"]
        new_settings = {**self.active_project["settings"], **settings}
        project_collection = self.mongo_client["user_management"]["projects"]
        project_collection.update_one(
            {"_id": _id}, {"$set": {"settings": new_settings}}
        )
        self.active_project["settings"] = new_settings

    def set_project_settings_value(self, parameter, value, project_id=None):
        if project_id:
            self.set_active_project_by_id(project_id)
        self.check_permission("write")
        _id = self.active_project["_id"]
        project_collection = self.mongo_client["user_management"]["projects"]
        setting_string = "settings.{}".format(parameter)
        project_collection.update_one({"_id": _id}, {"$set": {setting_string: value}})
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
            {"_id": project_data["_id"]},
            [
                {
                    "$unset": "metadata"
                },  # deletion needed because set won't delete not existing fields
                {"$set": {"metadata": update_metadata}},
            ],
        )
        self.active_project["metadata"] = update_metadata

    # -------------------------
    # Project user management
    # -------------------------

    def get_project_users(self):
        self.check_permission("user_management")
        project_users = self.active_project["users"]
        users = self.mongo_client["user_management"]["users"].find(
            {"_id": {"$in": [UUID(user_id) for user_id in project_users.keys()]}}
        )
        enriched_users = []
        for user in users:
            enriched_users.append(
                {"email": user["email"], "role": project_users[str(user["_id"])]}
            )
        return enriched_users

    def add_user_to_project(self, email, role):
        self.check_permission("user_management")
        user = self.get_user_by_email(email)
        if user is None:
            return
        user_id = user["_id"]
        self.mongo_client["user_management"]["projects"].update_one(
            {"_id": self.active_project["_id"]}, {"$set": {f"users.{user_id}": role}}
        )
        return user

    def change_project_user_role(self, email, new_role):
        self.check_permission("user_management")
        user = self.get_user_by_email(email)
        if user is None:
            return
        user_id = user["_id"]
        self.mongo_client["user_management"]["projects"].update_one(
            {"_id": self.active_project["_id"]},
            {"$set": {f"users.{user_id}": new_role}},
        )

    def remove_user_from_project(self, email: str) -> None:
        """
        Remove a user from the active project.

        Users can always remove themselves from a project, regardless of their project role. Removing other users from the currently active
        project requires the "user_management" role.

        The project will be deleted if it had no other members.

        Parameters
        ----------
        email: str
            The email adress of the user to remove from project.

        Returns
        -------
        None
        """
        user = self.get_user_by_email(email)
        if user is None:
            return None
        user_id = user["_id"]
        # check permission only if the user tries to remove a different user to
        # allow leaving a project with just 'read' permission
        removing_own_user = str(user_id) == self.user_id
        if not removing_own_user:
            self.check_permission("user_management")
        self._remove_user_from_project(user_id, self.active_project["_id"])
        if removing_own_user:
            self.active_project = None
        return None

    def _remove_user_from_project(self, user_id: UUID | str, project_id: ProjectID) -> None:
        """Remove the user id from the project document and delete project if it has no other users.."""
        project = self.projects_collection.find_one_and_update({"_id": project_id},
                                                               {"$unset": {f"users.{user_id}": ""}},
                                                               ["users"],
                                                               return_document=ReturnDocument.AFTER)
        if len(project["users"]) == 0:
            self._delete_project(project_id)

    # -------------------------
    # Net handling
    # -------------------------

    def get_all_nets_metadata_from_db(self, project_id=None):
        if project_id:
            self.set_active_project_by_id(project_id)
        self.check_permission("read")
        db = self._get_project_database()
        return db["_networks"].find().to_list()


    def get_network_by_name(
        self,
        name,
        include_results=True,
        only_tables=None,
        project_id=None,
        geo_mode="string",
        variant=None,
        convert=True,
    ):
        if project_id:
            self.set_active_project_by_id(project_id)
        self.check_permission("read")
        db = self._get_project_database()
        net_id = self._get_net_id_from_name(name, db)
        if net_id is None:
            return None
        return self.get_network(
            net_id, include_results, only_tables, geo_mode=geo_mode, variant=variant, convert=convert
        )

    def get_network(
        self,
        net_id,
        include_results=True,
        only_tables=None,
        convert=True,
        geo_mode="string",
        variant=None,
    ):
        self.check_permission("read")
        return self._get_net_from_db_by_id(
            net_id,
            include_results,
            only_tables,
            convert=convert,
            geo_mode=geo_mode,
            variant=variant,
        )

    def _get_net_from_db_by_id(
        self,
        net_id,
        include_results=True,
        only_tables=None,
        convert=True,
        geo_mode="string",
        variant=None,
    ):
        db = self._get_project_database()
        meta = self._get_network_metadata(db, net_id)

        package = pp if meta.get("sector", "power") == "power" else pps
        net = package.create_empty_network()

        # add all elements that are stored as dataframes
        collection_names = self.get_net_collections(db)
        for collection_name in collection_names:
            el = self._element_name_of_collection(collection_name)
            self._add_element_from_collection(
                net,
                db,
                el,
                net_id,
                include_results=include_results,
                only_tables=only_tables,
                geo_mode=geo_mode,
                variant=variant,
            )
        # add data that is not stored in dataframes
        self.deserialize_and_update_data(net, meta)

        if convert:
            package.convert_format(net)

        return net

    def deserialize_and_update_data(self, net, meta):
        registry = io_pp.FromSerializableRegistry if meta.get("sector", "power") == "power" \
            else FromSerializableRegistryPpipe
        if version.parse(self.get_project_version()) <= version.parse("0.2.3"):
            data = dict((k, json.loads(v, cls=io_pp.PPJSONDecoder, registry_class=registry))
                        for k, v in meta["data"].items())
            net.update(data)
        else:
            for key, value in meta["data"].items():
                if type(value) == str and value.startswith("serialized_"):
                    value = json.loads(value[11:], cls=io_pp.PPJSONDecoder, registry_class=registry)
                net[key] = value


    def get_subnet_by_name(
        self,
        name,
        node_filter=None,
        include_results=True,
        add_edge_branches=True,
        geo_mode="string",
        variant=None,
        additional_filters: dict[
            str, Callable[[PandaNet], dict]
        ] | None = None,
    ) -> PandaNet | (tuple[PandaNet, list] | None):
        self.check_permission("read")
        db = self._get_project_database()
        net_id = self._get_net_id_from_name(name, db)
        if net_id is None:
            return None
        return self.get_subnet(
            net_id,
            node_filter=node_filter,
            include_results=include_results,
            add_edge_branches=add_edge_branches,
            geo_mode=geo_mode,
            variant=variant,
            additional_filters=additional_filters,
        )


    @re_arg({"bus_filter": "node_filter"})
    def get_subnet(
        self,
        net_id,
        node_filter=None,
        line_filter=None,
        include_results=True,
        add_edge_branches=True,
        geo_mode="string",
        variant=None,
        ignore_elements=tuple([]),
        additional_filters: dict[str, Callable[[PandaNet], dict]] | None = None,
        additional_edge_filters: dict[
            str, tuple[
                list[str] | tuple[str, ...] | None,
                bool,
                dict | None,
                Callable[[PandaNet], dict] | None
            ]] | None = None,
        *,
        return_edge_branch_nodes: bool = False
    ) -> PandaNet | (tuple[PandaNet, list]):
        db = self._get_project_database()
        meta = self._get_network_metadata(db, net_id)
        dtypes = meta["dtypes"]
        sector = meta["sector"]
        is_power = sector == "power"

        if additional_filters is None:
            additional_filters = {}
        if additional_edge_filters is None:
            additional_edge_filters = {}

        filtered_elements = set(additional_filters.keys()) | set(additional_edge_filters.keys())
        if is_power:
            net = pp.create_empty_network()
            data_func = get_subnet_filter_data_power
            args = [filtered_elements]
        else:
            net = pps.create_empty_network()
            data_func = get_subnet_filter_data_pipe
            args = [net, filtered_elements, meta]
        (
            node_name,
            branch_tables,
            branch_node_cols,
            special_filters,
            get_nodes_func,
            node_elements,
        ) = data_func(*args)

        if db[self._collection_name_of_element(node_name)].find_one() is None:
            net["empty"] = True

        add_args = {
            "net": net,
            "db": db,
            "net_id": net_id,
            "include_results": include_results,
            "geo_mode": geo_mode,
            "variant": variant,
            "dtypes": dtypes,
        }

        # Add buses with filter
        if node_filter is not None:
            self._add_element_from_collection(element_type=node_name, filter=node_filter, **add_args)
        nodes = net[node_name].index.tolist()

        if isinstance(add_edge_branches, bool):
            if add_edge_branches:
                add_edge_branches = branch_tables
            else:
                add_edge_branches = []
        elif not isinstance(add_edge_branches, list):
            raise ValueError("add_edge_branches must be a list or a boolean")

        for tbl, (node_cols, add_edge, filter_func, node_getter) in additional_edge_filters.items():
            branch_tables.append(tbl)
            branch_node_cols.append(tuple(node_cols))
            if add_edge:
                add_edge_branches.append(tbl)
            if filter_func is not None:
                special_filters[tbl] = filter_func
            get_nodes_func[tbl] = node_getter if node_getter is not None else get_nodes_from_element

        branch_nodes = set()
        for branch_name, node_cols in zip(branch_tables, branch_node_cols):
            if branch_name == "line" and line_filter is not None:
                 branch_filter = line_filter
            else:
                operator = "$or" if branch_name in add_edge_branches else "$and"
                # Add branch elements connected to at least one node
                branch_filter = {operator: [{b: {"$in": nodes}} for b in node_cols]}
                if branch_name in special_filters:
                    branch_filter = special_filters[branch_name](branch_filter)
            self._add_element_from_collection(element_type=branch_name, filter=branch_filter, **add_args)
            get_nd_func = get_nodes_func[branch_name]
            branch_nodes.update(set(chain.from_iterable(get_nd_func(net, branch_name, node_cols))))

        branch_nodes_outside = []
        if branch_nodes:
            # Add buses on the other side of the branches
            branch_nodes_outside = list(map(int, branch_nodes - set(nodes)))
            self._add_element_from_collection(element_type=node_name, filter={"index": {"$in": branch_nodes_outside}}, **add_args)
            nodes = net[node_name].index.tolist()

        if is_power:
            self._add_element_from_collection(element_type="switch", filter=filter_switch_for_element(net), **add_args)

        # add all node elements that are connected to buses within the network
        for element in node_elements:
            self._add_element_from_collection(element_type=element, filter={node_name: {"$in": nodes}}, **add_args)

        # Add elements for which the user has provided a filter function
        for element, filter_func in additional_filters.items():
            if element in ignore_elements:
                continue
            self._add_element_from_collection(element_type=element, filter=filter_func(net), **add_args)

        # add all other collections
        collection_names = self.get_net_collections(db)
        for collection in collection_names:
            table_name = self._element_name_of_collection(collection)
            # skip all element tables that we have already added
            if table_name in filtered_elements or table_name in ignore_elements:
                continue
            # for tables that share an index with an element (e.g. load->res_load) load only relevant entries
            for element in filtered_elements:
                if table_name.startswith(element + "_") or table_name.startswith(
                    "net_res_" + element
                ):
                    element_filter = {"index": {"$in": net[element].index.tolist()}}
                    break
            else:
                # all other tables (e.g. std_types) are loaded without filter
                element_filter = None
            self._add_element_from_collection(element_type=table_name, filter=element_filter, **add_args)
        self.deserialize_and_update_data(net, meta)
        if return_edge_branch_nodes:
            return net, branch_nodes_outside
        return net

    def _collection_name_of_element(self, element: str) -> str:
        """Get the project collection name for an element."""
        return f"net_{element}" if element not in ["variant", "_networks"] else element

    def _element_name_of_collection(self, collection: str) -> str:
        """Get the element name for a project collection."""
        return collection[4:] if collection.startswith("net_") else collection

    def write_network_to_db(
        self,
        net: PandaNet,
        name: str,
        sector="power",
        overwrite: bool = True,
        project_id: str = None,
        metadata: dict = None,
        skip_results: bool = False,
        net_id: int | str = None,
    ) -> dict:
        """
        Write a pandapower or pandapipes network to the database.

        A network is uniquely identified only by its net_id (multiple networks with the same name are allowed).
        If no net_id is passed, an incrementing integer id is generated.
        Raises an error if a passed net_id already exists in the database unless overwrite is set to True.


        Parameters
        ----------
        net:
            pandapower or pandapipes network
        name:
            name of the network
        sector:
            sector of the network
        overwrite:
            if True, an existing network with the same net_id will be overwritten
        project_id:
            id of the project (defaults to active project)
        metadata:
            additional metadata to be stored with the network
        skip_results:
            if True, results tables are not stored in the database
        net_id:
            id of the network. If None, a new integer id is generated

        Returns
        -------
        dict
            metadata of the created network
        """
        if project_id:
            self.set_active_project_by_id(project_id)
        self.check_permission("write")
        db = self._get_project_database()
        networks_coll = db["_networks"]

        if net_id is None:
            net_id = self._get_int_index("_networks")
        else:
            try:
                networks_coll.insert_one({"_id": net_id})
            except DuplicateKeyError:
                if overwrite:
                    self.delete_network(net_id)
                    networks_coll.insert_one({"_id": net_id})
                else:
                    msg = f"Network with net_id {net_id} already exists"
                    raise PandaHubError(msg)

        networks_coll.update_one({"_id": net_id}, {"$set": {"name": name, "sector": sector}})

        data = {}
        dtypes = {}
        version_ = version.parse(self.get_project_version())
        for element, element_data in net.items():
            if skip_results and element.startswith("res"):
                continue
            if element.startswith("_"):
                continue
            if isinstance(element_data, pd.core.frame.DataFrame):
                # create type lookup
                dtypes[element] = get_dtypes(element_data, self._datatypes.get(element))
                if element_data.empty:
                    continue
                element_data = element_data.copy(deep=True)
                if element not in self._elements_without_vars:
                    if "var_type" in element_data:
                        element_data["var_type"] = element_data["var_type"].fillna("base")
                    else:
                        element_data["var_type"] = "base"
                        element_data["not_in_var"] = np.empty((len(element_data.index), 0)).tolist()
                        element_data["variant"] = None
                element_data = convert_element_to_dict(element_data, net_id, self._datatypes.get(element))
                self._write_element_to_db(db, element, element_data)

            else:
                element_data = serialize_object_data(element, element_data, version_)
                if element_data:
                    data[element] = element_data

        # write network metadata
        network_data = {
            "dtypes": dtypes,
            "data": data,
        }
        if metadata is not None:
            network_data.update(metadata)
        networks_coll.update_one({"_id": net_id}, {"$set": network_data})
        return network_data | {"_id": net_id}

    def _write_net_collections_to_db(self, db, collections):
        for element, element_data in collections.items():
            self._write_element_to_db(db, element, element_data)

    def _write_element_to_db(self, db, element_type, element_data):
        existing_collections = set(db.list_collection_names())
        collection_name = self._collection_name_of_element(element_type)
        if len(element_data) > 0:
            if collection_name not in existing_collections:
                self._create_mongodb_indexes(collection=collection_name)
            db[collection_name].insert_many(element_data, ordered=False)
            # print(f"\nFAILED TO WRITE TABLE '{element_type}' TO DATABASE! (details above)")

    def delete_network(self, net_id):
        if net_id is None:
            raise PandaHubError(f"No net_id was passed", 404)
        db = self._get_project_database()
        collection_names = self.get_net_collections(db)
        for collection_name in collection_names:
            db[collection_name].delete_many({"net_id": net_id})
        db["_networks"].delete_one({"_id": net_id})

    def delete_network_by_name(self, name):
        self.check_permission("write")
        db = self._get_project_database()
        net_id = self._get_net_id_from_name(name, db)
        if net_id is None:
            raise PandaHubError(f"Network with name {name} does not exist", 404)
        self.delete_network(net_id)

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
        nets = pd.DataFrame(db.find(fi, projection=proj)).to_list()
        return nets

    def _get_net_id_from_name(self, name, db):
        metadata = db["_networks"].find({"name": name}).to_list()
        if len(metadata) > 1:
            msg = (f"Multiple networks with the name {metadata[0]['name']} found in the database. "
                   f"Use the corresponding function which selects the network by net_id instead.")
            raise PandaHubError(msg)
        return None if len(metadata) == 0 else metadata[0]["_id"]

    def _network_with_name_exists(self, name, db):
        return self._get_net_id_from_name(name, db) is not None

    def _get_net_collections(self, db, with_areas=True):
        return self.get_net_collections(db, with_areas)

    def get_net_collections(self, db=None, with_areas=True):
        if db is None:
            db = self.get_project_database()
        if with_areas:
            collection_filter = {"name": {"$regex": "^net_"}}
        else:
            collection_filter = {"name": {"$regex": "^net_.*(?<!area)$"}}
        return db.list_collection_names(filter=collection_filter)

    def _get_network_metadata(self, db, net_id):
        return db["_networks"].find_one({"_id": net_id})

    def _add_element_from_collection(
        self,
        net,
        db,
        element_type,
        net_id,
        filter=None,
        include_results=True,
        only_tables=None,
        geo_mode="string",
        variant=None,
        dtypes=None,
    ):
        if only_tables is not None and element_type not in only_tables:
            return
        if not include_results and element_type.startswith("res_"):
            return
        filter_dict = {"net_id": net_id, **self.get_variant_filter(variant)}
        if filter is not None:
            if "$or" in filter_dict.keys() and "$or" in filter.keys():
                # if 'or' is in both filters create 'and' with
                # both to avoid override during filter merge
                filter_and = {
                    "$and": [
                        {"$or": filter_dict.pop("$or")},
                        {"$or": filter.pop("$or")},
                    ]
                }
                filter_dict = {**filter_dict, **filter, **filter_and}
            else:
                filter_dict = {**filter_dict, **filter}

        data = db[self._collection_name_of_element(element_type)].find(filter_dict).to_list()
        if len(data) == 0:
            return
        if dtypes is None:
            dtypes = db["_networks"].find_one({"_id": net_id}, projection={"dtypes"})[
                "dtypes"
            ]
        df = pd.DataFrame.from_records(data, index="index")
        if element_type in dtypes:
            dtypes_found_columns = {
                column: dtype
                for column, dtype in dtypes[element_type].items()
                if column in df.columns
            }
            df = df.astype(dtypes_found_columns, errors="ignore")
        df.index.name = None
        df.drop(columns=["_id", "net_id"], inplace=True)
        df.sort_index(inplace=True)
        convert_geojsons(df, geo_mode)
        if "object" in df.columns:
            df["object"] = df["object"].apply(json_to_object)
        if "q_max_characteristic" in df.columns:
            df["q_max_characteristic"] = df["q_max_characteristic"].apply(json_to_object)
            df["q_min_characteristic"] = df["q_min_characteristic"].apply(json_to_object)
        if not element_type in net or net[element_type].empty:
            net[element_type] = df
        else:
            new_rows = set(df.index) - set(net[element_type].index)
            if new_rows:
                net[element_type] = pd.concat(
                    [net[element_type], df.loc[list(new_rows)]]
                )

    # -------------------------
    # Net element handling
    # -------------------------

    def get_net_value_from_db(
        self, net_id, element_type, element_index, parameter, variant=None, project_id=None
    ):
        if project_id:
            self.set_active_project_by_id(project_id)
        self.check_permission("write")

        collection = self._collection_name_of_element(element_type)
        dtypes = self._datatypes.get(element_type)

        variant_filter = self.get_variant_filter(variant)
        documents = self.project_db[collection].find({"index": element_index, "net_id": net_id, **variant_filter}).to_list()
        if len(documents) == 1:
            document = documents[0]
        else:
            if len(documents) == 0:
                raise PandaHubError("Element doesn't exist", 404)
            else:
                raise PandaHubError("Multiple elements found", 404)
        if parameter not in document:
            raise PandaHubError("Parameter doesn't exist", 404)
        if dtypes is not None and parameter in dtypes:
            return dtypes[parameter](document[parameter])
        else:
            return document[parameter]

    def delete_element(
        self, net_id, element_type, element_index, variant=None, project_id=None, **kwargs
    ) -> dict:
        """
        Delete an element from the database.

        Parameters
        ----------
        net_id: str or int
            Network to add elements to, either a name or numeric id
        element_type: str
            Name of the element type (e.g. bus, line)
        element_index: int
            Index of the element to delete
        project_id: str or None
            ObjectId (as str) of the project in which the network is stored. Defaults to current active project if None
        variant: int or None
            Variant index if elements should be created in a variant
        Returns
        -------
        dict
            The deleted element as dict with all fields
        """
        return self.delete_elements(
            net_id=net_id,
            element_type=element_type,
            element_indexes=[element_index],
            variant=variant,
            project_id=project_id,
            **kwargs,
        )[0]

    def delete_elements(
        self,
        net_id: Union[int, str],
        element_type: str,
        element_indexes: list[int],
        variant: Union[int, None] = None,
        project_id: Union[str, None] = None,
        **kwargs,
    ) -> list[dict]:
        """
        Delete multiple elements of the same type from the database.

        Parameters
        ----------
        net_id: str or int
            Network to add elements to, either a name or numeric id
        element_type: str
            Name of the element type (e.g. bus, line)
        element_indexes: list of int
            Indexes of the elements to delete
        project_id: str or None
            ObjectId (as str) of the project in which the network is stored. Defaults to current active project if None
        variant: int or None
            Variant index if elements should be created in a variant
        Returns
        -------
        list
            A list of deleted elements as dicts with all fields
        """
        if not isinstance(element_indexes, list):
            raise TypeError("Parameter element_indexes must be a list of ints!")
        self.validate_variant(variant, element_type)
        if project_id:
            self.set_active_project_by_id(project_id)

        self.check_permission("write")
        db = self._get_project_database()
        collection = self._collection_name_of_element(element_type)

        element_filter = {
            "index": {"$in": element_indexes},
            "net_id": int(net_id),
            **self.get_variant_filter(variant),
        }

        deletion_targets = db[collection].find(element_filter).to_list()
        if not deletion_targets:
            return []

        if variant is not None:
            delete_ids_variant, delete_ids = [], []
            for target in deletion_targets:
                delete_ids_variant.append(target["_id"]) if target[
                    "var_type"
                ] == "base" else delete_ids.append(target["_id"])
            db[collection].update_many(
                {"_id": {"$in": delete_ids_variant}},
                {"$addToSet": {"not_in_var": variant}},
            )
        else:
            delete_ids = [target["_id"] for target in deletion_targets]
        db[collection].delete_many({"_id": {"$in": delete_ids}})
        return deletion_targets

    def set_net_value_in_db(
        self,
        net_id,
        element_type,
        element_index,
        parameter,
        value,
        variant=None,
        project_id=None,
        **kwargs,
    ):
        self.validate_variant(variant, element_type)
        if project_id:
            self.set_active_project_by_id(project_id)
        self.check_permission("write")
        db = self._get_project_database()
        dtypes = self._datatypes.get(element_type)
        if value is not None and dtypes is not None and parameter in dtypes:
            value = dtypes[parameter](value)
        collection = self._collection_name_of_element(element_type)
        element_filter = {
            "index": element_index,
            "net_id": int(net_id),
            **self.get_variant_filter(variant),
        }
        document = db[collection].find_one({**element_filter})
        if not document:
            raise UserWarning(
                f"No element '{element_type}' to change with index '{element_index}' in this variant"
            )

        old_value = document.get(parameter, None)
        if old_value == value:
            logger.warning(
                f'Value "{value}" for "{parameter}" identical to database element - no change applied'
            )
            return None
        if "." in parameter:
            key, subkey = parameter.split(".")
            document[key][subkey] = value
        else:
            document[parameter] = value

        if variant is None:
            db[collection].update_one(
                {**element_filter, **self.base_variant_filter},
                {"$set": {parameter: value}},
            )
        else:
            if document["var_type"] == "base":
                base_variant_id = document.pop("_id")
                db[collection].update_one(
                    {"_id": base_variant_id}, {"$addToSet": {"not_in_var": variant}}
                )
                document.update(
                    var_type="change", variant=variant, changed_fields=[parameter]
                )
                insert_result = db[collection].insert_one(document)
                document["_id"] = insert_result.inserted_id
            else:
                update_dict = {"$set": {parameter: value}, "$unset": {"not_in_var": ""}}
                if document["var_type"] == "change":
                    update_dict["$addToSet"] = {"changed_fields": parameter}
                db[collection].update_one({"_id": document["_id"]}, update_dict)
        return {
            "document": document,
            parameter: {"previous": old_value, "current": value},
        }

    def set_object_attribute(
        self,
        net_id,
        element_type,
        element_index,
        parameter,
        value,
        variant=None,
        project_id=None,
    ):
        self.validate_variant(variant, element_type)
        if project_id:
            self.set_active_project_by_id(project_id)
        self.check_permission("write")
        db = self._get_project_database()
        dtypes = self._datatypes.get(element_type)
        if dtypes is not None and parameter in dtypes:
            value = dtypes[parameter](value)
        collection = self._collection_name_of_element(element_type)

        js = db[collection].find({"index": element_index, "net_id": net_id}).to_list(1)
        obj = json_to_object(js["object"])
        setattr(obj, parameter, value)
        db[collection].update_one(
            {"index": element_index, "net_id": net_id},
            {"$set": {"object._object": obj.to_json()}},
        )

        element_filter = {"index": element_index, "net_id": int(net_id)}

        if variant is None:
            document = db[collection].find_one(
                {**element_filter, **self.base_variant_filter}
            )
            obj = json_to_object(document["object"])
            setattr(obj, parameter, value)
            db[collection].update_one(
                {**element_filter, **self.base_variant_filter},
                {"$set": {"object._object": obj.to_json()}},
            )
        else:
            element_filter = {**element_filter, **self.get_variant_filter(variant)}
            document = db[collection].find_one({**element_filter})
            if not document:
                raise UserWarning(
                    f"No element '{element_type}' to change with index '{element_index}' in this variant"
                )
            obj = json_to_object(document["object"])
            setattr(obj, parameter, value)
            if document["var_type"] == "base":
                base_variant_id = document.pop("_id")
                db[collection].update_one(
                    {"_id": base_variant_id}, {"$addToSet": {"not_in_var": variant}}
                )
                document["object"]["_object"] = obj
                document["var_type"] = "change"
                db[collection].insert_one(document)
            else:
                db[collection].update_one(
                    {"_id": document["_id"]}, {"$set": {"object._object": obj}}
                )

    def create_element(
        self,
        net_id: Union[int, str],
        element_type: str,
        element_index: int,
        element_data: dict,
        variant=None,
        project_id=None,
        **kwargs,
    ) -> dict:
        """
        Creates an element in the database.

        Parameters
        ----------
        net_id: str or int
            Network to add elements to, either as name or numeric id
        element_type: str
            Name of the element type (e.g. bus, line)
        element_index: int
            Index of the element to add
        element_data: dict
            Field-value dict to create element from
        project_id: str or None
            ObjectId (as str) of the project in which the network is stored. Defaults to current active project if None
        variant: int or None
            Variant index if elements should be created in a variant
        Returns
        -------
        dict
            The created element (element_data with added _id field)
        """
        return self.create_elements(
            net_id=net_id,
            element_type=element_type,
            elements_data=[{"index": element_index, **element_data}],
            variant=variant,
            project_id=project_id,
            **kwargs,
        )[0]

    def create_elements(
        self,
        net_id: Union[int, str],
        element_type: str,
        elements_data: list[dict],
        variant: int | None = None,
        project_id: str = None,
        **kwargs,
    ) -> list[dict]:
        """
        Creates multiple elements of the same type in the database.

        Parameters
        ----------
        net_id: str or int
            Network to add elements to, either a name or numeric id
        element_type: str
            Name of the element type (e.g. bus, line)
        elements_data: list of dict
            Field-value dicts to create elements from - must include a valid "index" field!
        project_id: str or None
            ObjectId (as str) of the project in which the network is stored. Defaults to current active project if None
        variant: int or None
            Variant index if elements should be created in a variant
        Returns
        -------
        list
            A list of the created elements (elements_data with added _id fields)
        """
        self.validate_variant(variant, element_type)
        if project_id:
            self.set_active_project_by_id(project_id)
        self.check_permission("write")
        db = self._get_project_database()
        if variant is None:
            var_data = {"var_type": "base", "not_in_var": [], "variant": None}
        else:
            var_data = {"var_type": "addition", "not_in_var": [], "variant": variant}
        net_doc = db["_networks"].find_one({"_id": net_id})
        data = []
        for elm_data in elements_data:
            self._add_missing_defaults(element_type, elm_data, net_doc)
            self._ensure_dtypes(element_type, elm_data)
            data.append({**elm_data, **var_data, "net_id": net_id})
        collection = self._collection_name_of_element(element_type)
        db[collection].insert_many(data, ordered=False)
        return data

    def _add_missing_defaults(self, element_type, element_data, net_doc):
        func_str = f"create_{element_type}"
        package = pp if net_doc['sector'] == 'power' else pps
        if not hasattr(package, func_str):
            return
        create_func = getattr(package, func_str)
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
            if net_doc is not None:
                # std_types = json.loads(net_doc["data"]["std_types"], cls=io_pp.PPJSONDecoder)[element_type]
                std_types = net_doc["data"]["std_types"]
                if std_type in std_types:
                    element_data.update(std_types[std_type])

            # add needed parameters not defined in standard type
            if element_type == "line":
                if "g_us_per_km" not in element_data:
                    element_data["g_us_per_km"] = 0
        if element_type in ['sink', 'source']:
            if not 'mdot_kg_per_s' in element_data:
                element_data["mdot_kg_per_s"] = None

    def _ensure_dtypes(self, element_type, data):
        dtypes = self._datatypes.get(element_type)
        if dtypes is None:
            return
        for key, val in data.items():
            if not val is None and key in dtypes and not dtypes[key] == object:
                data[key] = dtypes[key](val)

    def _create_mongodb_indexes(
        self, project_id: Optional[str] = None, collection: Optional["str"] = None
    ):
        """
        Create indexes on mongodb collections. Indexes are defined in pandahub.lib.mongodb_indexes

        Parameters
        ----------
        project_id: str or None
            Project to create indexes in - if None, current active project is used.
        collection: str or None
            Single collection to create index for - if None, alle defined Indexes are created.

        Returns
        -------
        None
        """
        if project_id:
            project_db = self.mongo_client[str(project_id)]
        else:
            project_db = self._get_project_database()
        if collection:
            if collection in self.mongodb_indexes:
                indexes_to_set = {collection: self.mongodb_indexes[collection]}
            else:
                return
        else:
            indexes_to_set = self.mongodb_indexes
        for collection, indexes in indexes_to_set.items():
            logger.info(f"creating indexes in {collection} collection")
            project_db[collection].create_indexes(indexes)

    def _get_int_index(self, collection: str, index_field: str = "_id", query_filter: dict | None = None,
                       retries: int = 10) -> int:
        """Create a document in a collection with an incrementing integer value for the index_field key.

        ! This function expects a unique index set in the database on collection.index_field, compound with the keys in query_filter (if not None) !

        Parameters
        ----------
        collection:
            The collection to create the document in.
        index_field:
            The field holding the incrementing integer index.
        query_filter:
            A query filter to apply when searching for the highest existing index.
        retries:
            Number of retries to create the index in case of a race condition.

        Returns
        -------
            The value of the created index.
        """
        query_filter = query_filter if query_filter is not None else {}
        coll = self.get_project_collection(collection)
        for retry in range(retries):
            try:
                max_index_doc = next(coll.find(query_filter, projection={index_field: 1})
                                 .sort(index_field, -1)
                                 .limit(1),
                                 None)
                index = 0 if max_index_doc is None else int(max_index_doc[index_field]) + 1
                coll.insert_one({index_field: index})
                break
            except DuplicateKeyError:
                continue
        else:
            msg = f"Failed to create integer index for field {index_field} in {collection}!"
            raise PandaHubError(msg)
        return index


    # -------------------------
    # Variants
    # -------------------------

    def create_variant(
        self,
        net_id: int,
        name: str | None = None,
        default_name: str = "Variant",
        index: int | None = None,
    ) -> dict:
        db = self._get_project_database()
        if index is None:
            index = self._get_int_index("variant", index_field="index", query_filter={"net_id": net_id})

        if name is None:
            name = f"{default_name}  {index + 1}"
        now = int(time.time())
        variant = {
            "net_id": net_id,
            "index": index,
            "name": name,
            "date_created": now,
            "date_changed": now,
        }
        db["variant"].insert_one(variant)
        del variant["_id"]
        return variant

    def delete_variant(self, net_id, index):
        db = self._get_project_database()
        collection_names = self.get_net_collections(db)
        for coll in collection_names:
            # remove references to deleted objects
            db[coll].update_many(
                {"net_id": net_id, "var_type": "base", "not_in_var": index},
                {"$pull": {"not_in_var": index}},
            )
            # remove changes and additions
            db[coll].delete_many(
                {
                    "net_id": net_id,
                    "var_type": {"$in": ["change", "addition"]},
                    "variant": index,
                }
            )
        # delete variant
        db["variant"].delete_one({"net_id": net_id, "index": index})

    def update_variant(self, net_id, index, data):
        db = self._get_project_database()
        db["variant"].update_one({"net_id": net_id, "index": index}, {"$set": data})

    def get_variant_filter(self, variant: int | None) -> dict:
        """
        Creates a mongodb query filter to retrieve pandapower elements for the given variant(s).

        Parameters
        ----------
        variant : int or None
                None represent the base variant, ints specify variant indices.

        Returns
        -------
        dict
            mongodb query filter for the given variant
        """
        self.validate_variant(variant)
        if variant is None:
            return self.base_variant_filter
        return {"$or": [{"var_type": "base", "not_in_var": {"$ne": variant}},
                        {"var_type": {"$in": ["change", "addition"]}, "variant": variant}, ]}

    def validate_variant(self, variant: int | None, element_type: str | None = None):
        """Raise a ValueError if variant is not int | None or element_type does not support variants."""
        validate_variant_type(variant)
        if element_type is not None and variant is not None and element_type in self._elements_without_vars:
            raise ValueError(f"{element_type} does not support variants")

    # -------------------------
    # Bulk operations
    # -------------------------

    def bulk_write_to_db(
        self, data, collection_name="tasks", global_database=False, project_id=None
    ):
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
        if self.collection_is_timeseries(
            collection_name=collection_name,
            project_id=project_id,
            global_database=global_database,
        ):
            raise NotImplementedError(
                "Bulk write is not fully supported for timeseries collections in MongoDB"
            )

        operations = [
            ReplaceOne(
                replacement=d,
                filter={"_id": d["_id"]},
                upsert=True,
            )
            for d in data
        ]
        db[collection_name].bulk_write(operations)

    def bulk_update_in_db(
        self,
        data,
        document_ids,
        collection_name="tasks",
        global_database=False,
        project_id=None,
    ):
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
        if project_id:
            self.set_active_project_by_id(project_id)
        if global_database:
            db = self._get_global_database()
        else:
            self.check_permission("write")
            db = self._get_project_database()
        operations = {}
        operations["UpdateOne"] = []
        i = 0
        for d in data:
            operations["UpdateOne"].append(
                {
                    "filter": {"_id": document_ids[i]},
                    "update": {"$push": d},
                    "upsert": False,
                }
            )
            i += 1

        db[collection_name].bulk_write(operations)

    # -------------------------
    # Timeseries
    # -------------------------

    def write_timeseries_to_db(
        self,
        timeseries,
        data_type,
        ts_format="timestamp_value",
        compress_ts_data=False,
        global_database=False,
        collection_name="timeseries",
        project_id=None,
        **kwargs,
    ):
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
        _id: timeseries _id

        """
        if project_id:
            self.set_active_project_by_id(project_id)
        if global_database:
            db = self._get_global_database()
        else:
            self.check_permission("write")
            db = self._get_project_database()
        if self.collection_is_timeseries(collection_name, project_id, global_database):
            # get metadata dictionary based on the input arguments
            metadata = get_metadata_for_timeseries_collections(db, data_type=data_type, **kwargs)
            _id =  metadata["_id"]
            start = timeseries.index.min()
            end = timeseries.index.max()
            # delete overlapping timeseries already in the database
            filter = {
                "metadata._id": _id,
                "timestamp": {
                    "$gte": start,
                    "$lte":  end
                }
            }
            db.timeseries.delete_many(filter)

            # create new timeseries documents
            if isinstance(timeseries, pd.Series):
                documents = [
                    {"metadata": metadata, "timestamp": idx, "value": value}
                    for idx, value in timeseries.items()
                ]
            elif isinstance(timeseries, pd.DataFrame):
                documents = [
                    {"metadata": metadata, "timestamp": idx, **row.to_dict()}
                    for idx, row in timeseries.iterrows()
                ]

            timeseries_db_is_empty = db[collection_name].find_one() is None
            metadata_collection_name = collection_name + "_metadata"
            timeseries_metadata_exists = metadata_collection_name in db.list_collection_names()

            db[collection_name].insert_many(documents)
            if timeseries_db_is_empty or timeseries_metadata_exists:
                meta_col = db[metadata_collection_name]
                max_value = timeseries.values.max()
                min_value = timeseries.values.min()
                id_match = {"_id": _id}
                if meta_col.find_one(id_match) is None:
                    meta_col.insert_one(
                        {
                            "first_timestamp": start,
                            "min_value": min_value,
                            "last_timestamp": end,
                            "max_value": max_value,
                            **metadata,
                        }
                    )
                else:
                    meta_col.update_one(
                        id_match,
                        {
                            "$min": {"first_timestamp": start, "min_value": min_value},   # only updates if start < current
                            "$max": {"last_timestamp": end, "max_value": max_value},       # only updates if end > current
                        }
                    )
            return _id
        else:
            document = create_timeseries_document(
                timeseries=timeseries,
                data_type=data_type,
                ts_format=ts_format,
                compress_ts_data=compress_ts_data,
                **kwargs,
            )
            db[collection_name].replace_one({"_id": document["_id"]}, document, upsert=True)
            logger.debug("document with _id {document['_id']} added to database")
            return document["_id"]

    def bulk_write_timeseries_to_db(
        self,
        timeseries,
        data_type,
        meta_frame=None,
        ts_format="timestamp_value",
        compress_ts_data=False,
        global_database=False,
        collection_name="timeseries",
        project_id=None,
        **kwargs,
    ):
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
        if project_id:
            self.set_active_project_by_id(project_id)
        if self.collection_is_timeseries(collection_name, project_id, global_database):
            raise NotImplementedError("Not implemented yet for timeseries collections")
        for col in timeseries.columns:
            if meta_frame is not None:
                args = {**kwargs, **meta_frame.loc[col]}
            else:
                args = kwargs
            doc = create_timeseries_document(
                timeseries[col],
                data_type,
                ts_format=ts_format,
                compress_ts_data=compress_ts_data,
                element_index=col,
                **args,
            )
            documents.append(doc)
        self.bulk_write_to_db(
            documents, collection_name=collection_name, global_database=global_database
        )
        logger.debug(f"{len(documents)} documents added to database")
        return [d["_id"] for d in documents]

    def update_timeseries_in_db(
        self,
        new_ts_content,
        document_id,
        collection_name="timeseries",
        global_database=False,
        project_id=None,
    ):
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
        if project_id:
            self.set_active_project_by_id(project_id)
        if global_database:
            db = self._get_global_database()
        else:
            self.check_permission("write")
            db = self._get_project_database()
        ts_update = {
            "timeseries_data": {
                "$each": convert_timeseries_to_subdocuments(new_ts_content)
            }
        }
        db[collection_name].update_one(
            {"_id": document_id},
            {"$push": ts_update},
        )
        # logger.info("document updated in database")

    def bulk_update_timeseries_in_db(
        self,
        new_ts_content,
        document_ids,
        project_id=None,
        collection_name="timeseries",
        global_database=False,
    ):
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
        if project_id:
            self.set_active_project_by_id(project_id)
        if self.collection_is_timeseries(collection_name, project_id, global_database):
            raise NotImplementedError("Not implemented yet for timeseries collections")
        documents = []
        for i in range(len(new_ts_content.columns)):
            col = new_ts_content.columns[i]
            document = {}
            document["timeseries_data"] = {
                "$each": convert_timeseries_to_subdocuments(new_ts_content[col])
            }
            documents.append(document)
        self.bulk_update_in_db(
            documents,
            document_ids,
            project_id=project_id,
            collection_name="timeseries",
            global_database=global_database,
        )

        # logger.debug(f"{len(documents)} documents added to database")

    def get_timeseries_from_db(
        self,
        filter_document={},
        timestamp_range=None,
        ts_format="timestamp_value",
        compressed_ts_data=False,
        global_database=False,
        collection_name="timeseries",
        include_metadata=False,
        project_id=None,
        **kwargs,
    ):
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
        if self.collection_is_timeseries(collection_name, project_id, global_database):
            metadata = get_metadata_for_timeseries_collections(db, **kwargs)
            pipeline = []
            pipeline.append({"$match": {"metadata._id": metadata["_id"]}})
            if timestamp_range is not None:
                pipeline.append({"$match": {"timestamp": {"$gte": timestamp_range[0], "$lt": timestamp_range[1]}}})
            pipeline.append({"$project": {"_id": 0, "metadata": 0}})
            timeseries = db[collection_name].aggregate_pandas_all(pipeline)
            if len(timeseries) == 0:
                raise PandaHubError("no documents matching the provided filter found", 404)
            timeseries.set_index("timestamp", inplace=True)
            if include_metadata:
                raise NotImplementedError(
                    "Not implemented yet for timeseries collections"
                )
            if len(timeseries.columns) == 1:
                return timeseries[timeseries.columns[0]]
            else:
                return timeseries
        filter_document = {**filter_document, **kwargs}
        pipeline = [{"$match": filter_document}]
        if not compressed_ts_data:
            if ts_format == "timestamp_value":
                if timestamp_range:
                    pipeline.append(
                        {
                            "$project": {
                                "timeseries_data": {
                                    "$filter": {
                                        "input": "$timeseries_data",
                                        "as": "timeseries_data",
                                        "cond": {
                                            "$and": [
                                                {
                                                    "$gte": [
                                                        "$$timeseries_data.timestamp",
                                                        timestamp_range[0],
                                                    ]
                                                },
                                                {
                                                    "$lt": [
                                                        "$$timeseries_data.timestamp",
                                                        timestamp_range[1],
                                                    ]
                                                },
                                            ]
                                        },
                                    }
                                }
                            }
                        }
                    )
                pipeline.append(
                    {
                        "$addFields": {
                            "timestamps": "$timeseries_data.timestamp",
                            "values": "$timeseries_data.value",
                        }
                    }
                )
                if include_metadata:
                    pipeline.append({"$project": {"timeseries_data": 0}})
                else:
                    pipeline.append(
                        {"$project": {"timestamps": 1, "values": 1, "_id": 0}}
                    )
            elif ts_format == "array":
                if not include_metadata:
                    pipeline.append({"$project": {"timeseries_data": 1}})
        else:
            if not include_metadata:
                pipeline.append({"$project": {"timeseries_data": 1,
                                              "num_timestamps": 1}})
        data = db[collection_name].aggregate(pipeline).to_list()
        if len(data) == 0:
            raise PandaHubError("no documents matching the provided filter found", 404)
        elif len(data) > 1:
            raise PandaHubError("multiple documents matching the provided filter found")
        else:
            data = data[0]
        if compressed_ts_data:
            timeseries_data = decompress_timeseries_data(data["timeseries_data"],
                                                         ts_format,
                                                         num_timestamps=data["num_timestamps"])
        else:
            if ts_format == "timestamp_value":
                timeseries_data = pd.Series(
                    data["values"], index=data["timestamps"], dtype="float64"
                )
            elif ts_format == "array":
                timeseries_data = data["timeseries_data"]
        if include_metadata:
            data["timeseries_data"] = timeseries_data
            del data["timestamps"]
            del data["values"]
            return data
        else:
            return timeseries_data

    def get_timeseries_metadata(
        self,
        filter_document,
        collection_name="timeseries",
        global_database=False,
        project_id=None,
        timestamp_range=None,
    ):
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
        if project_id:
            self.set_active_project_by_id(project_id)
        if global_database:
            db = self._get_global_database()
        else:
            self.check_permission("read")
            db = self._get_project_database()
        if self.collection_is_timeseries(collection_name, project_id, global_database):
            metadata_collection_name = collection_name + "_metadata"
            # if there is no metadata collection yet, compile the metadata for all timeseries
            if metadata_collection_name not in db.list_collection_names():
                metadata = self.get_timeseries_metadata_from_timeseries_collection(filter_document={}, collection_name=collection_name)
                db[metadata_collection_name].insert_many(metadata)
            metadata = self.get_timeseries_metadata_from_metadata_collection(filter_document, metadata_collection_name, timestamp_range)
        else:
            match_filter = []
            pipeline = []
            for key in filter_document:
                if key == "timestamp_range":
                    continue
                filter_value = filter_document[key]
                if isinstance(filter_value, list) and key not in ["$or", "$and"]:
                    match_filter.append({key: {"$in": filter_value}})
                else:
                    match_filter.append({key: filter_value})
            if match_filter:
                pipeline.append({"$match": {"$and": match_filter}})
            projection = {"$project": {"timeseries_data": 0}}
            pipeline.append(projection)
            metadata = db[collection_name].aggregate(pipeline).to_list()
        df_metadata = pd.DataFrame(metadata)
        if len(df_metadata):
            df_metadata.set_index("_id", inplace=True)
        if "return_id" in df_metadata:
            del df_metadata["return_id"]
        return df_metadata


    def get_timeseries_metadata_from_metadata_collection(self, filter_document, metadata_collection_name, timestamp_range=None):
        db = self._get_project_database()
        if timestamp_range is not None:
            filter_document["first_timestamp"] = {"$lte": timestamp_range[1]}
            filter_document["last_timestamp"] = {"$gte": timestamp_range[0]}
        return db[metadata_collection_name].find(filter_document).to_list()


    def get_timeseries_metadata_from_timeseries_collection(self, filter_document, collection_name, timestamp_range=None):
        db = self._get_project_database()
        pipeline = []
        if len(filter_document) > 0:
            document_filter = {
                "metadata." + key: value for key, value in filter_document.items()
            }
            pipeline.append({"$match": document_filter})
        else:
            document_filter = {}
        if timestamp_range is not None:
            document_filter["timestamp"] = {
                "$gte": timestamp_range[0],
                "$lt": timestamp_range[1],
            }
        document = db[collection_name].find_one(
            document_filter, projection={"timestamp": 0, "_id": 0}
        )
        if document is None:
            return pd.DataFrame()
        value_fields = ["$%s" % field for field in document.keys() if field != "metadata"]
        group_dict = {
            "_id": "$metadata._id",
            "max_value": {"$max": {"$max": value_fields}},
            "min_value": {"$min": {"$min": value_fields}},
            "first_timestamp": {"$min": "$timestamp"},
            "last_timestamp": {"$max": "$timestamp"},
        }
        metadata_fields = {
            metadata_field: {"$first": "$metadata.%s" % metadata_field}
            for metadata_field in document["metadata"].keys()
            if metadata_field != "_id"
        }
        group_dict.update(metadata_fields)
        pipeline.append({"$group": group_dict})
        return db[collection_name].aggregate(pipeline).to_list()


    def add_metadata(
        self,
        filter_document,
        add_meta,
        global_database=False,
        collection_name="timeseries",
    ):
        if global_database:
            db = self._get_global_database()
        else:
            self.check_permission("write")
            db = self._get_project_database()

        # get metada before change
        meta_before = self.get_timeseries_metadata(
            filter_document,
            global_database=global_database,
            collection_name=collection_name,
        )
        # add the new information to the metadata dict of the existing timeseries
        if (
            len(meta_before) > 1
        ):  # TODO is this the desired behaviour? Needs to specified
            raise PandaHubError
        meta_copy = {**meta_before.iloc[0].to_dict(), **add_meta}
        # write new metadata to mongo db
        db[collection_name].replace_one(
            {"_id": meta_before.index[0]}, meta_copy, upsert=True
        )
        return meta_copy

    def multi_get_timeseries_from_db(
        self,
        filter_document={},
        timestamp_range=None,
        exclude_timestamp_range=None,
        include_metadata=False,
        ts_format="timestamp_value",
        compressed_ts_data=False,
        global_database=False,
        collection_name="timeseries",
        project_id=None,
        **kwargs,
    ):
        if project_id:
            self.set_active_project_by_id(project_id)
        if global_database:
            db = self._get_global_database()
        else:
            self.check_permission("read")
            db = self._get_project_database()
        if self.collection_is_timeseries(collection_name, project_id, global_database):
            pipeline = []
            if timestamp_range is not None:
                pipeline.append(
                    {
                        "$match": {
                            "timestamp": {
                                "$gte": timestamp_range[0],
                                "$lt": timestamp_range[1],
                            }
                        }
                    }
                )
            if exclude_timestamp_range is not None:
                pipeline.append(
                    {
                        "$match": {
                            "timestamp": {
                                "$gte": exclude_timestamp_range[0],
                                "$lt": exclude_timestamp_range[1],
                            }
                        }
                    }
                )
            if filter_document is not None:
                document_filter = {
                    "metadata." + key: value for key, value in filter_document.items()
                }
                pipeline.append({"$match": document_filter})

            pipeline.append({"$addFields": {"_id": "$metadata._id"}})
            pipeline.append({"$project": {"metadata": 0}})
            timeseries = []
            ts_all = db[collection_name].aggregate_pandas_all(pipeline)
            if len(ts_all) == 0:
                return timeseries
            for _id, ts in ts_all.groupby("_id"):
                ts.set_index("timestamp", inplace=True)
                value_columns = list(set(ts.columns) - {"timestamp", "_id"})
                value_columns.sort()
                for col in value_columns:
                    timeseries_dict = {"timeseries_data": ts[col]}
                    if include_metadata:
                        metadata = db[f"{collection_name}_metadata"].find_one({"_id": _id})
                        if metadata is not None:
                            timeseries_dict.update(metadata)
                        if len(value_columns) > 1:
                            timeseries_dict["name"] = "%s, %s" % (
                                timeseries_dict["name"],
                                col,
                            )
                    timeseries.append(timeseries_dict)
            return timeseries

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
            projection = {
                "timeseries_data": {
                    "$filter": {
                        "input": "$timeseries_data",
                        "as": "timeseries_data",
                        "cond": {
                            "$and": [
                                {
                                    "$gte": [
                                        "$$timeseries_data.timestamp",
                                        timestamp_range[0],
                                    ]
                                },
                                {
                                    "$lt": [
                                        "$$timeseries_data.timestamp",
                                        timestamp_range[1],
                                    ]
                                },
                            ]
                        },
                    }
                }
            }
            pipeline.append({"$project": projection})
        if exclude_timestamp_range:
            projection = {
                "timeseries_data": {
                    "$filter": {
                        "input": "$timeseries_data",
                        "as": "timeseries_data",
                        "cond": {
                            "$or": [
                                {
                                    "$lt": [
                                        "$$timeseries_data.timestamp",
                                        timestamp_range[0],
                                    ]
                                },
                                {
                                    "$gte": [
                                        "$$timeseries_data.timestamp",
                                        timestamp_range[1],
                                    ]
                                },
                            ]
                        },
                    }
                }
            }
            pipeline.append({"$project": projection})
        if not include_metadata:
            if compressed_ts_data:
                pipeline.append({"$project": {"timeseries_data": 1, "num_timestamps": 1}})
            else:
                pipeline.append({"$project": {"timeseries_data": 1}})

        timeseries = []
        for ts in db[collection_name].aggregate(pipeline):
            if len(ts["timeseries_data"]) == 0:
                continue
            data = ts["timeseries_data"]
            if compressed_ts_data:
                timeseries_data = decompress_timeseries_data(data, ts_format, ts["num_timestamps"])
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
                    metadata = db[collection_name].find_one(
                        {"_id": ts["_id"]}, projection={"timeseries_data": 0}
                    )
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

    def bulk_get_timeseries_from_db(
        self,
        filter_document={},
        timestamp_range=None,
        exclude_timestamp_range=None,
        additional_columns=None,
        pivot_by_column=None,
        global_database=False,
        collection_name="timeseries",
        project_id=None,
        **kwargs,
    ):
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

        if self.collection_is_timeseries(collection_name, project_id, global_database):
            document_filter = {
                "metadata." + key: value for key, value in filter_document.items()
            }
            if timestamp_range is not None and exclude_timestamp_range is not None:
                raise NotImplementedError(
                    "timestamp_range and exclude_timestamp_range cannot be used at the same time with timeseries collections"
                )
            if timestamp_range is not None:
                document_filter["timestamp"] = {
                    "$gte": timestamp_range[0],
                    "$lte": timestamp_range[1],
                }
            if exclude_timestamp_range is not None:
                document_filter["timestamp"] = {
                    "$lte": exclude_timestamp_range[0],
                    "$gte": exclude_timestamp_range[1],
                }
            timeseries = {
                d["timestamp"]: d["value"]
                for d in db[collection_name].find(document_filter)
            }
            return pd.Series(timeseries)

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
            projection = {
                "timeseries_data": {
                    "$filter": {
                        "input": "$timeseries_data",
                        "as": "timeseries_data",
                        "cond": {
                            "$and": [
                                {
                                    "$gte": [
                                        "$$timeseries_data.timestamp",
                                        timestamp_range[0],
                                    ]
                                },
                                {
                                    "$lt": [
                                        "$$timeseries_data.timestamp",
                                        timestamp_range[1],
                                    ]
                                },
                            ]
                        },
                    }
                }
            }
            projection = {**projection, **custom_projection}
            pipeline.append({"$project": projection})
        if exclude_timestamp_range:
            projection = {
                "timeseries_data": {
                    "$filter": {
                        "input": "$timeseries_data",
                        "as": "timeseries_data",
                        "cond": {
                            "$or": [
                                {
                                    "$lt": [
                                        "$$timeseries_data.timestamp",
                                        timestamp_range[0],
                                    ]
                                },
                                {
                                    "$gte": [
                                        "$$timeseries_data.timestamp",
                                        timestamp_range[1],
                                    ]
                                },
                            ]
                        },
                    }
                }
            }
            projection = {**projection, **custom_projection}
            pipeline.append({"$project": projection})
        pipeline.append({"$unwind": "$timeseries_data"})
        projection = {
            "value": "$timeseries_data.value",
            "timestamp": "$timeseries_data.timestamp",
        }
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


    def delete_timeseries_from_db(
        self,
        element_type,
        data_type,
        netname=None,
        element_index=None,
        collection_name="timeseries",
        global_database=False,
        **kwargs,
    ):
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
        if global_database:
            db = self._get_global_database()
        else:
            self.check_permission("write")
            db = self._get_project_database()
        if self.collection_is_timeseries(collection_name):
            metadata = get_metadata_for_timeseries_collections(db, data_type, **kwargs)
            return self.delete_timeseries_from_db_by_id(metadata["_id"], collection_name)
        else:
            filter_document = {"element_type": element_type, "data_type": data_type}
            if netname is not None:
                filter_document["netname"] = netname
            if element_index is not None:
                filter_document["element_index"] = element_index
            filter_document = {**filter_document, **kwargs}
            del_res = db[collection_name].delete_one(filter_document)
            return del_res

    def delete_timeseries_from_db_by_id(
        self,
        _id,
        collection_name="timeseries",
        global_database=False,
    ):
        """
        This function can be used to delete a single timeseries that matches
        the provided metadata from a MongoDB database. The element_type and data_type
        of the timeseries are required, netname and element_index are optional.
        Additionally, arbitrary kwargs can be used.

        Parameters
        ----------
        collection_name : str
            Name of the collection that shall be queried.
        global_database: bool

        Returns
        -------
        Success : Delete Result

        """
        if global_database:
            db = self._get_global_database()
        else:
            self.check_permission("write")
            db = self._get_project_database()
        if self.collection_is_timeseries(collection_name):
            db[f"{collection_name}_metadata"].delete_many({"_id": _id})
            return db[collection_name].delete_many({"metadata._id": _id})
        else:
            return db[collection_name].delete_one({"_id": _id})


    def bulk_del_timeseries_from_db(
        self, filter_document, collection_name="timeseries"
    ):
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
        if self.collection_is_timeseries(collection_name):
            meta_filter = {
                "metadata." + key: value for key, value in filter_document.items()
            }
            db[f"{collection_name}_metadata"].delete_many(filter_document)
            return db[collection_name].delete_many(meta_filter)
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

    def create_timeseries_collection(self, collection_name, overwrite=False, project_id=None):
        if project_id is None:
            db = self._get_project_database()
        else:
            db = self.mongo_client[str(project_id)]
        collection_exists = collection_name in db.list_collection_names()
        if collection_exists:
            if overwrite:
                db.drop_collection(collection_name)
            else:
                logger.info("Collection already exists, skipping")
                return
        db.create_collection(
            collection_name,
            timeseries={
                "timeField": "timestamp",
                "metaField": "metadata",
                "granularity": "minutes",
            },
        )
        db[collection_name].create_index({"metadata._id": 1})

    def collection_is_timeseries(
        self,
        collection_name,
        project_id=None,
        global_database=False,
    ):
        db = self._get_project_or_global_db(project_id, global_database)
        collections = db.list_collections(filter={"name": collection_name}).to_list()
        return len(collections) == 1 and collections[0]["type"] == "timeseries"

    def _get_project_or_global_db(self, project_id=None, global_database=False):
        if project_id:
            self.set_active_project_by_id(project_id)
        if global_database:
            return self._get_global_database()
        else:
            return self._get_project_database()

    #### deprecated functions

    def create_element_in_db(
        self,
        net: Union[int, str],
        element: str,
        element_index: int,
        data: dict,
        variant=None,
        project_id=None,
    ):
        warnings.warn(
            "ph.create_element_in_db was renamed - use ph.create_element instead!"
        )
        return self.create_element(
            net_id=net,
            element_type=element,
            element_index=element_index,
            element_data=data,
            variant=variant,
            project_id=project_id,
        )

    def create_elements_in_db(
        self,
        net: Union[int, str],
        element_type: str,
        elements_data: list[dict],
        project_id: str = None,
        variant: int | None = None,
    ):
        warnings.warn(
            "ph.create_elements_in_db was renamed - use ph.create_elements instead! "
            "Watch out for changed order of project_id and variant args"
        )
        return self.create_elements(
            net_id=net,
            element_type=element_type,
            elements_data=elements_data,
            variant=variant,
            project_id=project_id,
        )

    def delete_net_element(
        self, net, element, element_index, variant=None, project_id=None
    ):
        warnings.warn(
            "ph.delete_net_element was renamed - use ph.delete_element instead!"
        )
        return self.delete_element(
            net_id=net,
            element_type=element,
            element_index=element_index,
            variant=variant,
            project_id=project_id,
        )

    def get_net_from_db(self, *args, **kwargs):
        msg = ("Getting a network by name can be ambiguous and will throw an error if more than one Network with the "
               "given name exists. Preferably, switch to get_network() and pass the network id. "
               "If you want to continue to retrieve a network by its name, switch to get_network_by_name(). "
               "get_net_from_db() will be removed in future versions.")
        warnings.warn(msg, DeprecationWarning)
        return self.get_network_by_name(*args, **kwargs)

    def get_net_from_db_by_id(self, *args, **kwargs):
        msg = "get_net_from_db_by_id() has been renamed - use get_network() as drop-in replacement. This function will be removed in future versions."
        warnings.warn(msg, DeprecationWarning)
        self.get_network(*args, **kwargs)

    def get_subnet_from_db(self, *args, **kwargs):
        msg = ("Getting a network by name can be ambiguous and will throw an error if more than one Network with the "
               "given name exists. Preferably, switch to get_subnet() and pass the network id. "
               "If you want to continue to retrieve a subnet by its name, switch to get_subnet_by_name(). "
               "get_subnet_from_db() will be removed in future versions.")
        warnings.warn(msg, DeprecationWarning)
        return self.get_subnet_by_name(*args, **kwargs)

    def get_subnet_from_db_by_id(self,*args, **kwargs):
        msg = "get_subnet_from_db_by_id() has been renamed - use get_subnet() as drop-in replacement. This function will be removed in future versions."
        warnings.warn(msg, DeprecationWarning)
        return self.get_subnet(*args, **kwargs)

    def delete_net_from_db(self, name):
        msg = ("Deleting a network by name can be ambiguous and will throw an error if more than one Network with the "
               "given name exists. Preferably, switch to delete_network() and pass the network id. "
               "If you want to continue to delete a net by its name, switch to delete_network_by_name(). "
               "delete_net_from_db() will be removed in future versions.")
        warnings.warn(msg, DeprecationWarning)
        return self.delete_network_by_name(name)

    def _get_net_collections(self, db, with_areas=True):
        msg = "_get_net_collections() has been made public - use get_net_collections() as drop-in replacement. This function will be removed in future versions."
        warnings.warn(msg, DeprecationWarning)
        return self.get_net_collections(db, with_areas)


def switch_filter(bus_filter):
    return {
        "$and": [
            {"et": "b"},
            bus_filter,
        ]
    }


def filter_switch_for_element(net):
    return {
        "$or": [
            {"$and": [{"et": "t"}, {"element": {"$in": net.trafo.index.tolist()}}]},
            {"$and": [{"et": "l"}, {"element": {"$in": net.line.index.tolist()}}]},
            {
                "$and": [
                    {"et": "t3"},
                    {"element": {"$in": net.trafo3w.index.tolist()}},
                ]
            },
        ]
    }


def get_nodes_from_element(ntw, element, nd_cols):
    if element not in ntw:
        return [[] for _ in nd_cols]
    return [ntw[element][ndc].tolist() for ndc in nd_cols]


def get_nodes_from_switch(ntw, _, _nd_cols):
    return [ntw.switch.bus.to_numpy(), ntw.switch.loc[ntw.switch.et == "b", "element"].to_numpy()]


def get_subnet_filter_data_power(filtered_elements):
    node_name = "bus"
    branch_tables = ["line", "trafo", "trafo3w", "switch"]
    branch_node_cols = [
        ("from_bus", "to_bus"),
        ("hv_bus", "lv_bus"),
        ("hv_bus", "lv_bus", "mv_bus"),
        ("bus", "element"),
    ]
    special_filters = {"switch": switch_filter}
    get_nodes_func = {br: get_nodes_from_element for br in branch_tables}
    get_nodes_func["switch"] = get_nodes_from_switch
    # add node elements
    node_elements = [
        "load",
        "asymmetric_load",
        "sgen",
        "asymmetric_sgen",
        "gen",
        "ext_grid",
        "shunt",
        "xward",
        "ward",
        "motor",
        "storage",
    ]
    branch_elements = ["trafo", "line", "trafo3w", "switch", "impedance"]
    filtered_elements |= set(node_elements) | set(branch_elements) | {"bus"}
    return (
        node_name,
        branch_tables,
        branch_node_cols,
        special_filters,
        get_nodes_func,
        node_elements,
    )


def get_subnet_filter_data_pipe(net, filtered_elements, metadata):
    node_name = "junction"
    component_list = []
    if "data" in metadata and "component_list" in metadata["data"]:
        component_list = json.loads(
            metadata["data"]["component_list"][11:],
            cls=io_pp.PPJSONDecoder,
            registry_class=FromSerializableRegistryPpipe,
        )
    branch_tables = []
    branch_node_cols = []
    node_elements = []
    for c in component_list:
        pps.add_new_component(net, c)
        filtered_elements.add(c.table_name())
        if issubclass(c, BranchComponent):
            branch_tables.append(c.table_name())
            branch_node_cols.append(c.from_to_node_cols())
        if issubclass(c, NodeElementComponent):
            node_elements.append(c.table_name())
    special_filters = dict()
    get_nodes_func = {br: get_nodes_from_element for br in branch_tables}
    return (
        node_name,
        branch_tables,
        branch_node_cols,
        special_filters,
        get_nodes_func,
        node_elements,
    )



if __name__ == "__main__":
    self = PandaHub()
    test_project_name = "test_project"
    self.set_active_project(test_project_name)
    ts = self.multi_get_timeseries_from_db(global_database=True)
    # r = self.create_account(email, password)
    # self.login(email, password)
    # project_exists = self.project_exists(project_name)
    # if project_exists:
    # r = self.delete_project(project_name, i_know_this_action_is_final=True)
    # self.create_project(project_name)

    # r = self.delete_project(project_name, i_know_this_action_is_final=True)

    # print(r.json())
