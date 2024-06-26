import requests
import pandapower as pp
import pandas as pd
from pathlib import Path
import os
import json
from fastapi.encoders import jsonable_encoder


class PandaHubClient:
    def __init__(self, config=None):
        d = None
        if config is None:
            config = os.path.join(Path.home(), "pandahub.config")
        elif type(config) == dict:
            d = config

        try:
            if d is None:
                with open(config, "r") as f:
                    d = json.load(f)
        except FileNotFoundError:
            raise UserWarning("No pandahub configuration file found - log in first")

        self.url = d["url"]
        self.token = d["token"]
        self.cert = None
        if d.get("client_cert_path") and d.get("client_key_path"):
            self.cert = (d["client_cert_path"], d["client_key_path"])
        self.project_id = None

    def set_active_project(self, project_name):
        r = self._post("/projects/set_active_project", json=locals())
        if r.ok:
            self.project_id = r.json()
        else:
            self.project_id = None
        return r

    ### PROJECT HANDLING

    def create_project(self, name):
        return self._post("/projects/create_project", json=locals())

    def delete_project(self, i_know_this_action_is_final=False,
                             error_on_missing_project=True):
        return self._post("/projects/delete_project", json=locals())

    def project_exists(self, project_name):
        return self._post("/projects/project_exists", json=locals()).json()

    def get_projects(self):
        return self._post("/projects/get_projects").json()

    def get_project_settings(self):
        return self._post("/projects/get_project_settings").json()

    def set_project_settings(self, settings):
        return self._post("/projects/set_project_settings", json=locals())

    def get_project_metadata(self):
        return self._post("/projects/get_project_metadata").json()

    def set_project_metadata(self, metadata):
        return self._post("/projects/set_project_metadata", json=locals())

    ### NET HANDLING

    def write_network_to_db(self, net, name, overwrite=True):
        json = locals()
        json["net"] = pp.to_json(net)
        return self._post("/net/write_network_to_db", json=json)

    def get_net_from_db(self, name, include_results=True, only_tables=None):
        r = self._post("/net/get_net_from_db", json=locals())
        return pp.from_json_string(r.json())


    ### ELEMENT HANDLING

    def get_net_value_from_db(self, net_name, element, element_index, parameter):
        return self._post("/net/get_net_value_from_db", json=locals()).json()

    def set_net_value_in_db(self, net_name, element_type, element_index, parameter, value):
        return self._post("/net/set_net_value_in_db", json=locals())

    def create_element(self, net, element_type, element_index, element_data):
        return self._post("/net/create_element", json=locals())

    def create_elements(self, net, element_type, elements_data):
        return self._post("/net/create_elements", json=locals())

    def delete_element(self, net, element_type, element_index):
        return self._post("/net/delete_element", json=locals())

    def delete_elements(self, net, element_type, element_index):
        return self._post("/net/delete_elements", json=locals())


    ### deprecated functions

    def create_element_in_db(self, *args, **kwargs):
        raise RuntimeError("create_element_in_db was deprecated - use create_element instead!")

    def create_elements_in_db(self, *args, **kwargs):
        raise RuntimeError("ph.create_elements_in_db was deprecated - use ph.create_elements instead! "
                      "Watch out for changed order of project_id and variant args")

    def delete_net_element(self, *args, **kwargs):
        raise RuntimeError("ph.delete_net_element was deprecated - use ph.delete_element instead!")


    ### TIMESERIES

    def multi_get_timeseries_from_db(self, filter_document={}, timestamp_range=None,
                                    exclude_timestamp_range=None,
                                    global_database=False):
        ts = self._post("/timeseries/multi_get_timeseries_from_db", json=locals()).json()
        for i, data in enumerate(ts):
            ts[i]["timeseries_data"] = pd.Series(json.loads(data["timeseries_data"]))
        return ts

    def get_timeseries_from_db(self, filter_document={}, timestamp_range=None,
                                    exclude_timestamp_range=None,
                                    global_database=False):
        r = self._post("/timeseries/get_timeseries_from_db", json=locals())
        return pd.Series(json.loads(r.json()))

    def write_timeseries_to_db(self, timeseries, data_type, element_type=None,
                               netname=None, element_index=None, name=None,
                               global_database=False, collection_name="timeseries"):
        json = locals()
        json["timeseries"] = json["timeseries"].to_json(date_format="iso")
        return self._post("/timeseries/write_timeseries_to_db", json=json)



    ### INTERNAL

    def _post(self, path, json=None, authorize=True):
        headers = {'Authorization': 'Bearer {}'.format(self.token)} if authorize else None
        path = self.url + path
        if json is None:
            json = {}
        if json is not None and "self" in json:
            del json['self']
        json = jsonable_encoder(json)
        json["project_id"] = self.project_id
        return requests.post(path, headers=headers, json=json, cert=self.cert)
