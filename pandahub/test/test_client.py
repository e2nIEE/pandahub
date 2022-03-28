# -*- coding: utf-8 -*-

from pandahub import PandaHubClient
import pandas as pd

if __name__ == '__main__':
    ph = PandaHubClient()
    ph.set_active_project("Manderbach")
    net = ph.get_net_from_db("power")

    ts = ph.multi_get_timeseries_from_db({"name": "H0"}, global_database=True)
    ph.write_timeseries_to_db(ts[0]["timeseries_data"], "p_mw", "sgen")
