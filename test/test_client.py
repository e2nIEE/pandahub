# -*- coding: utf-8 -*-

from pandahub import PandaHubClient
import pandapower.networks as nw


def test_client_io(phc: PandaHubClient):
    phc.set_active_project("Manderbach")

    net = nw.mv_oberrhein()
    phc.write_network_to_db(net, name='mv_oberrhein', overwrite=True)
    net_loaded = phc.get_net_from_db(name='mv_oberrhein')

    assert len(net_loaded) != 0


if __name__ == '__main__':
    phc = PandaHubClient()
    phc.set_active_project("Manderbach")
    net = phc.get_net_from_db("power")

    ts = phc.multi_get_timeseries_from_db({"name": "H0"}, global_database=True)
    phc.write_timeseries_to_db(ts[0]["timeseries_data"], "p_mw", "sgen")
