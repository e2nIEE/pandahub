import pytest

import pandapipes as pps
import pandapipes.networks as nw_pps
import pandapower as pp
import pandapower.networks as nw_pp
from pandahub import PandaHubError
from pandapipes.toolbox import nets_equal
from pandahub.api.internal import settings


def test_additional_res_tables(ph):
    import pandas as pd
    ph.set_active_project("pytest")

    # reset project aka delete everything
    db = ph._get_project_database()
    for cname in db.list_collection_names():
        db.drop_collection(cname)

    net1 = pp.create_empty_network()
    net1['res_test'] = pd.DataFrame(data={'col1': [1, 2], 'col2': [3, 4]})
    ph.write_network_to_db(net1, 'test')
    net2 = ph.get_net_from_db('test')

    assert('res_test' in net2)
    assert(net1.res_test.shape == (2,2))


def test_network_io(ph):
    ph.set_active_project("pytest")
    # reset project aka delete everything
    db = ph._get_project_database()
    for cname in db.list_collection_names():
        db.drop_collection(cname)

    net1 = nw_pp.mv_oberrhein()
    name1 = "oberrhein_network"
    # for some unknown reason the format of mv_oberrhein does not match the latest pandapower format
    net1.gen.rename(columns={"qmax_mvar": "max_q_mvar", "qmin_mvar": "min_q_mvar"}, inplace=True)
    del net1.shunt["scaling"]
    del net1.impedance["r_pu"]
    del net1.impedance["x_pu"]
    del net1.dcline["cost_per_mw"]

    net2 = nw_pp.simple_four_bus_system()
    pp.create_bus(net2, vn_kv=20, index=10)  # check non-consecutive indices
    name2 = "simple_network"

    # we check storing two different networks consecutively to ensure the nets are properly separated
    for net, name in [(net1, name1), (net2, name2)]:
        if ph.network_with_name_exists(name):
            ph.delete_net_from_db(name)

        assert ph.network_with_name_exists(name) == False

        ph.write_network_to_db(net, name)
        assert ph.network_with_name_exists(name) == True

        net_loaded = ph.get_net_from_db(name)

        pp.runpp(net)
        pp.runpp(net_loaded)

        # This will fail if the net contains 'None' values. Since they get casted to NaN which by definition
        # doesn't compare to itself
        # assert pp.nets_equal(net, net_loaded, check_dtype=False)

        net3 = ph.get_net_from_db(name, only_tables=["bus"])
        assert len(net3.bus) == len(net.bus)
        assert len(net3.line) == 0
        assert len(net3.load) == 0

    # delete first network
    ph.delete_net_from_db(name1)
    assert ph.network_with_name_exists(name1) == False

    # check that second network is still in database
    assert ph.network_with_name_exists(name2) == True
    net2_loaded = ph.get_net_from_db(name2)
    pp.runpp(net2_loaded)
    assert pp.nets_equal(net2, net2_loaded, check_only_results=True)


def test_load_subnetwork(ph):
    ph.set_active_project("pytest")
    name = "oberrhein_network"

    if not ph.network_with_name_exists(name):
        net = nw_pp.mv_oberrhein()
        ph.write_network_to_db(net, name)

    subnet = ph.get_subnet_from_db(name, bus_filter={"vn_kv": 110})
    expected_sizes = [("bus", 4), ("line", 0), ("trafo", 2), ("ext_grid", 2)]

    for element, size in expected_sizes:
        assert len(subnet[element]) == size
        assert len(subnet["res_" + element]) == size

    subnet = ph.get_subnet_from_db(name, bus_filter={"vn_kv": 110},
                                   include_results=False)
    for element, size in expected_sizes:
        assert len(subnet[element]) == size
        assert len(subnet["res_" + element]) == 0

    subnet = ph.get_subnet_from_db(name, bus_filter={"vn_kv": 110},
                                   add_edge_branches=False)
    expected_sizes = [("bus", 2), ("line", 0), ("trafo", 0), ("ext_grid", 2)]

    for element, size in expected_sizes:
        assert len(subnet[element]) == size
        assert len(subnet["res_" + element]) == size


def test_access_and_set_single_values(ph):
    ph.set_active_project("pytest")
    name = "oberrhein_network"

    net = nw_pp.mv_oberrhein()
    if not ph.network_with_name_exists(name):
        ph.write_network_to_db(net, name)

    element = "sgen"
    parameter = "p_mw"
    index = 4
    p_mw_new = 0.1

    value = ph.get_net_value_from_db(name, element, index, parameter)
    assert value == net[element][parameter].at[index]

    ph.set_net_value_in_db(name, element, index, parameter, p_mw_new)
    value = ph.get_net_value_from_db(name, element, index, parameter)
    assert value == p_mw_new

    ph.delete_element(name, element, index)
    with pytest.raises(PandaHubError):
        ph.get_net_value_from_db(name, element, index, parameter)
    net = ph.get_net_from_db(name)
    assert index not in net[element].index


def test_pandapipes(ph):
    ph.set_active_project('pytest')
    net = nw_pps.gas_versatility()
    ph.write_network_to_db(net, 'versatility')
    net2 = ph.get_net_from_db('versatility', convert=False)
    pps.pipeflow(net)
    pps.pipeflow(net2)
    assert nets_equal(net, net2, check_only_results=True)


def test_get_set_single_value(ph):
    ph.set_active_project('pytest')
    net = nw_pp.mv_oberrhein()
    ph.write_network_to_db(net, 'oberrhein')
    val = ph.get_net_value_from_db('oberrhein', 'load', 0, 'p_mw')
    assert val == net.load.at[0, 'p_mw']
    ph.set_net_value_in_db('oberrhein', 'load', 0, 'p_mw', 0.5)
    val = ph.get_net_value_from_db('oberrhein', 'load', 0, 'p_mw')
    assert val == 0.5


if __name__ == '__main__':
    from pandahub import PandaHub

    ph = PandaHub(connection_url=settings.MONGODB_URL)
    ph.create_project('pytest')
    net = nw_pps.gas_versatility()
    ph.write_network_to_db(net, 'versatility')
    net2 = ph.get_net_from_db('versatility')
    pps.pipeflow(net)
    pps.pipeflow(net2)
    # test_network_io(ph)
    # 0 / 0
    # project_name = "pytest"

    # if ph.project_exists(project_name):
    #     ph.set_active_project(project_name)
    #     ph.delete_project(i_know_this_action_is_final=True)

    # ph.create_project(project_name)
    # ph.set_active_project("pytest")
    # net = ph.get_net_from_db("oberrhein_network")
    # name = "oberrhein_network"

    # net = nw.mv_oberrhein()
    # if not ph.network_with_name_exists(name):
    #     ph.write_network_to_db(net, name)

    # element = "sgen"
    # parameter = "p_mw"
    # index = 4
    # p_mw_new = 0.1

    # value = ph.get_net_value_from_db(name, element, index, parameter)
    # assert value == net[element][parameter].at[index]

    # ph.set_net_value_in_db(name, element, index, parameter, p_mw_new)
    # value = ph.get_net_value_from_db(name, element, index, parameter)
    # assert value == p_mw_new

    # ph.delete_net_element(name, element, index)
    # with pytest.raises(PandaHubError):
    #     ph.get_net_value_from_db(name, element, index, parameter)
    # net = ph.get_net_from_db(name)
    # assert index not in net[element].index
