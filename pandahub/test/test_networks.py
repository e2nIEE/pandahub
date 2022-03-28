import pandapower.networks as nw
from pandahub import PandaHubError
import pandapower as pp
import pytest

def test_network_io(ph):
    ph.set_active_project("pytest")

    net1 = nw.mv_oberrhein()
    name1 = "oberrhein_network"

    net2 = nw.simple_four_bus_system()
    pp.create_bus(net2, vn_kv=20, index=10) #check non-consecutive indices
    name2 = "simple_network"

    #we check storing two different networks consecutively to ensure the nets are properly separated
    for net, name in [(net1, name1), (net2, name2)]:
        if ph.network_with_name_exists(name):
            ph.delete_net_from_db(name)
        
        assert ph.network_with_name_exists(name) == False
            
        ph.write_network_to_db(net, name)
        assert ph.network_with_name_exists(name) == True
        
        net_loaded = ph.get_net_from_db(name)
        
        pp.runpp(net)
        pp.runpp(net_loaded)
        
        assert pp.nets_equal(net, net_loaded, check_only_results=True)
    
        net3 = ph.get_net_from_db(name, only_tables=["bus"])
        assert len(net3.bus) == len(net.bus)
        assert len(net3.line) == 0
        assert len(net3.load) == 0
    
    #delete first network
    ph.delete_net_from_db(name1)
    assert ph.network_with_name_exists(name1) == False

    #check that second network is still in database    
    assert ph.network_with_name_exists(name2) == True
    net2_loaded = ph.get_net_from_db(name2)
    pp.runpp(net2_loaded)
    assert pp.nets_equal(net2, net2_loaded, check_only_results=True)

def test_load_subnetwork(ph):
    ph.set_active_project("pytest")
    name = "oberrhein_network"
    
    if not ph.network_with_name_exists(name):
        net = nw.mv_oberrhein()
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
    
    net = nw.mv_oberrhein()
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

    ph.delete_net_element(name, element, index)
    with pytest.raises(PandaHubError):
        ph.get_net_value_from_db(name, element, index, parameter)
    net = ph.get_net_from_db(name)    
    assert index not in net[element].index
    
  
if __name__ == '__main__':
    from pandahub import PandaHub

    ph = PandaHub(connection_url="mongodb://localhost:27017")

    project_name = "pytest"
    
    if ph.project_exists(project_name):
        ph.set_active_project(project_name)
        ph.delete_project(i_know_this_action_is_final=True)
    
    ph.create_project(project_name)
    ph.set_active_project(project_name)
    
    name = "oberrhein_network"
    
    net = nw.mv_oberrhein()
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

    ph.delete_net_element(name, element, index)
    with pytest.raises(PandaHubError):
        ph.get_net_value_from_db(name, element, index, parameter)
    net = ph.get_net_from_db(name)    
    assert index not in net[element].index
    
    

    