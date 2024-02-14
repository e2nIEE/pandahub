# -*- coding: utf-8 -*-
"""
Created on Sun Nov 26 12:25:55 2023

@author: LeonThurner
"""

import pandapower as pp
from pandahub import PandaHub
import time
from line_profiler import LineProfiler

def get_test_net(n_buses):
    net = pp.create_empty_network()
    pp.create_buses(net, n_buses, vn_kv=0.4)
    pp.create_loads(net, net.bus.index[::50], p_mw=0, q_mvar=0)
    pp.create_sgens(net, net.bus.index[::50], p_mw=0, q_mvar=0)
    pp.create_gens(net, net.bus.index[::1000], p_mw=0, q_mvar=0)
    pp.create_lines(net, net.bus.index[::2], net.bus.index[1::2], 1.0, "NAYY 4x50 SE")
    pp.create_transformers_from_parameters(net, net.bus.index[::1000], 
                                            net.bus.index[1::1000],
                                            vn_hv_kv=20.,
                                            vn_lv_kv=0.4,
                                            sn_mva=0.4,
                                            vkr_percent=5.,
                                            vk_percent=0.5, 
                                            pfe_kw=0,
                                            i0_percent=0,
                                            )
    line_buses = list(net.line.from_bus.values) + list(net.line.to_bus.values)
    line_indices = list(net.line.index) + list(net.line.index)
    pp.create_switches(net, line_buses, line_indices, "l")
    return net
    
def write_test_net_to_mongodb(net, project_name):
    ph = PandaHub()
    if ph.project_exists(project_name):
        ph.set_active_project(project_name)
        ph.delete_project(True)
    ph.create_project(project_name)
    ph.write_network_to_db(net, "test_net")
    
def load_test_subnet_from_mongodb(project_name):
    ph = PandaHub()
    ph.set_active_project(project_name)
    buses = list(range(1000))
    t0 = time.time()
    subnet = ph.get_subnet_from_db("test_net", 
                                 bus_filter={"index": {"$in": buses}})
    t1 = time.time()
    print("LOADED NET IN %.2fs"%(t1-t0))
    return subnet

def profile_load_test(project_name):
    lp = LineProfiler()
    lp_wrapper = lp(load_test_subnet_from_mongodb)
    lp.add_function(PandaHub.get_subnet_from_db_by_id)
    lp.add_function(PandaHub._add_element_from_collection)
    lp_wrapper(project_name)
    lp.print_stats()
    
if __name__ == '__main__':
    n_buses = 3e6 #3 Million buses
    project_name = "test_%u"%n_buses
    net = get_test_net(n_buses)
    write_test_net_to_mongodb(net, project_name)
    profile_load_test(project_name)
    # subnet = load_test_subnet_from_mongodb(project_name)
