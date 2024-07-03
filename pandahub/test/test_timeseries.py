"""Tests for `spinai_backend` package."""
import copy
import pandas as pd
import numpy as np
import pytest
import datetime
import pandapower.networks as nw
import simbench as sb
from pandahub.api.internal import settings

code = "1-HV-urban--0--sw"
project = "pytest"

def test_from_tutorial(ph):
    ph.set_active_project(project)
    net = nw.simple_mv_open_ring_net()
    p_mw_profiles = np.random.randint(low=0, high=100, size=(35041, len(net.load))) / 100 * net.load.p_mw.values
    q_mvar_profiles = np.ones((35041, len(net.load)))
    timestamps = pd.date_range(start="01/01/2020", end="31/12/2020", freq="15min")
    p_mw_profiles = pd.DataFrame(p_mw_profiles, index=timestamps)
    weekindex = p_mw_profiles.index[0:(7 * 96)]
    q_mvar_profiles = pd.DataFrame(q_mvar_profiles, index=timestamps)

    # writing the p_mw profile
    ph.write_timeseries_to_db(timeseries=p_mw_profiles[0], netname="simple_mv_open_ring_net",
                           element_index=0, element_type="load",
                           data_type="p_mw", test_kwarg="test_p_mw", collection_name="test_collection")
    # writing the q_mvar profile
    ph.write_timeseries_to_db(timeseries=q_mvar_profiles[0], netname="simple_mv_open_ring_net",
                           element_index=0, element_type="load",
                           data_type="q_mvar", test_kwarg="test_q_mvar", collection_name= "test_collection")

    result = ph.bulk_get_timeseries_from_db({"netname": 'simple_mv_open_ring_net',
                                                "element_type" : "load",
                                                "element_index": 0,
                                                  },
                                                 collection_name="test_collection",
                                                 pivot_by_column="data_type"
                                                 )

    week = ph.get_timeseries_from_db(netname= 'simple_mv_open_ring_net', element_index=0,
                                          element_type="load",
                                          data_type="p_mw",
                                          collection_name='test_collection',
                                          timestamp_range=(datetime.datetime(2020, 1, 1, 0, 0),
                                                           datetime.datetime(2020, 1, 8, 0, 0)))

    assert (result.keys() == ['p_mw', 'q_mvar']).all()
    assert result.size == 70082
    assert result.index.dtype == "<M8[ns]"
    assert np.isclose(result.p_mw.sum(), p_mw_profiles.sum()[0])
    assert len(week) == 672
    # 17348.07

def test_simbench_sinlge_ts(ph):
    ph.set_active_project(project)
    net2 = sb.get_simbench_net(code)
    profiles = net2.profiles["load"].set_index('time')
    i = net2.load.index[0]
    element_type = 'load'
    data_type = 'p_mw'
    collection = 'test_collection'
    p_mw_profile = profiles[net2.load.profile[i] + "_pload"]
    ph.write_timeseries_to_db(p_mw_profile, netname=code,
                                    element_index=int(i),
                                    element_type=element_type,
                                    data_type=data_type,
                                    collection_name=collection)

    read_profile = ph.get_timeseries_from_db(netname=code,
                                                   element_index=int(i),
                                                   element_type=element_type,
                                                   data_type=data_type,
                                                   collection_name=collection)

    assert p_mw_profile.sum() == read_profile.sum()
    meta = ph.get_timeseries_metadata({"netname": code,
                                             "element_type": element_type,
                                    },
                                   collection_name=collection)

    assert meta.iloc[0].element_type == element_type
    assert meta.iloc[0].data_type == data_type
    assert meta.iloc[0].netname == code
    assert meta.iloc[0].first_timestamp == p_mw_profile.index[0]
    assert meta.iloc[0].last_timestamp == p_mw_profile.index[-1]

def test_simbench_loop_ts(ph):
    ph.set_active_project(project)
    net = sb.get_simbench_net(code)
    profiles = net.profiles["load"].set_index('time')

    for i in net.load.index[:3]:
        i = net.load.index[0]
        p_mw_profile = profiles[net.load.profile[i] + "_pload"]#.astype(float)

        ph.write_timeseries_to_db(p_mw_profile, netname=code, element_index=int(i), element_type='load',
                               data_type='p_mw', collection_name='test_collection')

        read_profile = ph.get_timeseries_from_db(netname=code, element_index=int(i), element_type='load',
                                       data_type='p_mw', collection_name='test_collection')

        assert p_mw_profile.sum() == read_profile.sum()


def test_del_single_ts_on_db(ph):
    ph.set_active_project(project)
    # write ts to db
    net2 = sb.get_simbench_net(code)
    profiles = net2.profiles["load"].set_index('time')
    i = net2.load.index[0]
    p_mw_profile = profiles[net2.load.profile[i] + "_pload"]#.astype(float)
    ph.write_timeseries_to_db(p_mw_profile, netname=code, element_index=int(i), element_type='load',
                               data_type='p_mw', collection_name='test_collection')

    # read ts from db
    read_profile = ph.get_timeseries_from_db(netname=code, element_index=int(i), element_type='load',
                                   data_type='p_mw', collection_name='test_collection')

    assert p_mw_profile.sum() == read_profile.sum()

    # delete ts

    ph.delete_timeseries_from_db(element_type='load',
                                   data_type='p_mw',
                                   element_index=int(i),
                                   collection_name="test_collection",
                                   netname=code)

    # read again
    try:
        ph.get_timeseries_from_db(netname=code,
                                            element_index=int(i),
                                            element_type='load',
                                            data_type='p_mw',
                                            collection_name='test_collection')
        assert False # this line shouldnt be reached, because the function triggers KeyError when no timeseries is found
    except:
        assert True


def test_bulk_ts_on_db(ph):
    ph.set_active_project(project)
    # write bulk cts to db
    p_mw_profiles = np.random.randint(low=0, high=100, size=(35041, 10)).astype(float)
    timestamps = pd.date_range(start="01/01/2020", end="31/12/2020", freq="15min")
    p_mw_profiles_no_time = pd.DataFrame(copy.deepcopy(p_mw_profiles))
    p_mw_profiles = pd.DataFrame(p_mw_profiles, index=timestamps)

    ph.bulk_write_timeseries_to_db(p_mw_profiles,
                                   element_type="load",
                                   data_type="p_mw",
                                   netname="bulk_write_net",
                                   collection_name="test_collection")

    result = ph.bulk_get_timeseries_from_db({"netname": 'bulk_write_net', "element_type": "load",},
                                            collection_name="test_collection",
                                            pivot_by_column="element_index")



    bulk_week = ph.bulk_get_timeseries_from_db({"netname": 'bulk_write_net', "element_type": "load",},
                                               collection_name="test_collection",
                                               pivot_by_column="element_index",
                                               timestamp_range=(datetime.datetime(2020, 1, 1, 0, 0),
                                                                datetime.datetime(2020, 1, 8, 0, 0)))


    p_mw_profiles_no_time = pd.DataFrame(p_mw_profiles_no_time)
    ph.bulk_write_timeseries_to_db(p_mw_profiles_no_time, element_type="sgen", data_type="p_mw",
                                        netname="bulk_write_net", collection_name="test_collection")

    result_no_time = ph.bulk_get_timeseries_from_db({"netname": 'bulk_write_net',
                                                  "element_type": "sgen",
                                                  }, collection_name="test_collection",
                                                 pivot_by_column="element_index"
                                                 )

    assert bulk_week.size == 6720
    assert result_no_time.size == 350410
    assert result.sum().sum() == p_mw_profiles.sum().sum()


    ph.bulk_del_timeseries_from_db({"netname": 'bulk_write_net',
                                                  "element_type": "load",
                                                  }, collection_name="test_collection")

    del_res = ph.bulk_get_timeseries_from_db({"netname": 'bulk_write_net',
                                                  "element_type": "load",
                                                  }, collection_name="test_collection"
                                                 , pivot_by_column="element_index"

                                                 )

    assert len(del_res) == 0

def test_add_metadata(ph):
    ph.set_active_project(project)
    p_mw_profiles = np.random.randint(low=0, high=100, size=(25, 10)).astype(float)
    timestamps = pd.date_range(start="01/01/2020", end="01/02/2020",
                               freq="60min")
    p_mw_profiles = pd.DataFrame(p_mw_profiles, index=timestamps)
    ph.write_timeseries_to_db(p_mw_profiles[0],
                                    netname="test_add_metadata",
                                    element_index=0,
                                    element_type="load",
                                    data_type="p_mw",
                                    test_kwarg="test_p_mw",
                                    collection_name= "test_collection")

    result = ph.get_timeseries_from_db(netname= 'test_add_metadata',
                                             element_index=0,
                                             element_type="load",
                                             data_type="p_mw",
                                             collection_name='test_collection')
    filter = {"netname": 'test_add_metadata',
                "element_type" : "load",
                "element_index": 0,
                 }

    meta_before = ph.get_timeseries_metadata(filter, collection_name="test_collection")

    add_meta = {"max": str(result.values.max())}

    ph.add_metadata(filter, add_meta=add_meta,
                          collection_name="test_collection")
    # check for new metadata
    meta_after = ph.get_timeseries_metadata(filter, collection_name="test_collection")

    assert len(meta_after.columns) == 13
    assert len(meta_before.columns) == 12

def test_bulk_write_with_meta(ph):
    ph.set_active_project(project)
    p_mw_profiles = np.random.randint(low=0, high=100, size=(97, 10)).astype(float)
    timestamps = pd.date_range(start="01/01/2020", end="01/02/2020", freq="15min")
    p_mw_profiles = pd.DataFrame(p_mw_profiles, index=timestamps)
    meta = pd.DataFrame(p_mw_profiles.max(), columns=["max"], dtype=object)

    ph.bulk_write_timeseries_to_db(p_mw_profiles, element_type="meta_test_load", data_type="p_mw",
                                netname="bulk_write_net", collection_name="test_collection",
                                meta_frame=meta)

    result = ph.bulk_get_timeseries_from_db({"netname": 'bulk_write_net',
                                                     "element_type": "meta_test_load",
                                                     },
                                         collection_name="test_collection",
                                         pivot_by_column="element_index")

    bulk_meta = ph.get_timeseries_metadata({"netname": 'bulk_write_net',
                                                  "element_type": "meta_test_load",
                                                  },
                                                 collection_name="test_collection")

    assert len(bulk_meta) == 10



if __name__ == '__main__':
    from pandahub import PandaHub

    ph = PandaHub(connection_url=settings.MONGODB_URL)

    project_name = "pytest"

    if ph.project_exists(project_name):
        ph.set_active_project(project_name)
        ph.delete_project(i_know_this_action_is_final=True)

    ph.create_project(project_name)
    ph.set_active_project(project_name)

    p_mw_profiles = np.random.randint(low=0, high=100, size=(24, 10))
    timestamps = pd.date_range(start="01/01/2020", end="01/02/2020",
                               freq="60min", closed="right")
    p_mw_profiles = pd.DataFrame(p_mw_profiles, index=timestamps)
    ph.write_timeseries_to_db(p_mw_profiles[0],
                                    netname="test_add_metadata",
                                    element_index=0,
                                    element_type="load",
                                    data_type="p_mw",
                                    test_kwarg="test_p_mw",
                                    collection_name= "test_collection")

    result = ph.get_timeseries_from_db(netname= 'test_add_metadata',
                                             element_index=0,
                                             element_type="load",
                                             data_type="p_mw",
                                             collection_name='test_collection')
    filter = {"netname": 'test_add_metadata',
                "element_type" : "load",
                "element_index": 0,
                 }

    meta_before = ph.get_timeseries_metadata(filter, collection_name="test_collection")

    add_meta = {"max": str(result.values.max())}

    ph.add_metadata(filter, add_meta=add_meta, collection_name="test_collection")
    # check for new metadata
    meta_after = ph.get_timeseries_metadata(filter, collection_name="test_collection")

    assert len(meta_after.columns) == 12
    assert len(meta_before.columns) == 11

