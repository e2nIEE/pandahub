from pymongo import DESCENDING, GEOSPHERE, IndexModel

mongodb_indexes = {
    # pandapower
    "net_bus": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("geo", GEOSPHERE)]),
    ],
    "net_line": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("from_bus", DESCENDING)]),
        IndexModel([("to_bus", DESCENDING)]),
        IndexModel([("geo", GEOSPHERE)]),
    ],
    "net_trafo":[
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("hv_bus", DESCENDING)]),
        IndexModel([("lv_bus", DESCENDING)]),
    ],
    "net_switch": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("bus", DESCENDING)]),
        IndexModel([("element", DESCENDING)]),
        IndexModel([("et", DESCENDING)]),
    ],
    "net_load": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("bus", DESCENDING)]),
    ],
    "net_sgen": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("bus", DESCENDING)]),
    ],
    "net_gen": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("bus", DESCENDING)]),
    ],
    "net_ext_grid": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("bus", DESCENDING)]),
        IndexModel([("junction", DESCENDING)]),
    ],
    "net_shunt": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("bus", DESCENDING)]),
    ],
    "net_xward": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("bus", DESCENDING)]),
    ],
    "net_ward": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("bus", DESCENDING)]),
    ],
    "net_motor": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("bus", DESCENDING)]),
    ],
    "net_storage": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("bus", DESCENDING)]),
    ],

    # pandapipes
    "net_junction": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("geo", GEOSPHERE)]),
    ],
    "net_pipe": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("from_junction", DESCENDING)]),
        IndexModel([("to_junction", DESCENDING)]),
        IndexModel([("geo", GEOSPHERE)]),
    ],
    "net_valve": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("from_junction", DESCENDING)]),
        IndexModel([("to_junction", DESCENDING)]),
    ],
    "net_sink": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("junction", DESCENDING)]),
    ],
    "net_source": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("junction", DESCENDING)]),
    ],
    "net_water_tank": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("junction", DESCENDING)]),
    ],
    "net_flow_control": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("from_junction", DESCENDING)]),
        IndexModel([("to_junction", DESCENDING)]),
    ],
    "net_press_control": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("from_junction", DESCENDING)]),
        IndexModel([("to_junction", DESCENDING)]),
    ],
    "net_compressor": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("from_junction", DESCENDING)]),
        IndexModel([("to_junction", DESCENDING)]),
    ],
    "net_pump": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("from_junction", DESCENDING)]),
        IndexModel([("to_junction", DESCENDING)]),
    ],
    "net_circ_pump_mass": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("return_junction", DESCENDING)]),
        IndexModel([("flow_junction", DESCENDING)]),
    ],
    "net_circ_pump_pressure": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("return_junction", DESCENDING)]),
        IndexModel([("flow_junction", DESCENDING)]),
    ],
    "net_heat_exchanger": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("from_junction", DESCENDING)]),
        IndexModel([("to_junction", DESCENDING)]),
    ],
    "net_heat_consumer": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("from_junction", DESCENDING)]),
        IndexModel([("to_junction", DESCENDING)]),
    ],

    # others
    "net_area": [
        IndexModel(
            [("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)],
            unique=True,
        ),
        IndexModel([("index", DESCENDING)]),
        IndexModel([("buses", DESCENDING)]),
        IndexModel([("lines", DESCENDING)]),
        IndexModel([("connection_points", DESCENDING)]),
        IndexModel([("feeders", DESCENDING)]),
        IndexModel([("trafos", DESCENDING)]),
        IndexModel([("substations", DESCENDING)]),
        IndexModel([("type", DESCENDING)]),
        IndexModel([("substation_buses", DESCENDING)]),
        IndexModel([("level", DESCENDING)]),
        IndexModel([("geo", GEOSPHERE)]),
        IndexModel([("variant", DESCENDING)]),
    ],
    "net_substation": [
        IndexModel(
            [("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)],
            unique=True,
        ),
        IndexModel([("index", DESCENDING)]),
        IndexModel([("type", DESCENDING)]),
        IndexModel([("level", DESCENDING)]),
        IndexModel([("geo", GEOSPHERE)]),
    ],
}
