from pymongo import DESCENDING, GEOSPHERE, IndexModel

VARIANT_INDEXES = [
    IndexModel([("variant", DESCENDING)]),
    IndexModel([("var_type", DESCENDING)]),
    IndexModel([("not_in_var", DESCENDING)]),
]
MONGODB_INDEXES = {
    # pandapower
    "net_bus": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("geo", GEOSPHERE)]),
        IndexModel([("substation", DESCENDING)]),
        *VARIANT_INDEXES,
    ],
    "net_line": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("from_bus", DESCENDING)]),
        IndexModel([("to_bus", DESCENDING)]),
        IndexModel([("geo", GEOSPHERE)]),
        *VARIANT_INDEXES,
    ],
    "net_trafo":[
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("hv_bus", DESCENDING)]),
        IndexModel([("lv_bus", DESCENDING)]),
        *VARIANT_INDEXES,
    ],
    "net_switch": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("bus", DESCENDING)]),
        IndexModel([("element", DESCENDING)]),
        IndexModel([("et", DESCENDING)]),
        *VARIANT_INDEXES,
    ],
    "net_load": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("bus", DESCENDING)]),
        *VARIANT_INDEXES,
    ],
    "net_sgen": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("bus", DESCENDING)]),
        *VARIANT_INDEXES,
    ],
    "net_gen": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("bus", DESCENDING)]),
        *VARIANT_INDEXES,
    ],
    "net_ext_grid": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("bus", DESCENDING)]),
        IndexModel([("junction", DESCENDING)]),
        *VARIANT_INDEXES,
    ],
    "net_shunt": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("bus", DESCENDING)]),
    ],
    "net_xward": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("bus", DESCENDING)]),
        *VARIANT_INDEXES,
    ],
    "net_ward": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("bus", DESCENDING)]),
        *VARIANT_INDEXES,
    ],
    "net_motor": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("bus", DESCENDING)]),
        *VARIANT_INDEXES,
    ],
    "net_storage": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("bus", DESCENDING)]),
        *VARIANT_INDEXES,
    ],

    # pandapipes
    "net_junction": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("geo", GEOSPHERE)]),
        *VARIANT_INDEXES,
    ],
    "net_pipe": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("from_junction", DESCENDING)]),
        IndexModel([("to_junction", DESCENDING)]),
        IndexModel([("geo", GEOSPHERE)]),
        *VARIANT_INDEXES,
    ],
    "net_valve": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("from_junction", DESCENDING)]),
        IndexModel([("to_junction", DESCENDING)]),
        *VARIANT_INDEXES,
    ],
    "net_sink": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("junction", DESCENDING)]),
        *VARIANT_INDEXES,
    ],
    "net_source": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("junction", DESCENDING)]),
        *VARIANT_INDEXES,
    ],
    "net_water_tank": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("junction", DESCENDING)]),
        *VARIANT_INDEXES,
    ],
    "net_flow_control": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("from_junction", DESCENDING)]),
        IndexModel([("to_junction", DESCENDING)]),
        *VARIANT_INDEXES,
    ],
    "net_press_control": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("from_junction", DESCENDING)]),
        IndexModel([("to_junction", DESCENDING)]),
        *VARIANT_INDEXES,
    ],
    "net_compressor": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("from_junction", DESCENDING)]),
        IndexModel([("to_junction", DESCENDING)]),
        *VARIANT_INDEXES,
    ],
    "net_pump": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("from_junction", DESCENDING)]),
        IndexModel([("to_junction", DESCENDING)]),
        *VARIANT_INDEXES,
    ],
    "net_circ_pump_mass": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("return_junction", DESCENDING)]),
        IndexModel([("flow_junction", DESCENDING)]),
        *VARIANT_INDEXES,
    ],
    "net_circ_pump_pressure": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("return_junction", DESCENDING)]),
        IndexModel([("flow_junction", DESCENDING)]),
        *VARIANT_INDEXES,
    ],
    "net_heat_exchanger": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("from_junction", DESCENDING)]),
        IndexModel([("to_junction", DESCENDING)]),
        *VARIANT_INDEXES,
    ],
    "net_heat_consumer": [
        IndexModel([("net_id", DESCENDING), ("index", DESCENDING), ("variant", DESCENDING)], unique=True),
        IndexModel([("from_junction", DESCENDING)]),
        IndexModel([("to_junction", DESCENDING)]),
        *VARIANT_INDEXES,
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
        *VARIANT_INDEXES,
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
        *VARIANT_INDEXES,
    ],
}
