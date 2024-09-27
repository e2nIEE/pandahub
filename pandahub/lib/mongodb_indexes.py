from pymongo import DESCENDING as DESC, GEOSPHERE, IndexModel

VARIANT_INDEXES = [
    IndexModel([("variant", DESC)]),
    IndexModel([("var_type", DESC)]),
    IndexModel([("not_in_var", DESC)]),
]
MONGODB_INDEXES = {
    # pandapower
    "net_bus": [
        IndexModel([("net_id", DESC), ("index", DESC), ("variant", DESC)], unique=True),
        IndexModel([("geo", GEOSPHERE)]),
        IndexModel([("substation", DESC)]),
        *VARIANT_INDEXES,
    ],
    "net_line": [
        IndexModel([("net_id", DESC), ("index", DESC), ("variant", DESC)], unique=True),
        IndexModel([("from_bus", DESC)]),
        IndexModel([("to_bus", DESC)]),
        IndexModel([("geo", GEOSPHERE)]),
        *VARIANT_INDEXES,
    ],
    "net_trafo": [
        IndexModel([("net_id", DESC), ("index", DESC), ("variant", DESC)], unique=True),
        IndexModel([("hv_bus", DESC)]),
        IndexModel([("lv_bus", DESC)]),
        *VARIANT_INDEXES,
    ],
    "net_switch": [
        IndexModel([("net_id", DESC), ("index", DESC), ("variant", DESC)], unique=True),
        IndexModel([("bus", DESC)]),
        IndexModel([("element", DESC)]),
        IndexModel([("et", DESC)]),
        *VARIANT_INDEXES,
    ],
    "net_load": [
        IndexModel([("net_id", DESC), ("index", DESC), ("variant", DESC)], unique=True),
        IndexModel([("bus", DESC)]),
        *VARIANT_INDEXES,
    ],
    "net_sgen": [
        IndexModel([("net_id", DESC), ("index", DESC), ("variant", DESC)], unique=True),
        IndexModel([("bus", DESC)]),
        *VARIANT_INDEXES,
    ],
    "net_gen": [
        IndexModel([("net_id", DESC), ("index", DESC), ("variant", DESC)], unique=True),
        IndexModel([("bus", DESC)]),
        *VARIANT_INDEXES,
    ],
    "net_ext_grid": [
        IndexModel([("net_id", DESC), ("index", DESC), ("variant", DESC)], unique=True),
        IndexModel([("bus", DESC)]),
        IndexModel([("junction", DESC)]),
        *VARIANT_INDEXES,
    ],
    "net_shunt": [
        IndexModel([("net_id", DESC), ("index", DESC), ("variant", DESC)], unique=True),
        IndexModel([("bus", DESC)]),
    ],
    "net_xward": [
        IndexModel([("net_id", DESC), ("index", DESC), ("variant", DESC)], unique=True),
        IndexModel([("bus", DESC)]),
        *VARIANT_INDEXES,
    ],
    "net_ward": [
        IndexModel([("net_id", DESC), ("index", DESC), ("variant", DESC)], unique=True),
        IndexModel([("bus", DESC)]),
        *VARIANT_INDEXES,
    ],
    "net_motor": [
        IndexModel([("net_id", DESC), ("index", DESC), ("variant", DESC)], unique=True),
        IndexModel([("bus", DESC)]),
        *VARIANT_INDEXES,
    ],
    "net_storage": [
        IndexModel([("net_id", DESC), ("index", DESC), ("variant", DESC)], unique=True),
        IndexModel([("bus", DESC)]),
        *VARIANT_INDEXES,
    ],
    # pandapipes
    "net_junction": [
        IndexModel([("net_id", DESC), ("index", DESC), ("variant", DESC)], unique=True),
        IndexModel([("geo", GEOSPHERE)]),
        *VARIANT_INDEXES,
    ],
    "net_pipe": [
        IndexModel([("net_id", DESC), ("index", DESC), ("variant", DESC)], unique=True),
        IndexModel([("from_junction", DESC)]),
        IndexModel([("to_junction", DESC)]),
        IndexModel([("geo", GEOSPHERE)]),
        *VARIANT_INDEXES,
    ],
    "net_valve": [
        IndexModel([("net_id", DESC), ("index", DESC), ("variant", DESC)], unique=True),
        IndexModel([("from_junction", DESC)]),
        IndexModel([("to_junction", DESC)]),
        *VARIANT_INDEXES,
    ],
    "net_sink": [
        IndexModel([("net_id", DESC), ("index", DESC), ("variant", DESC)], unique=True),
        IndexModel([("junction", DESC)]),
        *VARIANT_INDEXES,
    ],
    "net_source": [
        IndexModel([("net_id", DESC), ("index", DESC), ("variant", DESC)], unique=True),
        IndexModel([("junction", DESC)]),
        *VARIANT_INDEXES,
    ],
    "net_water_tank": [
        IndexModel([("net_id", DESC), ("index", DESC), ("variant", DESC)], unique=True),
        IndexModel([("junction", DESC)]),
        *VARIANT_INDEXES,
    ],
    "net_flow_control": [
        IndexModel([("net_id", DESC), ("index", DESC), ("variant", DESC)], unique=True),
        IndexModel([("from_junction", DESC)]),
        IndexModel([("to_junction", DESC)]),
        *VARIANT_INDEXES,
    ],
    "net_press_control": [
        IndexModel([("net_id", DESC), ("index", DESC), ("variant", DESC)], unique=True),
        IndexModel([("from_junction", DESC)]),
        IndexModel([("to_junction", DESC)]),
        *VARIANT_INDEXES,
    ],
    "net_compressor": [
        IndexModel([("net_id", DESC), ("index", DESC), ("variant", DESC)], unique=True),
        IndexModel([("from_junction", DESC)]),
        IndexModel([("to_junction", DESC)]),
        *VARIANT_INDEXES,
    ],
    "net_pump": [
        IndexModel([("net_id", DESC), ("index", DESC), ("variant", DESC)], unique=True),
        IndexModel([("from_junction", DESC)]),
        IndexModel([("to_junction", DESC)]),
        *VARIANT_INDEXES,
    ],
    "net_circ_pump_mass": [
        IndexModel([("net_id", DESC), ("index", DESC), ("variant", DESC)], unique=True),
        IndexModel([("return_junction", DESC)]),
        IndexModel([("flow_junction", DESC)]),
        *VARIANT_INDEXES,
    ],
    "net_circ_pump_pressure": [
        IndexModel([("net_id", DESC), ("index", DESC), ("variant", DESC)], unique=True),
        IndexModel([("return_junction", DESC)]),
        IndexModel([("flow_junction", DESC)]),
        *VARIANT_INDEXES,
    ],
    "net_heat_exchanger": [
        IndexModel([("net_id", DESC), ("index", DESC), ("variant", DESC)], unique=True),
        IndexModel([("from_junction", DESC)]),
        IndexModel([("to_junction", DESC)]),
        *VARIANT_INDEXES,
    ],
    "net_heat_consumer": [
        IndexModel([("net_id", DESC), ("index", DESC), ("variant", DESC)], unique=True),
        IndexModel([("from_junction", DESC)]),
        IndexModel([("to_junction", DESC)]),
        *VARIANT_INDEXES,
    ],
    # others
    "net_area": [
        IndexModel(
            [("net_id", DESC), ("index", DESC), ("variant", DESC)],
            unique=True,
        ),
        IndexModel([("index", DESC)]),
        IndexModel([("buses", DESC)]),
        IndexModel([("lines", DESC)]),
        IndexModel([("connection_points", DESC)]),
        IndexModel([("feeders", DESC)]),
        IndexModel([("trafos", DESC)]),
        IndexModel([("substations", DESC)]),
        IndexModel([("type", DESC)]),
        IndexModel([("substation_buses", DESC)]),
        IndexModel([("level", DESC)]),
        IndexModel([("geo", GEOSPHERE)]),
        *VARIANT_INDEXES,
    ],
    "net_substation": [
        IndexModel(
            [("net_id", DESC), ("index", DESC), ("variant", DESC)],
            unique=True,
        ),
        IndexModel([("index", DESC)]),
        IndexModel([("type", DESC)]),
        IndexModel([("level", DESC)]),
        IndexModel([("geo", GEOSPHERE)]),
        *VARIANT_INDEXES,
    ],
}

{"geometry": {"coordinates": [9.84125980224971, 50.88414180743948], "type": "Point"}}
