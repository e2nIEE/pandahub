from pymongo import DESCENDING, GEOSPHERE, IndexModel

mongodb_indexes = {
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
}
