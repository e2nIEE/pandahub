"""Utility functions for serializing, converting, and storing pandapower/pandapipes network data in MongoDB."""

import base64
import hashlib
import importlib
import json
import logging
import blosc
import numpy as np
import pandas as pd

from pandahub.lib.datatypes import DATATYPES

logger = logging.getLogger(__name__)
from packaging import version
from pandapower.io_utils import PPJSONEncoder
from pymongo.database import Database


def get_document_hash(task) -> str:
    """Return a hash value of the input task to generate consistent but unique _ids.

    Parameters
    ----------
    task : dict
        task that shall be hashed.

    Returns
    -------
    sha256 hash
        hash value that can be uses as _id.

    """
    hasher = hashlib.sha256()
    hasher.update(repr(make_task_hashable(task)).encode())
    return base64.urlsafe_b64encode(hasher.digest()).decode()


def make_task_hashable(task):
    """Make a task dict hashable.

    Parameters
    ----------
    task : dict
        task that shall be made hashable.

    Returns
    -------
    TYPE
        hashable task.
    """
    if isinstance(task, (tuple, list)):
        return tuple(make_task_hashable(e) for e in task)

    if isinstance(task, dict):
        return tuple(sorted((k, make_task_hashable(v)) for k, v in task.items()))

    if isinstance(task, (set, frozenset)):
        return tuple(sorted(make_task_hashable(e) for e in task))
    return task


def add_timestamp_info_to_document(document: dict, timeseries: pd.Series, ts_format: str) -> dict:
    """Add meta information to documents that contain time series.

    The document will get the additional attributes 'first_timestamp',
    'last_tiemstamp' and 'num_timestamps' (number of timestamps). This
    information can be used to calculate the timeseries' resolution, to
    check, if there are any timestamps missing or to query documents based
    on their contained tiemstamps.

    Parameters
    ----------
    document : dict
        dict, that contains the timeseries' metadata.
    timeseries : pandas.Series
        The timeseries itself with the timestamps as index.

    Returns
    -------
    document : dict
        The updated timeseries metadata document.

    """
    if ts_format == "timestamp_value":
        document["first_timestamp"] = timeseries.index[0]
        document["last_timestamp"] = timeseries.index[-1]
    document["num_timestamps"] = len(timeseries.index)
    document["max_value"] = timeseries.max()
    document["min_value"] = timeseries.min()
    return document


def convert_timeseries_to_subdocuments(timeseries: pd.Series) -> list[dict]:
    """Convert a timeseries to a list of dicts.

    Every dict represents one timestep and contains the keys 'timestamp' and 'value'
    as well as the according values.

    Parameters
    ----------
    timeseries : pandas.Series
        A timeseries with the timestamps as index.

    Returns
    -------
    subdocuments : list
        List of timestep dictionaries.

    """
    subdocuments = []
    for timestamp, value in list(timeseries.items()):
        subdocuments.append({"timestamp": timestamp, "value": value})
    return subdocuments


def compress_timeseries_data(timeseries_data: pd.Series, ts_format: str) -> bytes | None:
    """Compress timeseries data using blosc."""
    if ts_format == "timestamp_value":
        timeseries_data = np.array([timeseries_data.index.astype("int64"), timeseries_data.values])
        return blosc.compress(timeseries_data.tobytes(), shuffle=blosc.SHUFFLE, cname="zlib")
    if ts_format == "array":
        return blosc.compress(timeseries_data.astype(float).values.tobytes(), shuffle=blosc.SHUFFLE, cname="zlib")


def decompress_timeseries_data(timeseries_data: bytes, ts_format: str, num_timestamps: int):
    """Decompress blosc-compressed timeseries data back to a pandas Series or numpy array."""
    if ts_format == "timestamp_value":
        data = np.frombuffer(blosc.decompress(timeseries_data), dtype=np.float64).reshape(
            (num_timestamps, 2), order="F"
        )
        return pd.Series(data[:, 1], index=pd.to_datetime(data[:, 0]))
    if ts_format == "array":
        return np.frombuffer(blosc.decompress(timeseries_data), dtype=np.float64)


def create_timeseries_document(
    timeseries: pd.Series,
    data_type: str,
    ts_format: str = "timestamp_value",
    compress_ts_data: bool = False,
    **kwargs,
) -> dict:
    """Create a document containing timeseries metadata and data.

    Uses 'add_timestamp_info_to_document' to add information about the embedded timeseries
    and 'convert_timeseries_to_subdocuments' to convert the input timeseries to a list of
    subdocuments accessible via the "timeseries_data" key. The element_type and data_type of
    the timeseries are required metadata, netname and element_index are optional.
    Additionally, arbitrary kwargs can be added.

    By default, the document will have an _id generated based on its metadata
    using 'get_document_hash', but an _id provided by the user as a kwarg will not be overwritten.


    Parameters
    ----------
    timeseries : pandas.Series
        A timeseries with the timestamps as index.
    element_type : str
        Kind of element the timeseries belongs to (load/sgen).
    data_type : str
        Type and unit the timeseries values' are given in. The recommended format
        is <type>_<unit> (e.g. p_mw).
    netname : str, optional
        Name of the network the timeseries belongs to. Is only added to the
        document if any value is specified. The default is None.
    element_index : int (could also be str, but int is recommended), optional
        The index of the element the timeseries belongs to. Is only added to the
        document if any value is specified. The default is None.
    **kwargs :
        Any additional metadata that shall be added to the document.

    Returns
    -------
    document : dict
        dict, representing a timeseries.

    """
    document = {"data_type": data_type, "ts_format": ts_format, "compressed_ts_data": compress_ts_data}
    document = add_timestamp_info_to_document(document, timeseries, ts_format)
    document = {**document, **kwargs}

    if "_id" not in document:  # IDs set by users will not be overwritten
        document["_id"] = get_document_hash(document)

    if compress_ts_data:
        document["timeseries_data"] = compress_timeseries_data(timeseries, ts_format)
    elif ts_format == "timestamp_value":
        document["timeseries_data"] = convert_timeseries_to_subdocuments(timeseries)
    elif ts_format == "array":
        document["timeseries_data"] = list(timeseries.values)

    return document


def convert_element_to_dict(element_data: pd.DataFrame, net_id, default_dtypes: dict | None = None) -> list[dict]:
    """Convert a pandapower pandas.DataFrame element into a list of record dicts.

    Casts columns to default dtypes. Columns of type Object are serialized to json.
    Columns named "geo" or "*_geo" containing strings are parsed into dicts.
    net_id and index (from element_data df index) are added as values.

    Parameters
    ----------
    element_data: pandas.DataFrame
        pandapower element table to convert to dict
    net_id: int
        Network id
    default_dtypes: dict
        Default dtypes for columns in element_data

    Returns
    -------
    dict
        Record-orientated dict representation of element_data

    """
    if default_dtypes is not None:
        for column in element_data.columns:
            if column in default_dtypes:
                element_data[column] = element_data[column].astype(default_dtypes[column], errors="ignore")

    if "object" in element_data.columns:
        element_data["object"] = element_data["object"].apply(object_to_json)
    if "q_max_characteristic" in element_data.columns:
        element_data["q_max_characteristic"] = element_data["q_max_characteristic"].apply(object_to_json)
        element_data["q_min_characteristic"] = element_data["q_min_characteristic"].apply(object_to_json)
    element_data["index"] = element_data.index
    element_data["net_id"] = net_id
    load_geojsons(element_data)
    return element_data.to_dict(orient="records")


def convert_dataframes_to_dicts(net, net_id, version_, datatypes: dict = DATATYPES) -> tuple[dict, dict, dict]:
    """Convert all DataFrame elements of a pandapower/pandapipes network to dicts.

    Returns separate dicts for dataframe elements, non-dataframe parameters, and column dtypes.
    """
    dataframes = {}
    other_parameters = {}
    types = {}
    for key, data in net.items():
        if key.startswith("_") or key.startswith("res"):
            continue
        if isinstance(data, pd.core.frame.DataFrame):
            # ------------
            # create type lookup
            types[key] = get_dtypes(data, datatypes.get(key))
            if data.empty:
                continue
            # ------------
            # convert pandapower objects in dataframes to dict
            dataframes[key] = convert_element_to_dict(data.copy(deep=True), net_id, datatypes.get(key))
        else:
            data = serialize_object_data(key, data, version_)
            if data:
                other_parameters[key] = data

    return dataframes, other_parameters, types


def serialize_object_data(element: str, element_data, version_):
    """Serialize a pandapower element which is not of type pandas.DataFrame into json.

    Parameters
    ----------
    element: str
        Name of the pandapower element
    element_data: object
        pandapower element data
    version_:
        pandahub version to target for serialization

    Returns
    -------
    json
        A json representation of the pandapower element
    """
    if version_ <= version.parse("0.2.3"):
        try:
            element_data = json.dumps(element_data, cls=PPJSONEncoder)
        except:
            print(f"Data in net[{element}] is not JSON serializable and was therefore omitted on import")
        else:
            return element_data
    else:
        try:
            json.dumps(element_data)
        except:
            element_data = f"serialized_{json.dumps(element_data, cls=PPJSONEncoder)}"
        return element_data


def get_dtypes(element_data: pd.DataFrame, default_dtypes: dict | None) -> dict[str, str]:
    """Construct data types from a pandas.DataFrame, with given defaults taking precedence.

    Parameters
    ----------
    element_data: pandas.DataFrame
        Input dataframe
    default_dtypes: dict
        Default datatypes definition

    Returns
    -------
    dict
        Datatypes for all columns present in element_data. Column type is taken from default_dtypes if defined,
        otherwise directly from element_data

    """
    types = {}
    if default_dtypes is not None:
        types.update({key: dtype.__name__ for key, dtype in default_dtypes.items()})
    types.update({column: str(dtype) for column, dtype in element_data.dtypes.items() if column not in types})
    return types


def load_geojsons(df: pd.DataFrame) -> None:
    """Parse GeoJSON strings in geo columns of a DataFrame into dicts in-place."""
    for column in df.columns:
        if column == "geo" or column.endswith("_geo"):
            df[column] = df[column].apply(lambda a: json.loads(a) if isinstance(a, str) else a)


def convert_geojsons(df: pd.DataFrame, geo_mode: str = "string") -> None:
    """Convert geo columns of a DataFrame to the specified representation in-place.

    Supports "dict", "string", and "shapely" output formats.
    """

    def to_dict(geo):
        if isinstance(geo, dict):
            return geo
        if isinstance(geo, str):
            return json.loads(geo)
        if hasattr(geo, "coords"):
            return {"type": geo.type, "coordinates": geo.coords}

    def to_string(geo):
        if isinstance(geo, str):
            return geo
        if isinstance(geo, dict):
            return json.dumps(geo)
        if hasattr(geo, "coords"):
            return json.dumps({"type": geo.type, "coordinates": geo.coords})

    def to_shapely(geo):
        from shapely.geometry import shape

        if hasattr(geo, "coords"):
            return geo
        if isinstance(geo, str):
            return shape(json.loads(geo))
        if isinstance(geo, dict):
            return shape(geo)

    conv_func = None
    if geo_mode == "dict":
        conv_func = to_dict
    elif geo_mode == "string":
        conv_func = to_string
    elif geo_mode == "shapely":
        conv_func = to_shapely
    else:
        raise NotImplementedError(f"Unknown geo_mode {geo_mode}")

    for column in df.columns:
        if column == "geo" or column.endswith("_geo"):
            df[column] = df[column].apply(conv_func)


def json_to_object(js: dict):
    """Deserialize a JSON-encoded object back to its original Python class instance."""
    _module = importlib.import_module(js["_module"])
    _class = getattr(_module, js["_class"])
    return _class.from_json(js["_object"])


def object_to_json(obj) -> dict:
    """Serialize a Python object to a JSON-compatible dict with module/class metadata."""
    return {"_module": obj.__class__.__module__, "_class": obj.__class__.__name__, "_object": obj.to_json()}


def migrate_userdb_to_beanie(ph) -> None:
    """Migrate existing users to beanie backend used by pandahub >= 0.3.0.

    Will raise an exception if the user database is inconsistent, and return silently if no users need to be migrated.
    See pandahub v0.3.0 release notes for details!

    Parameters
    ----------
    ph: pandahub.PandaHub
        PandaHub instance with connected mongodb database to apply migrations to.

    Returns
    -------
    None
    """
    from datetime import datetime

    from pymongo.errors import OperationFailure

    userdb_backup = ph.mongo_client["user_management"][datetime.now().strftime("users_fa9_%Y-%m-%d_%H-%M")]
    userdb = ph.mongo_client["user_management"]["users"]
    old_users = list(userdb.find({"_id": {"$type": "objectId"}}))
    new_users = list(userdb.find({"_id": {"$not": {"$type": "objectId"}}}))
    if old_users and new_users:
        old_users = [user.get("email") for user in old_users]
        new_users = [user.get("email") for user in new_users]
        raise RuntimeError(
            "Inconsistent user database - you need to resolve conflicts manually! "
            "See pandahub v0.3.0 release notes for details."
            f"pandahub < 0.3.0 users: {old_users}"
            f"pandahub >= 0.3.0 users: {new_users}"
        )
    if not old_users:
        return
    userdb_backup.insert_many(old_users)
    try:
        userdb.drop_index("id_1")
    except OperationFailure as e:
        if e.code == 27:
            pass
        else:
            raise e

    migration = [{"$addFields": {"_id": "$id"}}, {"$unset": "id"}, {"$out": "users"}]
    userdb.aggregate(migration)


def get_metadata_for_timeseries_collections(
    db: Database,
    data_type: str | None = None,
    net_id=None,
    element_type: str | None = None,
    element_index=None,
    **kwargs,
) -> dict:
    """Build a metadata dict for a timeseries collection query, validating required fields."""
    if element_type is None:
        raise ValueError("element_type needs to be defined for timeseries collections")
    if element_index is None:
        raise ValueError("element_index needs to be defined for timeseries collections")
    if data_type is None:
        raise ValueError("data_type needs to be defined for timeseries collections")
    if net_id is None:
        net_ids = db["_networks"].distinct("_id")
        if len(net_ids) == 1:
            net_id = net_ids[0]
        else:
            raise ValueError(
                "No net_id was provided and multiple networks exist in the database. Please provide a net_id."
            )
    metadata = {
        "data_type": data_type,
        "net_id": net_id,
        "element_type": element_type,
        "element_index": element_index,
        **kwargs,
    }
    metadata["_id"] = f"{net_id}_{element_type}_{element_index}_{data_type}"
    return metadata
