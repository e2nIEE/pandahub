# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
from pandahub.api.internal import settings
import base64
import hashlib
import logging
import json
import importlib
logger = logging.getLogger(__name__)


def get_document_hash(task):
    """
    Returns a hash value of the input task in order to generate consistent but
    unique _ids.

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
    """
    Makes a task dict hashable.
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
        return tuple((make_task_hashable(e) for e in task))

    if isinstance(task, dict):
        return tuple(sorted((k,make_task_hashable(v)) for k,v in task.items()))

    if isinstance(task, (set, frozenset)):
        return tuple(sorted(make_task_hashable(e) for e in task))
    return task


def add_timestamp_info_to_document(document, timeseries, ts_format):
    """
    Adds some meta information to documents that containg time series.
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
    document["max_value"] = timeseries.max().item()
    document["min_value"] = timeseries.min().item()
    return document


def convert_timeseries_to_subdocuments(timeseries):
    """
    Converts a timeseries to a list of dicts. Every dict represents one timestep
    and contains the keys 'timestamp' and 'value' as well as the according values.

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
        subdocuments.append({"timestamp": timestamp,
                             "value": value})
    return subdocuments


def compress_timeseries_data(timeseries_data, ts_format):
    import blosc
    if ts_format == "timestamp_value":
        timeseries_data = np.array([timeseries_data.index.astype(int), 
                                    timeseries_data.values])
        return blosc.compress(timeseries_data.tobytes(),
                              shuffle=blosc.SHUFFLE,
                              cname="zlib")
    elif ts_format == "array":
        return blosc.compress(timeseries_data.astype(float).values.tobytes(),
                              shuffle=blosc.SHUFFLE,
                              cname="zlib")


def decompress_timeseries_data(timeseries_data, ts_format):
    import blosc
    if ts_format == "timestamp_value":
        data = np.frombuffer(blosc.decompress(timeseries_data), 
                             dtype=np.float64).reshape((35040,2), 
                                                       order="F")
        return pd.Series(data[:,1], index=pd.to_datetime(data[:,0]))
    elif ts_format == "array":
        return np.frombuffer(blosc.decompress(timeseries_data), 
                             dtype=np.float64)
        

def create_timeseries_document(timeseries, 
                               data_type, 
                               ts_format="timestamp_value",
                               compress_ts_data=False,
                               **kwargs):
    """
    Creates a document that contains timeseries metadata as well as the timeseries
    itself. Uses the function 'add_timestamp_info_to_document' to add information
    about the embedded timeseries and the function 'convert_timeseries_to_subdocuments'
    to convert the input timeseries to a list of subdocuments, that can be accessed
    with the attribute "timeseries_data". The element_type and data_type of the
    timeseries are required metadata, netname and element_index are optional.
    Additionally, arbitrary kwargs can be added.

    By default, the document will have an _id generated based on its metadata
    using the function get_document_hash but an _id provided by the user as a
    kwarg will not be overwritten.


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
    document = {"data_type": data_type,
                "ts_format": ts_format,
                "compressed_ts_data": compress_ts_data}
    document = add_timestamp_info_to_document(document, timeseries, ts_format)
    document = {**document, **kwargs}
    if not "_id" in document: # IDs set by users will not be overwritten
        document["_id"] = get_document_hash(document)
    if compress_ts_data:
        document["timeseries_data"] = compress_timeseries_data(timeseries, 
                                                               ts_format)
    else:
        if ts_format == "timestamp_value":
            document["timeseries_data"] = convert_timeseries_to_subdocuments(timeseries)
        elif ts_format == "array":
            document["timeseries_data"] = list(timeseries.values)
    
    return document

def convert_dataframes_to_dicts(net, _id, datatypes=None):
    if datatypes is None:
        datatypes = getattr(importlib.import_module(settings.DATATYPES_MODULE), "datatypes")

    dataframes = {}
    other_parameters = {}
    types = {}
    for key, data in net.items():
        if key.startswith("_") or key.startswith("res"):
            continue
        if isinstance(data, pd.core.frame.DataFrame):

            # ------------
            # create type lookup

            types[key] = dict()
            default_dtypes = datatypes.get(key)
            if default_dtypes is not None:
                types[key].update({key: dtype.__name__ for key, dtype in default_dtypes.items()})
            types[key].update(
                {
                    column: str(dtype) for column, dtype in net[key].dtypes.items()
                    if column not in types[key]
                }
            )
            if data.empty:
                continue

            # ------------
            # convert pandapower objects in dataframes to dict

            df = net[key].copy(deep=True)

            # ------------
            # cast all columns with their default datatype

            if default_dtypes is not None:
                for column in df.columns:
                    if column in default_dtypes:
                        df[column] = df[column].astype(default_dtypes[column], errors="ignore")

            if "object" in df.columns:
                df["object"] = df["object"].apply(object_to_json)
            df["index"] = df.index
            df["net_id"] = _id
            load_geojsons(df)
            dataframes[key] = df.to_dict(orient="records")
        else:
            try:
                json.dumps(data)
            except:
                print("Data in net[{}] is not JSON serializable and was therefore omitted on import".format(key))
            else:
                other_parameters[key] = data
    return dataframes, other_parameters, types

def load_geojsons(df):
    for column in df.columns:
        if column == "geo" or column.endswith("_geo"):
            df[column] = df[column].apply(lambda a: json.loads(a) if isinstance(a, str) else a)

def convert_geojsons(df, geo_mode="string"):
    if geo_mode == "dict":
        return
    for column in df.columns:
        if column == "geo" or column.endswith("_geo"):
            if geo_mode == "string":
                df[column] = df[column].apply(lambda a: json.dumps(a) if isinstance(a, dict) else a)
            elif geo_mode == "shapely":
                from shapely.geometry import shape
                df[column] = df[column].apply(lambda a: shape(a) if isinstance(a, dict) else a)
            else:
                raise NotImplementedError("Unknown geo_mode {}".format(geo_mode))

def json_to_object(js):
    _module = importlib.import_module(js["_module"])
    _class = getattr(_module, js["_class"])
    return _class.from_json(js["_object"])

def object_to_json(obj):
    return {
        "_module": obj.__class__.__module__,
        "_class": obj.__class__.__name__,
        "_object": obj.to_json()
    }