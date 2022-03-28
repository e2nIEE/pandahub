# -*- coding: utf-8 -*-
"""
Created on Tue Feb  2 11:32:07 2021

@author: julffers
"""

import pandas as pd
import pandapower as pp
import pandapipes as pps
import base64
import hashlib
import logging
import json
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


def add_timestamp_info_to_document(document, timeseries):
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

def create_timeseries_document(timeseries, data_type, element_type=None,
                               netname=None, element_index=None, **kwargs):
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
    document = {"data_type": data_type}
    if element_type is not None:
        document["element_type"] = element_type
    if netname is not None:
        document["netname"] = netname
    if element_index is not None:
        document["element_index"] = element_index
    document = add_timestamp_info_to_document(document, timeseries)
    document = {**document, **kwargs}
    if not "_id" in document: # IDs set by users will not be overwritten
        document["_id"] = get_document_hash(document)
    document["timeseries_data"] = convert_timeseries_to_subdocuments(timeseries)
    return document

def convert_dataframes_to_dicts(net, _id):
    dataframes = {}
    other_parameters = {}
    types = {}
    for key, data in net.items():
        if key.startswith("_"):
            continue
        if isinstance(data, pd.core.frame.DataFrame):
            if data.empty:
                continue
            # convert pandapower objects in dataframes to dict
            if "object" in net[key].columns:
                net[key]["object"] = net[key]["object"].apply(lambda obj: obj.to_dict())
            net[key]["index"] = net[key].index
            net[key]["net_id"] = _id
            dataframes[key] = net[key].to_dict(orient="records")
            net[key].drop(columns=["index", "net_id"], inplace=True)
            types[key] = {column: str(dtype) for column, dtype
                                       in net[key].dtypes.items()}
        else:
            try:
                json.dumps(data)
            except:
                print("Data in net[{}] is not JSON serializable and was therefore omitted on import".format(key))
            else:
                other_parameters[key] = data
    return dataframes, other_parameters, types
