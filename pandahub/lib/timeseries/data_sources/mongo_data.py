import numpy as np
import datetime
from pandahub.mongo_io_methods import MongoIOMethods
from pandapower.timeseries.data_source import DataSource
import pandas as pd

try:
    import pplog
    logger = pplog.getLogger(__name__)
except ImportError:
    import logging


class MongoData(DataSource):
    """
    Fetches timeseries data from a mongodb
    """

    def __init__(self, io_methods: MongoIOMethods, netname: str, db_name: str, element_index: list,
                 data_type='p_mw', element_type='load', collection_name='timeseries_data', prefetch_count=1000,
                 **kwargs):

        super(MongoData, self).__init__()
        self.db_name = db_name
        self.collection_name = collection_name

        if type(element_index) is pd.Int64Index:
            element_index = element_index.values.tolist()

        filter = {
            "netname": netname,
            "data_type": data_type,
            "element_type": element_type,
            # "element_index": element_index
        }
        self.filter = {**filter, **kwargs}

        self.io_methods = io_methods

        self.tseries = None
        self.prefetch_count = prefetch_count
        self.current_fetch_position = -1
        self.metadata = self.io_methods.get_timeseries_metadata(filter_document=self.filter, db_name=self.db_name,
                                                                collection_name=self.collection_name)

        first_timestamp = self.metadata['first_timestamp'].values[0]

        if type(first_timestamp) == str:
            self.first_timestamp = datetime.datetime.fromisoformat(first_timestamp)
        elif type(first_timestamp) == np.datetime64:
            self.first_timestamp = pd.Timestamp(first_timestamp).to_pydatetime()
        else:
            self.first_timestamp = first_timestamp

    def get_time_step_value(self, time_step, profile_name, scale_factor=1.0):
        fs = self.first_timestamp + datetime.timedelta(minutes=15 * time_step)
        if time_step >= self.current_fetch_position:
            self.current_fetch_position = time_step + self.prefetch_count
            # fs = self.first_timestamp + datetime.timedelta(minutes=15*time_step)
            es = self.first_timestamp + datetime.timedelta(minutes=15*self.current_fetch_position)
            self.tseries = self.io_methods.bulk_get_timeseries_from_db(filter_document=self.filter, db_name=self.db_name,
                                                                       collection_name=self.collection_name,
                                                                       timestamp_range=[fs, es],
                                                                       pivot_by_column="element_index")

        try:
            return self.tseries.loc[fs.isoformat(), profile_name] * scale_factor
        except KeyError:
            if type(profile_name) == pd.Int64Index:
                self.tseries.columns = self.tseries.columns.astype(int)
                return self.tseries.loc[fs.isoformat(), profile_name] * scale_factor
            elif type(profile_name) == list:
                self.tseries.columns = self.tseries.columns.astype(str)
                t = self.tseries.loc[fs.isoformat(), profile_name] * scale_factor
                t.index = t.index.astype(int)
                return t
