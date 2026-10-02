import datetime
from typing import Any, override

import numpy as np
import pandas as pd
from pandapower.timeseries.data_source import DataSource

from pandahub.mongo_io_methods import MongoIOMethods

try:
    import pplog

    logger = pplog.getLogger(__name__)
except ImportError:
    pass


class MongoData(DataSource):
    """Fetch timeseries data from a MongoDB."""

    def __init__(
        self,
        io_methods: MongoIOMethods,
        netname: str,
        db_name: str,
        element_index: list,
        data_type: str = "p_mw",
        element_type: str = "load",
        collection_name: str = "timeseries_data",
        prefetch_count: int = 1000,
        **kwargs: Any,
    ) -> None:

        super().__init__()
        self.db_name = db_name
        self.collection_name = collection_name

        if isinstance(element_index, pd.Int64Index):
            element_index = element_index.to_numpy().tolist()

        query = {
            "netname": netname,
            "data_type": data_type,
            "element_type": element_type,
            # "element_index": element_index
        }
        self.filter = {**query, **kwargs}

        self.io_methods = io_methods

        self.tseries = None
        self.prefetch_count = prefetch_count
        self.current_fetch_position = -1
        self.metadata = self.io_methods.get_timeseries_metadata(
            filter_document=self.filter, db_name=self.db_name, collection_name=self.collection_name
        )

        first_timestamp = self.metadata["first_timestamp"].to_numpy()[0]

        if isinstance(first_timestamp, str):
            self.first_timestamp = datetime.datetime.fromisoformat(first_timestamp)
        elif isinstance(first_timestamp, np.datetime64):
            self.first_timestamp = pd.Timestamp(first_timestamp).to_pydatetime()
        else:
            self.first_timestamp = first_timestamp

    @override
    def get_time_step_value(self, time_step, profile_name, scale_factor: float = 1.0):
        fs = self.first_timestamp + datetime.timedelta(minutes=15 * time_step)
        if time_step >= self.current_fetch_position:
            self.current_fetch_position = time_step + self.prefetch_count
            # fs = self.first_timestamp + datetime.timedelta(minutes=15*time_step)
            es = self.first_timestamp + datetime.timedelta(minutes=15 * self.current_fetch_position)
            self.tseries = self.io_methods.bulk_get_timeseries_from_db(
                filter_document=self.filter,
                db_name=self.db_name,
                collection_name=self.collection_name,
                timestamp_range=[fs, es],
                pivot_by_column="element_index",
            )

        try:
            return self.tseries.loc[fs.isoformat(), profile_name] * scale_factor
        except KeyError:
            if isinstance(profile_name, pd.Int64Index):
                self.tseries.columns = self.tseries.columns.astype(int)
                return self.tseries.loc[fs.isoformat(), profile_name] * scale_factor
            if isinstance(profile_name, list):
                self.tseries.columns = self.tseries.columns.astype(str)
                t = self.tseries.loc[fs.isoformat(), profile_name] * scale_factor
                t.index = t.index.astype(int)
                return t
