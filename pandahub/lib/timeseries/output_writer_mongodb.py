from datetime import datetime
from types import FunctionType
from typing import Any, override

import numpy as np
import pandas as pd
from pandapower.timeseries import OutputWriter

from pandahub.mongo_io_methods import MongoIOMethods

try:
    import pplog

    logger = pplog.getLogger(__name__)
except ImportError:
    pass


class OutputWriterMongoDB(OutputWriter):
    """Output writer that writes timeseries results to a MongoDB database."""

    def __init__(
        self,
        net,
        io_methods: MongoIOMethods,
        netname: str,
        db_name: str,
        start_date: datetime,
        time_steps=None,
        write_time=None,
        log_variables=None,
        write_caching: int = 10,
        freq: str = "15min",
        collection_name: str = "timeseries_data",
        **kwargs: Any,
    ) -> None:
        super().__init__(net, time_steps=time_steps, write_time=write_time, log_variables=log_variables)
        self.io_methods = io_methods
        self.args = kwargs
        self.NET_NAME = netname
        self.db_name = db_name
        self.write_caching = write_caching
        self.current_pos = 0
        self.ids = {}
        self.collection_name = collection_name
        self.output = {}
        self.freq = freq
        self.start_date = start_date

    # def dump_to_file(self, net, append=False, recycle_options=None):
    #     pass

    # def _save_single_xls_sheet(self, append):
    # TODO: implement save to a single sheet
    #     raise NotImplementedError("Sorry not implemented yet")

    def _init_np_array(self, partial_func):
        (_, _, _, index, eval_function, _) = partial_func.args
        hash_name = self._get_np_name(partial_func.args)
        n_columns = len(index)
        if eval_function is not None:
            n_columns = 1
            if isinstance(eval_function, FunctionType) and "n_columns" in eval_function.__code__.co_varnames:
                    n_columns = eval_function.__defaults__[0]
        # self.np_results[hash_name] = np.zeros((len(self.time_steps), n_columns))
        self.np_results[hash_name] = np.zeros((self.write_caching, n_columns))

    def _log(self, table, variable, net, index, eval_function=None, eval_name=None):
        try:
            # TODO: Create a mask for the numpy array in the beginning and use this one for getting the values. Faster
            if net[table].index.equals(pd.Index(index)):
                # if index equals all values -> get numpy array directly
                result = net[table][variable].to_numpy()
            else:
                # get by loc (slow)
                result = net[table].loc[index, variable].to_numpy()

            if eval_function is not None:
                result = eval_function(result)

            # save results to numpy array
            # time_step_idx = self.time_step_lookup[self.time_step]
            hash_name = self._get_np_name((table, variable, net, index, eval_function, eval_name))
            # self.np_results[hash_name][time_step_idx, :] = result
            self.np_results[hash_name][self.current_pos, :] = result

        except Exception as e:
            logger.error("Error at index %s for %s[%s]: %s", index, table, variable, e)

    def _np_to_pd(self) -> dict[str, pd.DataFrame]:
        # convert numpy arrays (faster so save results) into pd Dataframes (user friendly)
        # intended use: At the end of time series simulation write results to pandas
        res_df = {}

        for partial_func in self.output_list:
            (table, variable, _, index, eval_func, eval_name) = partial_func.args
            # res_name = self._get_hash(table, variable)
            res_name = self._get_output_name(table, variable)
            np_name = self._get_np_name(partial_func.args)
            columns = index
            if eval_name is not None and eval_func is not None:
                if isinstance(eval_func, FunctionType):
                    if "n_columns" not in eval_func.__code__.co_varnames:
                        columns = [eval_name]
                else:
                    columns = [eval_name]

            # res_df = pd.DataFrame(self.np_results[np_name], index=self.time_steps, columns=columns)
            res_df[res_name] = pd.DataFrame(self.np_results[np_name], columns=columns)

        return res_df

    @override
    def save_results(self, _net, time_step, pf_converged: bool, ctrl_converged: bool, _recycle_options=None) -> None:
        # remember the last time step
        self.time_step = time_step

        if not pf_converged:
            super().save_nans_to_parameters()
            self.output["Parameters"].loc[time_step, "powerflow_failed"] = True
        elif not ctrl_converged:
            self.output["Parameters"].loc[time_step, "controller_unstable"] = True
        else:
            super().save_to_parameters()

        res = self._np_to_pd()
        write_to_db = False

        self.current_pos += 1
        if self.current_pos >= self.write_caching:
            write_to_db = True

        # last time_step
        if self.time_step == self.time_steps[-1]:
            print(time_step)
            write_to_db = True

        if write_to_db:
            for res_name, res_df in res.items():
                end = self.start_date + (self.current_pos - 1) * pd.Timedelta(self.freq)
                trimmed_df = res_df.drop(range(self.current_pos, self.write_caching), axis=0) if self.current_pos < self.write_caching else res_df
                trimmed_df.index = pd.date_range(start=self.start_date, end=end, freq=self.freq)
                if res_name in self.ids:
                    for ii in trimmed_df.index:
                        row = trimmed_df.loc[ii]
                        self.io_methods.bulk_update_timeseries_in_db(
                            new_ts_content=pd.DataFrame(row).transpose(),
                            document_ids=self.ids[res_name],
                            db_name=self.db_name,
                            collection_name=self.collection_name,
                            # **self.args
                        )
                else:
                    et = res_name.split(".")[0]
                    dt = res_name.split(".")[1]
                    self.ids[res_name] = self.io_methods.bulk_write_timeseries_to_db(
                        timeseries=trimmed_df,
                        netname=self.NET_NAME,
                        element_type=et,
                        data_type=dt,
                        collection_name=self.collection_name,
                        db_name=self.db_name,
                        return_ids=True,
                        last_timestamp=self.start_date + pd.Timedelta(self.freq) * len(self.time_steps),
                        num_timestamps=len(self.time_steps),
                        **self.args,
                    )

            self.start_date = self.start_date + self.current_pos * pd.Timedelta(self.freq)
            self.current_pos = 0
