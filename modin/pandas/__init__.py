# Licensed to Modin Development Team under one or more contributor license agreements.
# See the NOTICE file distributed with this work for additional information regarding
# copyright ownership.  The Modin Development Team licenses this file to you under the
# Apache License, Version 2.0 (the "License"); you may not use this file except in
# compliance with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under
# the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific language
# governing permissions and limitations under the License.

import pandas

__pandas_version__ = "1.1.3"

if pandas.__version__ != __pandas_version__:
    import warnings

    warnings.warn(
        "The pandas version installed {} does not match the supported pandas version in"
        " Modin {}. This may cause undesired side effects!".format(
            pandas.__version__, __pandas_version__
        )
    )

from pandas import (
    eval,
    cut,
    factorize,
    test,
    qcut,
    date_range,
    period_range,
    Index,
    MultiIndex,
    CategoricalIndex,
    bdate_range,
    DatetimeIndex,
    Timedelta,
    Timestamp,
    to_timedelta,
    set_eng_float_format,
    options,
    set_option,
    NaT,
    PeriodIndex,
    Categorical,
    Interval,
    UInt8Dtype,
    UInt16Dtype,
    UInt32Dtype,
    UInt64Dtype,
    SparseDtype,
    Int8Dtype,
    Int16Dtype,
    Int32Dtype,
    Int64Dtype,
    StringDtype,
    BooleanDtype,
    CategoricalDtype,
    DatetimeTZDtype,
    IntervalDtype,
    PeriodDtype,
    RangeIndex,
    Int64Index,
    UInt64Index,
    Float64Index,
    TimedeltaIndex,
    IntervalIndex,
    IndexSlice,
    Grouper,
    array,
    Period,
    show_versions,
    DateOffset,
    timedelta_range,
    infer_freq,
    interval_range,
    ExcelWriter,
    datetime,
    NamedAgg,
    NA,
)
import threading
import os
import multiprocessing

from modin.config import Engine, Parameter

# Set this so that Pandas doesn't try to multithread by itself
os.environ["OMP_NUM_THREADS"] = "1"
DEFAULT_NPARTITIONS = 4
num_cpus = 1


_is_first_update = {}
dask_client = None
_NOINIT_ENGINES = {
    "Python",
}  # engines that don't require initialization, useful for unit tests


def _update_engine(publisher: Parameter):
    global DEFAULT_NPARTITIONS, dask_client, num_cpus
    from modin.config import Backend, CpuCount

    if publisher.get() == "Ray":
        import ray
        from modin.engines.ray.utils import initialize_ray

        # With OmniSci backend there is only a single worker per node
        # and we allow it to work on all cores.
        if Backend.get() == "Omnisci":
            CpuCount.put(1)
            os.environ["OMP_NUM_THREADS"] = str(multiprocessing.cpu_count())
        if _is_first_update.get("Ray", True):
            initialize_ray()
        num_cpus = ray.cluster_resources()["CPU"]
    elif publisher.get() == "Dask":  # pragma: no cover
        from distributed.client import get_client

        if threading.current_thread().name == "MainThread" and _is_first_update.get(
            "Dask", True
        ):
            import warnings

            warnings.warn("The Dask Engine for Modin is experimental.")

            try:
                dask_client = get_client()
            except ValueError:
                from distributed import Client

                dask_client = Client(n_workers=CpuCount.get())

    elif publisher.get() == "Cloudray":
        from modin.experimental.cloud import get_connection

        conn = get_connection()
        remote_ray = conn.modules["ray"]
        if _is_first_update.get("Cloudray", True):

            @conn.teleport
            def init_remote_ray(partition):
                from ray import ray_constants
                import modin
                from modin.engines.ray.utils import initialize_ray

                modin.set_backends("Ray", partition)
                initialize_ray(
                    override_is_cluster=True,
                    override_redis_address=f"localhost:{ray_constants.DEFAULT_PORT}",
                    override_redis_password=ray_constants.REDIS_DEFAULT_PASSWORD,
                )

            init_remote_ray(Backend.get())
            # import EngineDispatcher here to initialize IO class
            # so it doesn't skew read_csv() timings later on
            import modin.data_management.factories.dispatcher  # noqa: F401
        else:
            get_connection().modules["modin"].set_backends("Ray", Backend.get())

        num_cpus = remote_ray.cluster_resources()["CPU"]
    elif publisher.get() == "Cloudpython":
        from modin.experimental.cloud import get_connection

        get_connection().modules["modin"].set_backends("Python")

    elif publisher.get() not in _NOINIT_ENGINES:
        raise ImportError("Unrecognized execution engine: {}.".format(publisher.get()))

    _is_first_update[publisher.get()] = False
    DEFAULT_NPARTITIONS = max(4, int(num_cpus))


Engine.subscribe(_update_engine)

from .. import __version__
from .dataframe import DataFrame
from .io import (
    read_csv,
    read_parquet,
    read_json,
    read_html,
    read_clipboard,
    read_excel,
    read_hdf,
    read_feather,
    read_stata,
    read_sas,
    read_pickle,
    read_sql,
    read_gbq,
    read_table,
    read_fwf,
    read_sql_table,
    read_sql_query,
    read_spss,
    ExcelFile,
    to_pickle,
    HDFStore,
    json_normalize,
    read_orc,
)
from .series import Series
from .general import (
    concat,
    isna,
    isnull,
    merge,
    merge_asof,
    merge_ordered,
    pivot_table,
    notnull,
    notna,
    pivot,
    to_numeric,
    to_datetime,
    unique,
    value_counts,
    get_dummies,
    melt,
    crosstab,
    lreshape,
    wide_to_long,
)
from .plotting import Plotting as plotting

### MERGE CONFLICT BEGIN
from .. import __execution_engine__ as execution_engine

# Set this so that Pandas doesn't try to multithread by itself
os.environ["OMP_NUM_THREADS"] = "1"
num_cpus = 1


def initialize_ray():
    import ray

    """Initializes ray based on environment variables and internal defaults."""
    if threading.current_thread().name == "MainThread":
        import secrets

        plasma_directory = None
        cluster = os.environ.get("MODIN_RAY_CLUSTER", None)
        redis_address = os.environ.get("MODIN_REDIS_ADDRESS", None)
        redis_password = secrets.token_hex(16)
        if cluster == "True" and redis_address is not None:
            # We only start ray in a cluster setting for the head node.
            ray.init(
                include_webui=False,
                ignore_reinit_error=True,
                redis_address=redis_address,
                redis_password=redis_password,
                logging_level=100,
            )
        elif cluster is None:
            object_store_memory = os.environ.get("MODIN_MEMORY", None)
            if os.environ.get("MODIN_OUT_OF_CORE", "False").title() == "True":
                from tempfile import gettempdir

                plasma_directory = gettempdir()
                # We may have already set the memory from the environment variable, we don't
                # want to overwrite that value if we have.
                if object_store_memory is None:
                    # Round down to the nearest Gigabyte.
                    mem_bytes = ray.utils.get_system_memory() // 10 ** 9 * 10 ** 9
                    # Default to 8x memory for out of core
                    object_store_memory = 8 * mem_bytes
            # In case anything failed above, we can still improve the memory for Modin.
            if object_store_memory is None:
                # Round down to the nearest Gigabyte.
                object_store_memory = int(
                    0.6 * ray.utils.get_system_memory() // 10 ** 9 * 10 ** 9
                )
                # If the memory pool is smaller than 2GB, just use the default in ray.
                if object_store_memory == 0:
                    object_store_memory = None
            else:
                object_store_memory = int(object_store_memory)
            ray.init(
                include_webui=False,
                ignore_reinit_error=True,
                plasma_directory=plasma_directory,
                object_store_memory=object_store_memory,
                redis_address=redis_address,
                redis_password=redis_password,
                logging_level=100,
                memory=object_store_memory,
            )
        # Register custom serializer for method objects to avoid warning message.
        # We serialize `MethodType` objects when we use AxisPartition operations.
        ray.register_custom_serializer(types.MethodType, use_pickle=True)

        # Register a fix import function to run on all_workers including the driver.
        # This is a hack solution to fix #647, #746
        def move_stdlib_ahead_of_site_packages(*args):
            site_packages_path = None
            site_packages_path_index = -1
            for i, path in enumerate(sys.path):
                if sys.exec_prefix in path and path.endswith("site-packages"):
                    site_packages_path = path
                    site_packages_path_index = i
                    # break on first found
                    break

            if site_packages_path is not None:
                # stdlib packages layout as follows:
                # - python3.x
                #   - typing.py
                #   - site-packages/
                #     - pandas
                # So extracting the dirname of the site_packages can point us
                # to the directory containing standard libraries.
                sys.path.insert(
                    site_packages_path_index, os.path.dirname(site_packages_path)
                )

        move_stdlib_ahead_of_site_packages()
        ray.worker.global_worker.run_function_on_all_workers(
            move_stdlib_ahead_of_site_packages
        )


if execution_engine == "Ray":
    import ray

    initialize_ray()
    num_cpus = ray.cluster_resources()["CPU"]
elif execution_engine == "Dask":  # pragma: no cover
    from distributed.client import get_client
    import warnings

    if threading.current_thread().name == "MainThread":
        warnings.warn("The Dask Engine for Modin is experimental.")
        try:
            client = get_client()
        except ValueError:
            from distributed import Client
            import multiprocessing

            num_cpus = multiprocessing.cpu_count()
            client = Client(n_workers=num_cpus)
elif execution_engine == "MPI":
    from modin.engines.mpi4py import _get_global_executor
    import multiprocessing

    num_cpus = multiprocessing.cpu_count()
    _get_global_executor()
elif execution_engine != "Python":
    raise ImportError("Unrecognized execution engine: {}.".format(execution_engine))

DEFAULT_NPARTITIONS = max(4, int(num_cpus))
### MERGE CONFLICT END

__all__ = [
    "DataFrame",
    "Series",
    "read_csv",
    "read_parquet",
    "read_json",
    "read_html",
    "read_clipboard",
    "read_excel",
    "read_hdf",
    "read_feather",
    "read_stata",
    "read_sas",
    "read_pickle",
    "read_sql",
    "read_gbq",
    "read_table",
    "read_spss",
    "read_orc",
    "json_normalize",
    "concat",
    "eval",
    "cut",
    "factorize",
    "test",
    "qcut",
    "to_datetime",
    "get_dummies",
    "isna",
    "isnull",
    "merge",
    "pivot_table",
    "date_range",
    "Index",
    "MultiIndex",
    "Series",
    "bdate_range",
    "period_range",
    "DatetimeIndex",
    "to_timedelta",
    "set_eng_float_format",
    "options",
    "set_option",
    "CategoricalIndex",
    "Timedelta",
    "Timestamp",
    "NaT",
    "PeriodIndex",
    "Categorical",
    "__version__",
    "melt",
    "crosstab",
    "plotting",
    "Interval",
    "UInt8Dtype",
    "UInt16Dtype",
    "UInt32Dtype",
    "UInt64Dtype",
    "SparseDtype",
    "Int8Dtype",
    "Int16Dtype",
    "Int32Dtype",
    "Int64Dtype",
    "CategoricalDtype",
    "DatetimeTZDtype",
    "IntervalDtype",
    "PeriodDtype",
    "BooleanDtype",
    "StringDtype",
    "NA",
    "RangeIndex",
    "Int64Index",
    "UInt64Index",
    "Float64Index",
    "TimedeltaIndex",
    "IntervalIndex",
    "IndexSlice",
    "Grouper",
    "array",
    "Period",
    "show_versions",
    "DateOffset",
    "timedelta_range",
    "infer_freq",
    "interval_range",
    "ExcelWriter",
    "read_fwf",
    "read_sql_table",
    "read_sql_query",
    "ExcelFile",
    "to_pickle",
    "HDFStore",
    "lreshape",
    "wide_to_long",
    "merge_asof",
    "merge_ordered",
    "notnull",
    "notna",
    "pivot",
    "to_numeric",
    "unique",
    "value_counts",
    "datetime",
    "NamedAgg",
    "DEFAULT_NPARTITIONS",
]

del pandas, Engine, Parameter
