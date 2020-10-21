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

import warnings

from ._version import get_versions


def custom_formatwarning(msg, category, *args, **kwargs):
    # ignore everything except the message
    return "{}: {}\n".format(category.__name__, msg)


warnings.formatwarning = custom_formatwarning
# Filter numpy version warnings because they are not relevant
warnings.filterwarnings("ignore", message="numpy.dtype size changed")
warnings.filterwarnings("ignore", message="Large object of size")
warnings.filterwarnings(
    "ignore",
    message="The pandas.datetime class is deprecated and will be removed from pandas in a future version. "
    "Import from datetime module instead.",
)


def set_backends(engine=None, partition=None):
    """
    Method to set the _pair_ of execution engine and partition format simultaneously.
    This is needed because there might be cases where switching one by one would be
    impossible, as not all pairs of values are meaningful.

    The method returns pair of old values, so it is easy to return back.
    """
    from .config import Engine, Backend

    old_engine, old_partition = None, None
    # defer callbacks until both entities are set
    if engine is not None:
        old_engine = Engine._put_nocallback(engine)
    if partition is not None:
        old_partition = Backend._put_nocallback(partition)
    # execute callbacks if something was changed
    if old_engine is not None:
        Engine._check_callbacks(old_engine)
    if old_partition is not None:
        Backend._check_callbacks(old_partition)

    return old_engine, old_partition


### MERGE CONFLICT BEGIN
# import os, sys
# def get_execution_engine():
#     # In the future, when there are multiple engines and different ways of
#     # backing the DataFrame, there will have to be some changed logic here to
#     # decide these things. In the meantime, we will use the currently supported
#     # execution engine + backing (Pandas + Ray).
#     if "MODIN_ENGINE" in os.environ:
#         if os.environ["MODIN_ENGINE"].title() == "Mpi":
#             return "MPI"
#         # .title allows variants like ray, RAY, Ray
#         return os.environ["MODIN_ENGINE"].title()
#     else:
#         if "MODIN_DEBUG" in os.environ:
#             return "Python"
#         else:
#             if sys.platform != "win32":
#                 try:
#                     import ray

#                 except ImportError:
#                     pass
#                 else:
#                     if version.parse(ray.__version__) != version.parse("0.8.0"):
#                         raise ImportError(
#                             "Please `pip install modin[ray]` to install compatible Ray version."
#                         )
#                     return "Ray"
#             try:
#                 import dask
#                 import distributed

#             except ImportError:
#                 raise ImportError(
#                     "Please `pip install {}modin[dask]` to install an engine".format(
#                         "modin[ray]` or `" if sys.platform != "win32" else ""
#                     )
#                 )
#             else:
#                 if version.parse(dask.__version__) < version.parse(
#                     "2.1.0"
#                 ) or version.parse(distributed.__version__) < version.parse("2.3.2"):
#                     raise ImportError(
#                         "Please `pip install modin[dask]` to install compatible Dask version."
#                     )
#                 return "Dask"


# def get_partition_format():
#     # See note above about engine + backing.
#     return os.environ.get("MODIN_BACKEND", "Pandas").title()


# __version__ = "0.6.3"
# __execution_engine__ = get_execution_engine()
# __partition_format__ = get_partition_format()

# # We don't want these used outside of this file.
# del get_execution_engine
# del get_partition_format
### MERGE CONFLICT END

__version__ = get_versions()["version"]
del get_versions
