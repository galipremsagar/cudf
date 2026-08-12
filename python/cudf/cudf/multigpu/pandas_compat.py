# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""Run ``cudf.pandas`` with the multi-GPU frame as the accelerated backend.

``cudf.pandas`` proxies every pandas object over a *fast* implementation
(normally ``cudf``) with pandas itself as the slow fallback.  This module
swaps the fast implementation for the chunked multi-GPU frame, so unmodified
pandas code operates on data spread across every GPU::

    import cudf.multigpu.pandas_compat as mgpandas
    mgpandas.install()

    import pandas as pd          # now the accelerated pandas
    df = pd.read_parquet(...)    # chunks land on all GPUs
    df.groupby("k").sum()        # shuffle + local aggregate

This is experimental.  Two properties of the proxy machinery matter:

* Attributes are resolved on the fast **class**, not the instance
  (``getattr(owner._fsproxy_fast, name)`` in ``fast_slow_proxy.py``), which is
  why the chunked types carry a metaclass-level dispatch hook.
* Fallback to pandas is silent and moves the whole frame to host memory.  On a
  frame that only fits in aggregate GPU memory that is fatal rather than slow,
  so :func:`install` warns and ``CUDF_PANDAS_FAIL_ON_FALLBACK=1`` is strongly
  recommended while evaluating coverage.
"""

from __future__ import annotations

import importlib
import os
import warnings
from typing import Any, Sequence

import pandas as pd

import cudf

from ._creation import from_pandas
from ._frame import ChunkedDataFrame, ChunkedIndex, ChunkedSeries
from ._runtime import get_runtime, init

__all__ = ["install", "is_installed"]

_installed = False


def is_installed() -> bool:
    return _installed


def _to_slow(obj):
    """Multi-GPU frame -> pandas (moves every chunk to host)."""
    return obj.to_pandas()


def _make_to_fast(npartitions: int | None):
    def _to_fast(obj):
        """pandas -> multi-GPU frame, uploading each slice to its own GPU."""
        return from_pandas(obj, npartitions=npartitions)

    return _to_fast


def install(
    devices: Sequence[int] | None = None,
    npartitions: int | None = None,
    fail_on_fallback: bool | None = None,
    **runtime_kwargs: Any,
) -> None:
    """Install the accelerated-pandas hook with the multi-GPU backend.

    Must run before anything imports ``pandas`` or
    ``cudf.pandas._wrappers.pandas``: the wrapper module binds
    ``make_final_proxy_type`` by value at import time, so a later patch is
    invisible.

    Parameters
    ----------
    devices
        GPUs to use.  Defaults to all visible devices.
    npartitions
        Chunks to split a host frame into.  Defaults to one per GPU.
    fail_on_fallback
        Sets ``CUDF_PANDAS_FAIL_ON_FALLBACK``.  Leave it on while checking
        which operations are covered; a fallback silently pulls the entire
        frame to host memory.
    """
    global _installed
    if _installed:
        return

    if "pandas" in _imported_wrappers():
        raise RuntimeError(
            "cudf.pandas wrappers are already imported; call "
            "cudf.multigpu.pandas_compat.install() before importing pandas "
            "or cudf.pandas"
        )

    # cudf.pandas otherwise sizes an RMM pool for the *current* device only,
    # which is wrong when we intend to use all of them.  We install our own
    # per-device pools instead.
    os.environ.setdefault("RAPIDS_NO_INITIALIZE", "1")
    if fail_on_fallback:
        os.environ["CUDF_PANDAS_FAIL_ON_FALLBACK"] = "1"

    init(devices=devices, **runtime_kwargs)

    # Under accelerated pandas these objects must be indistinguishable from
    # pandas frames, so drop the chunk-placement banner from repr().
    from . import _frame

    _frame.SHOW_PLACEMENT_IN_REPR = False
    _frame.DEFAULT_NPARTITIONS = npartitions

    from cudf.pandas import fast_slow_proxy as fsp

    original_final = fsp.make_final_proxy_type
    to_fast = _make_to_fast(npartitions)

    replacements = {
        cudf.DataFrame: ChunkedDataFrame,
        cudf.Series: ChunkedSeries,
        cudf.Index: ChunkedIndex,
    }

    def patched_final_proxy_type(name, fast_type, slow_type, **kwargs):
        # cudf.pandas names the proxy's module by walking the stack, and this
        # wrapper adds a frame -- so without this every proxy type would be
        # attributed to cudf.multigpu.pandas_compat, where pickle cannot find
        # it ("Can't pickle <class '...pandas_compat.ndarray'>").
        kwargs.setdefault("module", _real_caller_module())
        if fast_type in replacements:
            kwargs["fast_to_slow"] = _to_slow
            kwargs["slow_to_fast"] = to_fast
            fast_type = replacements[fast_type]
            # "GPU object" means a cuDF object in cuDF's own API -- it is what
            # cudf.DataFrame(proxy) consumes. Handing back a chunked frame
            # makes that constructor fail with "data must be list or
            # dict-like", so this collapses onto one device. The result must
            # therefore fit there, which is exactly what the caller asked for
            # by requesting a single cuDF object.
            extra = dict(kwargs.get("additional_attributes") or {})
            extra.setdefault("as_gpu_object", _as_gpu_object)
            kwargs["additional_attributes"] = extra
        return original_final(name, fast_type, slow_type, **kwargs)

    fsp.make_final_proxy_type = patched_final_proxy_type

    # Intermediates (group-by objects and friends) need the same treatment.
    # ``df.groupby(...)`` returns one of these; if the chunked group-by is not
    # registered, the proxy hands the raw object back and every subsequent
    # call escapes the proxy entirely.
    original_intermediate = fsp.make_intermediate_proxy_type
    intermediate_replacements = _intermediate_replacements()

    def patched_intermediate_proxy_type(name, fast_type, slow_type, **kwargs):
        kwargs.setdefault("module", _real_caller_module())
        fast_type = intermediate_replacements.get(fast_type, fast_type)
        return original_intermediate(name, fast_type, slow_type, **kwargs)

    fsp.make_intermediate_proxy_type = patched_intermediate_proxy_type

    # Bind as a distinct name: `import cudf.pandas` would rebind the local
    # `cudf` and shadow the module-level import used just above.
    from cudf import pandas as cudf_pandas

    _patch_cudf_entrypoints(npartitions)
    cudf_pandas.install()
    _installed = True

    warnings.warn(
        "cudf.multigpu's cudf.pandas backend is experimental. Operations "
        "without a multi-GPU implementation fall back to pandas, which copies "
        "the whole frame to host memory -- set "
        "CUDF_PANDAS_FAIL_ON_FALLBACK=1 to turn that into an error while you "
        "check coverage.",
        UserWarning,
        stacklevel=2,
    )


def _as_gpu_object(self):
    """The single-GPU cuDF object behind this proxy."""
    from ._frame import ChunkedFrame

    fast = self._fsproxy_slow_to_fast()
    return fast.compute() if isinstance(fast, ChunkedFrame) else fast


def _real_caller_module() -> str:
    """The module that asked for a proxy type, skipping this module's frames.

    ``cudf.pandas`` derives a proxy type's ``__module__`` from the call stack.
    Wrapping its factories shifts that stack, so the real caller has to be
    found explicitly.
    """
    import inspect

    frame = inspect.currentframe()
    while frame is not None:
        name = frame.f_globals.get("__name__", "")
        if name != __name__:
            return name
        frame = frame.f_back
    return __name__


def wrap_fast(obj):
    """Wrap a chunked frame in the pandas proxy that fronts it."""
    from cudf.pandas import fast_slow_proxy as fsp

    proxy_type = fsp.get_final_type_map().get(type(obj))
    if proxy_type is None:
        return obj
    return proxy_type._fsproxy_wrap(obj, None)


def _patch_cudf_entrypoints(npartitions: int | None) -> None:
    """Make the multi-GPU readers and concat the *fast* implementations.

    An earlier version replaced ``pd.read_parquet`` / ``pd.concat`` outright.
    That works but breaks the proxy: ``xpd.concat`` is a function proxy whose
    ``_fsproxy_fast`` is ``cudf.concat``, so overwriting the pandas name left a
    bare function with no fast side and broke ``xpd.concat is
    xpd.core.reshape.concat.concat``. Patching the cuDF side instead leaves the
    proxy machinery intact and still routes the work through the GPUs.

    Must run before ``cudf.pandas.install()``: the proxies capture these
    functions by value.
    """
    from . import _io
    from ._creation import concat as chunked_concat
    from ._frame import ChunkedFrame

    #: keywords the partitioned readers do not reproduce. chunksize/iterator
    #: return an iterator rather than a frame; nrows and skipfooter are defined
    #: against the whole file, which a byte-range split does not preserve.
    csv_unsupported = frozenset({"chunksize", "iterator", "skipfooter", "nrows"})

    def _addressable(path) -> bool:
        if not isinstance(path, (str, os.PathLike)):
            return False  # StringIO, buffers, file objects
        text = os.fspath(path)
        return not any(ch in text for ch in "*?[") and os.path.exists(text)

    original_read_parquet = cudf.read_parquet
    original_read_csv = cudf.read_csv
    original_concat = cudf.concat

    def read_parquet(path, columns=None, **kwargs):
        kwargs.pop("engine", None)
        if not (_addressable(path) or isinstance(path, list)):
            return original_read_parquet(path, columns=columns, **kwargs)
        try:
            return _io.read_parquet(
                path, columns=columns, npartitions=npartitions, **kwargs
            )
        except (NotImplementedError, TypeError, ValueError):
            return original_read_parquet(path, columns=columns, **kwargs)

    def read_csv(path, **kwargs):
        if not _addressable(path) or (csv_unsupported & kwargs.keys()):
            return original_read_csv(path, **kwargs)
        try:
            return _io.read_csv(path, npartitions=npartitions, **kwargs)
        except (NotImplementedError, TypeError, ValueError):
            return original_read_csv(path, **kwargs)

    def concat(objs, *args, **kwargs):
        """Concatenate chunked frames without moving any chunk."""
        items = list(objs)
        if items and all(isinstance(o, ChunkedFrame) for o in items):
            axis = kwargs.get("axis", args[0] if args else 0)
            if axis in (0, "index"):
                return chunked_concat(items, **kwargs)
        return original_concat(objs, *args, **kwargs)

    for name, fn, original in (
        ("read_parquet", read_parquet, original_read_parquet),
        ("read_csv", read_csv, original_read_csv),
        ("concat", concat, original_concat),
    ):
        fn.__name__ = name
        fn.__doc__ = original.__doc__
        setattr(cudf, name, fn)


def _unwrap(obj):
    from ._frame import unwrap_proxy

    return unwrap_proxy(obj)


def _intermediate_replacements() -> dict:
    """Map cuDF intermediate types onto their chunked equivalents.

    The ``.str`` / ``.dt`` / ``.cat`` accessors belong here as much as group-by
    does. Without them the proxy resolves ``s.dt`` on the fast object, gets a
    bare ``_Accessor`` it does not recognise, and hands back whatever the
    accessor returns unwrapped -- so ``s.dt.tz_localize(...)`` yields a raw
    ChunkedSeries where pandas code expects a Series, and every subsequent
    operation escapes the proxy.
    """
    from . import _frame
    from ._ops import ChunkedGroupBy

    groupby = cudf.core.groupby.groupby
    accessors = {}
    # The .iloc/.loc indexers are intermediates too: without them the proxy
    # returns a bare indexer, and df.iloc[:, :] hands back a raw
    # ChunkedDataFrame where pandas code expects a DataFrame.
    for module, attr, chunked in (
        ("cudf.core.dataframe", "_DataFrameIlocIndexer",
         "_DataFrameILocIndexer"),
        ("cudf.core.dataframe", "_DataFrameLocIndexer", "_DataFrameLocIndexer"),
        ("cudf.core.series", "_SeriesIlocIndexer", "_SeriesILocIndexer"),
        ("cudf.core.series", "_SeriesLocIndexer", "_SeriesLocIndexer"),
        ("cudf.core.series", "DatetimeProperties", "_DatetimeAccessor"),
        ("cudf.core.series", "TimedeltaProperties", "_TimedeltaAccessor"),
        ("cudf.core.accessors.string", "StringMethods", "_StringAccessor"),
        ("cudf.core.accessors.categorical", "CategoricalAccessor",
         "_CategoricalAccessor"),
        ("cudf.core.accessors.lists", "ListMethods", "_ListAccessor"),
        ("cudf.core.accessors.struct", "StructMethods", "_StructAccessor"),
    ):
        try:
            mod = importlib.import_module(module)
        except ImportError:
            continue
        cls = getattr(mod, attr, None)
        if cls is not None:
            accessors[cls] = getattr(_frame, chunked)
    return {
        **accessors,
        groupby.DataFrameGroupBy: ChunkedGroupBy,
        groupby.SeriesGroupBy: ChunkedGroupBy,
    }


def _imported_wrappers() -> set[str]:
    import sys

    return {
        module.rsplit(".", 1)[-1]
        for module in sys.modules
        if module.startswith("cudf.pandas._wrappers")
    }
