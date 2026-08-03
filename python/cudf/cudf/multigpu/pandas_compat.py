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

    from cudf.pandas import fast_slow_proxy as fsp

    original_final = fsp.make_final_proxy_type
    to_fast = _make_to_fast(npartitions)

    replacements = {
        cudf.DataFrame: ChunkedDataFrame,
        cudf.Series: ChunkedSeries,
        cudf.Index: ChunkedIndex,
    }

    def patched_final_proxy_type(name, fast_type, slow_type, **kwargs):
        if fast_type in replacements:
            kwargs["fast_to_slow"] = _to_slow
            kwargs["slow_to_fast"] = to_fast
            fast_type = replacements[fast_type]
        return original_final(name, fast_type, slow_type, **kwargs)

    fsp.make_final_proxy_type = patched_final_proxy_type

    # Intermediates (group-by objects and friends) need the same treatment.
    # ``df.groupby(...)`` returns one of these; if the chunked group-by is not
    # registered, the proxy hands the raw object back and every subsequent
    # call escapes the proxy entirely.
    original_intermediate = fsp.make_intermediate_proxy_type
    intermediate_replacements = _intermediate_replacements()

    def patched_intermediate_proxy_type(name, fast_type, slow_type, **kwargs):
        fast_type = intermediate_replacements.get(fast_type, fast_type)
        return original_intermediate(name, fast_type, slow_type, **kwargs)

    fsp.make_intermediate_proxy_type = patched_intermediate_proxy_type

    # Bind as a distinct name: `import cudf.pandas` would rebind the local
    # `cudf` and shadow the module-level import used just above.
    from cudf import pandas as cudf_pandas

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


def _intermediate_replacements() -> dict:
    """Map cuDF intermediate types onto their chunked equivalents."""
    from ._ops import ChunkedGroupBy

    groupby = cudf.core.groupby.groupby
    return {
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
