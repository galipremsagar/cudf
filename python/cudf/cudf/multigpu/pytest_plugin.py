# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""pytest plugin that brings the multi-GPU backend up before tests collect.

    pytest -p cudf.multigpu.pytest_plugin ...

Point of this: the backend does not only sit under cudf.pandas. Installing it
also replaces ``cudf.concat``, ``cudf.read_parquet`` and ``cudf.read_csv`` with
partitioning versions, so cuDF's own test suite is a fair question to ask of it
-- those entrypoints are what the classic tests call directly.

CUDF_MULTIGPU_PLUGIN_MODE selects how much is installed:

``entrypoints`` (default)
    Start the runtime and patch the cuDF entrypoints only. pandas is left
    alone, which matters because cuDF's tests use pandas as their reference
    oracle -- proxying it would replace the thing being compared against.

``full``
    Everything ``pandas_compat.install()`` does, including the cudf.pandas
    proxy layer.
"""

from __future__ import annotations

import os


def pytest_configure(config):
    mode = os.environ.get("CUDF_MULTIGPU_PLUGIN_MODE", "entrypoints")
    pool = float(os.environ.get("CUDF_MULTIGPU_PLUGIN_POOL", "0.10"))
    initial = float(os.environ.get("CUDF_MULTIGPU_PLUGIN_INITIAL", "0.05"))

    if mode == "full":
        from . import pandas_compat

        pandas_compat.install(
            max_pool_fraction=pool, initial_pool_fraction=initial
        )
        return

    from . import _runtime

    # The runtime alone: per-device pools and one pinned worker thread each.
    # Nothing in the cuDF namespace is replaced, which is the point -- the
    # classic suite calls cudf.read_parquet directly and must keep getting a
    # cudf.DataFrame.
    _runtime.init(max_pool_fraction=pool, initial_pool_fraction=initial)
