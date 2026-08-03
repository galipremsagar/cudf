# SPDX-FileCopyrightText: Copyright (c) 2020-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import functools
from typing import TYPE_CHECKING

import pylibcudf as plc

from cudf.utils.device import cache_device_key, register_device_cache

if TYPE_CHECKING:
    import pyarrow as pa


# A ``plc.Scalar`` holds *device* memory, so a cache of them is only valid for
# the device the memory was allocated on. Handing a scalar allocated on GPU 0
# to a kernel launched on GPU 3 is an illegal access, not a slow path.
@functools.lru_cache(maxsize=128)
def _pa_scalar_to_plc_scalar(pa_scalar: pa.Scalar, device: int) -> plc.Scalar:
    return plc.Scalar.from_arrow(pa_scalar)


def pa_scalar_to_plc_scalar(pa_scalar: pa.Scalar) -> plc.Scalar:
    """
    Cached conversion from a pyarrow.Scalar to pylibcudf.Scalar.

    The cache is keyed on the current CUDA device as well as the value, since
    the returned scalar owns device memory.

    Parameters
    ----------
    pa_scalar: pa.Scalar

    Returns
    -------
    pylibcudf.Scalar
        pylibcudf.Scalar to use in pylibcudf APIs
    """
    return _pa_scalar_to_plc_scalar(pa_scalar, cache_device_key())


def clear_scalar_cache() -> None:
    """Drop all cached device scalars."""
    _pa_scalar_to_plc_scalar.cache_clear()


register_device_cache(clear_scalar_cache)
