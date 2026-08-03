# SPDX-FileCopyrightText: Copyright (c) 2020-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import functools
from typing import TYPE_CHECKING

import pylibcudf as plc

if TYPE_CHECKING:
    import pyarrow as pa


# A ``plc.Scalar`` holds *device* memory, so a cache of them is only valid for
# the device the memory was allocated on.  Handing a scalar allocated on GPU 0
# to a kernel launched on GPU 3 is an illegal access, not a slow path.
#
# Single-GPU processes (the overwhelmingly common case) must not pay for a
# device query on every scalar conversion, so the device is only consulted once
# more than one device is actually in use.  ``cudf.multigpu`` flips this via
# :func:`enable_multi_device_scalar_cache`.
_multi_device = False


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
    if _multi_device:
        from rmm._cuda.gpu import getDevice

        return _pa_scalar_to_plc_scalar(pa_scalar, getDevice())
    return _pa_scalar_to_plc_scalar(pa_scalar, 0)


def enable_multi_device_scalar_cache() -> None:
    """Make the scalar cache device-aware (used by ``cudf.multigpu``).

    Clears any entries cached under the single-device fast path first, since
    those were keyed as if every device were device 0.
    """
    global _multi_device
    if not _multi_device:
        clear_scalar_cache()
        _multi_device = True


def clear_scalar_cache() -> None:
    """Drop all cached device scalars.

    Must be called before the memory resources that allocated them are torn
    down, because ``plc.Scalar`` does not keep its memory resource alive.
    """
    _pa_scalar_to_plc_scalar.cache_clear()
