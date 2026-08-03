# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""Device-awareness for cuDF's process-wide caches.

Several cuDF caches hold values that are only valid on the device that created
them -- converted scalars (device memory) and compiled UDF kernels (which have
device pointers to libcudf's character tables baked into their PTX). Keyed only
on their logical inputs, those caches hand a GPU 0 value to a kernel launched on
GPU 3, which faults or reads foreign memory.

Including the current device in the key fixes that, but querying the device on
every lookup costs about as much as the cached work itself in the single-GPU
case. So the device is only consulted once more than one device is actually in
use; :func:`enable_multi_device_caches` flips that on and is called by
``cudf.multigpu``.
"""

from __future__ import annotations

__all__ = [
    "cache_device_key",
    "clear_device_caches",
    "enable_multi_device_caches",
    "multi_device_caches_enabled",
    "register_device_cache",
]

_multi_device = False

#: callables that drop every entry of a device-keyed cache
_registered_caches: list = []


def multi_device_caches_enabled() -> bool:
    return _multi_device


def cache_device_key() -> int:
    """The device component of a cache key.

    Always ``0`` in single-device processes, so the key shape does not change
    and no device query is performed on the hot path.
    """
    if not _multi_device:
        return 0
    from rmm._cuda.gpu import getDevice

    return getDevice()


def register_device_cache(clear: callable) -> None:
    """Register a cache-clearing callable, invoked by :func:`clear_device_caches`."""
    if clear not in _registered_caches:
        _registered_caches.append(clear)


def enable_multi_device_caches() -> None:
    """Make cuDF's device-sensitive caches key on the current device.

    Existing entries were keyed as though every device were device 0, so they
    are dropped first.
    """
    global _multi_device
    if _multi_device:
        return
    clear_device_caches()
    _multi_device = True


def clear_device_caches() -> None:
    """Drop every registered device-keyed cache.

    Must run before the memory resources that allocated the cached values are
    torn down, since some cached values (``plc.Scalar``) do not keep their
    memory resource alive.
    """
    for clear in _registered_caches:
        clear()
