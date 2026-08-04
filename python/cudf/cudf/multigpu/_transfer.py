# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""Moving cuDF objects between CUDA devices inside one process.

Transfer uses cuDF's own device serialization protocol
(:meth:`cudf.core.abc.Serializable.device_serialize`), which yields a purely
host-side ``header`` plus a flat list of device ``Buffer`` objects covering the
index, the data columns, null masks, and nested/dictionary children.  That
gives exact round-trip fidelity for every dtype cuDF supports -- strings,
categoricals, decimals, datetimes, lists, structs -- without reimplementing any
of it.

The device buffers are then copied into a *single* contiguous allocation on the
destination device, so a transfer costs one allocation and a batch of
asynchronous peer copies rather than one allocation per buffer.

Note on peer access: we deliberately never call ``cudaDeviceEnablePeerAccess``.
The driver's staged peer-copy path is correct and reaches full PCIe bandwidth,
whereas enabling direct peer mappings is silently wrong on some systems that
nevertheless advertise P2P support.
"""

from __future__ import annotations

from typing import Any, Iterable, Sequence

from cuda.bindings import runtime as cudart
from rmm.pylibrmm.device_buffer import DeviceBuffer

import cudf
from cudf.core.abc import Serializable

from ._runtime import DeviceRuntime, _chk, _current_device, get_runtime

__all__ = ["move", "move_batch", "broadcast", "gather_concat"]

_ALIGNMENT = 256


class _BufferView:
    """A window into a larger device allocation, exposed via CAI.

    Holds a reference to the base allocation so the window keeps it alive.
    The attribute is deliberately *not* named ``owner``: cuDF's
    ``get_buffer_owner`` walks an ``owner`` chain looking for a ``BufferOwner``
    and we want cuDF to treat this as fresh device memory.
    """

    __slots__ = ("_base", "__cuda_array_interface__")

    def __init__(self, base: DeviceBuffer, offset: int, size: int) -> None:
        self._base = base
        # A zero-length frame must present a *null* pointer.  cuDF emits such
        # frames for the data buffer of compound (list/struct) parent columns,
        # and libcudf rejects a compound column whose data pointer is non-null
        # ("Compound (parent) columns cannot have data").
        ptr = int(base.ptr) + offset if size else 0
        self.__cuda_array_interface__ = {
            "data": (ptr, False),
            "shape": (size,),
            "strides": None,
            "typestr": "|u1",
            "version": 3,
        }


def is_managed(ptr: int) -> bool:
    """Whether ``ptr`` is a cudaMallocManaged allocation."""
    err, attrs = cudart.cudaPointerGetAttributes(int(ptr))
    if err != cudart.cudaError_t.cudaSuccess:
        return False
    return attrs.type == cudart.cudaMemoryType.cudaMemoryTypeManaged


def copy_across_devices(dst_ptr: int, src_ptr: int, nbytes: int) -> None:
    """Copy between two device allocations that may live on different GPUs.

    Managed allocations are staged through host memory. A direct
    managed-to-managed copy is silently wrong on hardware where P2P is
    advertised but not functional -- it yields zeros -- and a managed pointer
    is not owned by a device, so the peer-copy API cannot express the intent
    either. Plain device allocations take the direct path, which is correct
    and reaches full PCIe bandwidth.
    """
    if not (is_managed(src_ptr) or is_managed(dst_ptr)):
        _chk(
            cudart.cudaMemcpyAsync(
                dst_ptr, src_ptr, nbytes,
                cudart.cudaMemcpyKind.cudaMemcpyDefault, 0,
            ),
            "cudaMemcpyAsync",
        )
        return
    import numpy as np

    staging = np.empty(nbytes, dtype=np.uint8)
    host = staging.ctypes.data
    _chk(
        cudart.cudaMemcpy(host, src_ptr, nbytes,
                          cudart.cudaMemcpyKind.cudaMemcpyDeviceToHost),
        "managed stage out",
    )
    _chk(
        cudart.cudaMemcpy(dst_ptr, host, nbytes,
                          cudart.cudaMemcpyKind.cudaMemcpyHostToDevice),
        "managed stage in",
    )


def _layout(sizes: Sequence[int]) -> tuple[list[int], int]:
    """Offsets for packing ``sizes`` back-to-back with alignment."""
    offsets = []
    total = 0
    for n in sizes:
        offsets.append(total)
        total += (n + _ALIGNMENT - 1) // _ALIGNMENT * _ALIGNMENT
    return offsets, total


# ----------------------------------------------------------------------
# source side
# ----------------------------------------------------------------------
def _extract(obj: Serializable) -> tuple[dict, list, list, Any]:
    """Runs on the *source* device thread.

    Returns ``(header, specs, host_frames, keepalive)`` where ``specs`` is a
    list of ``(ptr, nbytes)`` for device frames (``None`` for host frames).
    ``keepalive`` must outlive the copy.
    """
    header, frames = obj.device_serialize()
    is_cuda = header["is-cuda"]
    specs: list[tuple[int, int] | None] = []
    host_frames: list[Any] = []
    for frame, cuda in zip(frames, is_cuda, strict=True):
        if cuda:
            specs.append((int(frame.ptr), int(frame.nbytes)))
            host_frames.append(None)
        else:
            specs.append(None)
            host_frames.append(memoryview(frame))
    # The producing work may still be in flight on this device's stream.
    _chk(cudart.cudaDeviceSynchronize(), "source sync before transfer")
    return header, specs, host_frames, frames


# ----------------------------------------------------------------------
# destination side
# ----------------------------------------------------------------------
def _receive(
    header: dict,
    specs: Sequence[tuple[int, int] | None],
    host_frames: Sequence[Any],
    src_device: int,
) -> Serializable:
    """Runs on the *destination* device thread."""
    dst_device = _current_device()
    device_sizes = [s[1] if s is not None else 0 for s in specs]
    offsets, total = _layout(device_sizes)

    base = DeviceBuffer(size=max(total, 1))
    base_ptr = int(base.ptr)
    for spec, offset in zip(specs, offsets, strict=True):
        if spec is None:
            continue
        src_ptr, nbytes = spec
        if nbytes == 0:
            continue
        copy_across_devices(base_ptr + offset, src_ptr, nbytes)
    _chk(cudart.cudaStreamSynchronize(0), "transfer stream sync")

    new_frames: list[Any] = []
    for spec, offset, host in zip(specs, offsets, host_frames, strict=True):
        if spec is None:
            new_frames.append(host)
        else:
            new_frames.append(_BufferView(base, offset, spec[1]))
    return Serializable.device_deserialize(header, new_frames)


# ----------------------------------------------------------------------
# public API
# ----------------------------------------------------------------------
def move(
    obj: Serializable,
    dst_device: int,
    src_device: int,
    runtime: DeviceRuntime | None = None,
) -> Serializable:
    """Copy ``obj`` from ``src_device`` to ``dst_device``.

    Returns a new object whose memory lives entirely on ``dst_device``.  A
    same-device request is a no-op and returns ``obj`` unchanged.
    """
    if src_device == dst_device:
        return obj
    runtime = runtime or get_runtime()
    header, specs, host_frames, keepalive = runtime.run(src_device, _extract, obj)
    try:
        return runtime.run(
            dst_device, _receive, header, specs, host_frames, src_device
        )
    finally:
        # Free the source-side serialization view on its own device.
        runtime.run(src_device, _release, keepalive)


def _release(keepalive: Any) -> None:
    del keepalive


def move_batch(
    items: Sequence[tuple[Serializable, int, int]],
    runtime: DeviceRuntime | None = None,
) -> list[Serializable]:
    """Move many objects at once as ``(obj, src_device, dst_device)``.

    Extraction happens concurrently across source devices and reception
    concurrently across destination devices, so an all-to-all shuffle keeps
    every PCIe link busy instead of serializing pair by pair.
    """
    runtime = runtime or get_runtime()
    results: list[Any] = [None] * len(items)

    # Phase 1: serialize on each source device (parallel across devices).
    local: list[tuple[int, Serializable]] = []
    remote: list[int] = []
    jobs = []
    for i, (obj, src, dst) in enumerate(items):
        if src == dst:
            local.append((i, obj))
        else:
            remote.append(i)
            jobs.append((src, _extract, (obj,), {}))
    for i, obj in local:
        results[i] = obj
    if not remote:
        return results
    extracted = runtime.run_many(jobs)

    # Phase 2: receive on each destination device (parallel across devices).
    recv_jobs = []
    for i, (header, specs, host_frames, _keep) in zip(
        remote, extracted, strict=True
    ):
        src = items[i][1]
        dst = items[i][2]
        recv_jobs.append((dst, _receive, (header, specs, host_frames, src), {}))
    received = runtime.run_many(recv_jobs)
    for i, obj in zip(remote, received, strict=True):
        results[i] = obj

    # Phase 3: release the source-side views on their own devices.
    release_jobs = [
        (items[i][1], _release, (extracted[k][3],), {})
        for k, i in enumerate(remote)
    ]
    runtime.run_many(release_jobs)
    return results


def broadcast(
    obj: Serializable,
    src_device: int,
    dst_devices: Iterable[int],
    runtime: DeviceRuntime | None = None,
) -> dict[int, Serializable]:
    """Replicate ``obj`` from ``src_device`` onto every device in ``dst_devices``.

    Used for broadcast joins, where one side is small enough to fit everywhere.
    """
    runtime = runtime or get_runtime()
    dst_devices = list(dst_devices)
    header, specs, host_frames, keepalive = runtime.run(
        src_device, _extract, obj
    )
    try:
        jobs = [
            (d, _receive, (header, specs, host_frames, src_device), {})
            for d in dst_devices
            if d != src_device
        ]
        out = {d: obj for d in dst_devices if d == src_device}
        received = runtime.run_many(jobs)
        for (d, *_), value in zip(jobs, received, strict=True):
            out[d] = value
        return out
    finally:
        runtime.run(src_device, _release, keepalive)


def gather_concat(
    chunks: Sequence[tuple[Serializable, int]],
    dst_device: int,
    runtime: DeviceRuntime | None = None,
    ignore_index: bool = False,
) -> Any:
    """Move every chunk to ``dst_device`` and concatenate into one object.

    This is the "collapse to a single GPU" path; the result must of course fit
    in one device's memory.
    """
    runtime = runtime or get_runtime()
    if not chunks:
        raise ValueError("no chunks to gather")
    moved = move_batch(
        [(obj, src, dst_device) for obj, src in chunks], runtime=runtime
    )
    if len(moved) == 1:
        return runtime.run(dst_device, _identity_copy, moved[0])
    return runtime.run(dst_device, _concat, moved, ignore_index)


def _identity_copy(obj: Any) -> Any:
    return obj


def _concat(objs: Sequence[Any], ignore_index: bool) -> Any:
    return cudf.concat(list(objs), ignore_index=ignore_index)
