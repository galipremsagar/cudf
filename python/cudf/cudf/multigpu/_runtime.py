# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""Device pool + per-device execution for the multi-GPU chunked cuDF layer.

The runtime owns one worker thread per CUDA device.  Each worker thread has
``cudaSetDevice`` pinned to its device for its entire lifetime, so any cuDF /
libcudf call issued from that thread allocates from that device's RMM memory
resource and runs on that device.  Because pylibcudf releases the GIL around
libcudf calls, work submitted to different devices genuinely overlaps.

Nothing here requires changes to libcudf.
"""

from __future__ import annotations

import os
import threading
import warnings
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Callable, Iterable, Sequence

import rmm
from cuda.bindings import runtime as cudart

__all__ = [
    "DeviceRuntime",
    "get_runtime",
    "init",
    "shutdown",
    "is_initialized",
]


def _chk(err: Any, what: str = "") -> Any:
    """Unwrap and check a cuda-python runtime API return value."""
    rest: tuple = ()
    if isinstance(err, tuple):
        err, *rest_list = err
        rest = tuple(rest_list)
    if err != cudart.cudaError_t.cudaSuccess:
        name = cudart.cudaGetErrorString(err)[1]
        raise RuntimeError(f"CUDA error in {what or 'call'}: {err} ({name})")
    if len(rest) == 1:
        return rest[0]
    return rest if rest else None


def visible_devices() -> list[int]:
    """Device ordinals visible to this process."""
    count = _chk(cudart.cudaGetDeviceCount(), "cudaGetDeviceCount")
    return list(range(count))


def device_memory(device: int) -> tuple[int, int]:
    """``(free, total)`` bytes on ``device``."""
    prev = _chk(cudart.cudaGetDevice(), "cudaGetDevice")
    _chk(cudart.cudaSetDevice(device), "cudaSetDevice")
    free, total = _chk(cudart.cudaMemGetInfo(), "cudaMemGetInfo")
    _chk(cudart.cudaSetDevice(prev), "cudaSetDevice")
    return free, total


class DeviceRuntime:
    """A pool of CUDA devices, each driven by a dedicated pinned worker thread.

    Parameters
    ----------
    devices
        Device ordinals to use.  Defaults to every visible device.
    pool_allocator
        Whether to install an RMM pool memory resource per device.  Pool
        allocation matters a lot here: a chunked workload does many
        allocations/frees per device and ``cudaMalloc`` synchronizes.
    initial_pool_fraction, max_pool_fraction
        Fractions of each device's *free* memory used to size the RMM pool.
    """

    def __init__(
        self,
        devices: Sequence[int] | None = None,
        *,
        pool_allocator: bool = True,
        initial_pool_fraction: float = 0.05,
        max_pool_fraction: float = 0.90,
        validate_transfers: bool = True,
        memory_resource: str = "pool",
    ) -> None:
        if devices is None:
            devices = visible_devices()
        devices = [int(d) for d in devices]
        if not devices:
            raise ValueError("No CUDA devices available")
        self.devices: tuple[int, ...] = tuple(devices)
        self._mrs: dict[int, Any] = {}
        self._executors: dict[int, ThreadPoolExecutor] = {}
        #: worker thread ident -> the device it is pinned to
        self._worker_devices: dict[int, int] = {}
        self._closed = False
        self._lock = threading.Lock()

        # --- 0. Make cuDF's process-wide caches device-aware.
        # cuDF caches values that only work on the device that made them:
        # converted pyarrow scalars (device memory) and compiled UDF kernels
        # (PTX with libcudf's character-table device pointers baked in).
        # Without this, `df + 1` or a string UDF evaluated on GPU 3 can reuse
        # something allocated on GPU 0 and fault.
        from cudf.utils.device import enable_multi_device_caches

        enable_multi_device_caches()
        _check_unsupported_features()

        # --- 1. Install a memory resource per device.
        # Must be created while the target device is current, since a pool
        # resource reserves its initial pool up-front on the current device.
        if pool_allocator:
            prev = _chk(cudart.cudaGetDevice(), "cudaGetDevice")
            try:
                for d in self.devices:
                    _chk(cudart.cudaSetDevice(d), "cudaSetDevice")
                    free, _total = _chk(
                        cudart.cudaMemGetInfo(), "cudaMemGetInfo"
                    )
                    initial = _align_down(int(free * initial_pool_fraction))
                    maximum = _align_down(int(free * max_pool_fraction))
                    if memory_resource == "managed":
                        # cudaMallocManaged: allocations may exceed device
                        # memory and pages migrate from host on demand, so the
                        # working set is bounded by host RAM rather than by the
                        # GPU. Much slower when it actually migrates -- this
                        # buys the ability to run at all, not speed.
                        #
                        # Deliberately *not* wrapped in a pool. A pool suballocates
                        # from large contiguous blocks and grows by doubling, so it
                        # asks the driver for one enormous managed region rather
                        # than many ordinary ones. Managed memory oversubscribes
                        # happily -- 400 GiB on a 95 GiB device here -- but that
                        # single doubling request is what fails, and it fails as a
                        # sticky CUDA error rather than a clean bad_alloc. Paying
                        # cudaMallocManaged per allocation is the cost of having
                        # host RAM actually be reachable.
                        mr = rmm.mr.ManagedMemoryResource()
                        try:
                            mr = rmm.mr.PrefetchResourceAdaptor(mr)
                        except Exception:
                            pass  # prefetch is an optimization, not required
                    elif memory_resource == "async":
                        # cudaMallocAsync returns memory to the driver once a
                        # pool exceeds its release threshold. A classic RMM
                        # pool never does, so a long sequence of queries keeps
                        # every peak it ever reached and eventually cannot
                        # satisfy a large request even though little is live.
                        mr = rmm.mr.CudaAsyncMemoryResource(
                            initial_pool_size=initial,
                            release_threshold=initial,
                        )
                    elif memory_resource == "pool":
                        mr = rmm.mr.PoolMemoryResource(
                            rmm.mr.CudaMemoryResource(),
                            initial_pool_size=initial,
                            maximum_pool_size=maximum,
                        )
                    else:
                        raise ValueError(
                            f"unknown memory_resource {memory_resource!r}; "
                            "use 'pool', 'async' or 'managed'"
                        )
                    rmm.mr.set_per_device_resource(d, mr)
                    self._mrs[d] = mr
            finally:
                _chk(cudart.cudaSetDevice(prev), "cudaSetDevice")

        # --- 2. One pinned worker thread per device.
        for d in self.devices:
            self._executors[d] = ThreadPoolExecutor(
                max_workers=1,
                thread_name_prefix=f"cudf-mgpu-{d}",
                initializer=_pin_thread_to_device,
                initargs=(d,),
            )
        # Force each worker to start and pin itself now, so later failures are
        # not attributed to unrelated work, and record which thread is which so
        # re-entrant calls can be detected.
        for d in self.devices:
            ident = self._executors[d].submit(threading.get_ident).result()
            self._worker_devices[ident] = d

        # --- 3. Verify that cross-device copies actually work.
        # On some multi-GPU boxes ``cudaDeviceCanAccessPeer`` reports true and
        # ``cudaDeviceEnablePeerAccess`` succeeds, yet peer copies silently
        # transfer zeros.  We never enable peer access (the driver's staged
        # path is correct and fast), but we still verify before trusting it.
        if validate_transfers and len(self.devices) > 1:
            self.validate_peer_copies()

    # ------------------------------------------------------------------
    # execution
    # ------------------------------------------------------------------
    @property
    def n_devices(self) -> int:
        return len(self.devices)

    def submit(self, device: int, fn: Callable, *args, **kwargs) -> Future:
        """Submit ``fn`` to run on ``device``'s worker thread.

        A call made *from* a worker thread that targets that same worker's
        device runs inline. Each device has exactly one worker, so queueing
        such a call would wait for a thread that is already busy waiting --
        i.e. deadlock. This happens for real: cuDF's ``as_column`` calls
        ``getattr`` on the values it is given, which can re-enter this layer
        from inside a chunk operation.
        """
        if self._closed:
            raise RuntimeError("DeviceRuntime has been shut down")
        if self._worker_devices.get(threading.get_ident()) == device:
            future: Future = Future()
            try:
                future.set_result(fn(*args, **kwargs))
            except BaseException as exc:  # noqa: BLE001 - propagated via future
                future.set_exception(exc)
            return future
        return self._executors[device].submit(fn, *args, **kwargs)

    def current_worker_device(self) -> int | None:
        """The device this thread is pinned to, or ``None`` if not a worker."""
        return self._worker_devices.get(threading.get_ident())

    def run(self, device: int, fn: Callable, *args, **kwargs) -> Any:
        """Run ``fn`` on ``device`` and return its result."""
        return self.submit(device, fn, *args, **kwargs).result()

    def run_many(
        self, jobs: Iterable[tuple[int, Callable, tuple, dict]]
    ) -> list[Any]:
        """Run ``(device, fn, args, kwargs)`` jobs concurrently, in order.

        All jobs are submitted before any result is awaited, so work on
        distinct devices overlaps.
        """
        futures = [
            self.submit(device, fn, *args, **kwargs)
            for device, fn, args, kwargs in jobs
        ]
        return [f.result() for f in futures]

    def map(self, fn: Callable, items: Sequence[tuple[int, Any]]) -> list[Any]:
        """Apply ``fn(item)`` for each ``(device, item)`` on that device."""
        return self.run_many(
            [(device, fn, (item,), {}) for device, item in items]
        )

    def sync(self, device: int) -> None:
        """Block until all work on ``device`` has completed."""
        self.run(device, _device_synchronize)

    def sync_all(self) -> None:
        self.run_many(
            [(d, _device_synchronize, (), {}) for d in self.devices]
        )

    # ------------------------------------------------------------------
    # diagnostics
    # ------------------------------------------------------------------
    def validate_peer_copies(self) -> None:
        """Assert that device-to-device copies move real bytes.

        Raises if any ordered device pair fails, since silent corruption here
        would produce wrong answers rather than an error.
        """
        import numpy as np
        from rmm.pylibrmm.device_buffer import DeviceBuffer

        n = 4096
        pattern = np.arange(n, dtype=np.uint8)
        expected = int(pattern.astype(np.int64).sum())
        src_dev = self.devices[0]

        def _make_src():
            buf = DeviceBuffer(size=n)
            _chk(
                cudart.cudaMemcpy(
                    buf.ptr,
                    pattern.ctypes.data,
                    n,
                    cudart.cudaMemcpyKind.cudaMemcpyHostToDevice,
                ),
                "validate H2D",
            )
            _chk(cudart.cudaDeviceSynchronize(), "validate sync")
            return buf

        src = self.run(src_dev, _make_src)

        def _recv(_ignored):
            dst = DeviceBuffer(size=n)
            _chk(cudart.cudaMemset(dst.ptr, 0, n), "validate memset")
            from ._transfer import copy_across_devices

            copy_across_devices(dst.ptr, src.ptr, n)
            _chk(cudart.cudaDeviceSynchronize(), "validate sync")
            out = np.zeros(n, dtype=np.uint8)
            _chk(
                cudart.cudaMemcpy(
                    out.ctypes.data,
                    dst.ptr,
                    n,
                    cudart.cudaMemcpyKind.cudaMemcpyDeviceToHost,
                ),
                "validate D2H",
            )
            return int(out.astype(np.int64).sum())

        bad = []
        for d in self.devices:
            if d == src_dev:
                continue
            if self.run(d, _recv, None) != expected:
                bad.append(d)
        del src
        if bad:
            raise RuntimeError(
                f"Cross-device copies from GPU {src_dev} to GPU(s) {bad} "
                "returned corrupt data. This usually means CUDA peer access "
                "was enabled by another library on a system where P2P is "
                "advertised but not functional. cudf.multigpu deliberately "
                "does not enable peer access; something else in this process "
                "did."
            )

    def memory_info(self) -> dict[int, tuple[int, int]]:
        """``{device: (free, total)}`` in bytes."""
        return {d: device_memory(d) for d in self.devices}

    # ------------------------------------------------------------------
    def shutdown(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            # Cached device values must die before their memory resources do:
            # ``plc.Scalar`` does not keep its MR alive, so freeing them after
            # the pools are gone segfaults at interpreter exit.
            from cudf.utils.device import clear_device_caches

            clear_device_caches()
            for ex in self._executors.values():
                ex.shutdown(wait=True)
            self._executors.clear()

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        gib = 1 << 30
        parts = []
        for d, (free, total) in self.memory_info().items():
            parts.append(f"GPU{d}: {(total - free) / gib:.1f}/{total / gib:.1f}GiB")
        return f"<DeviceRuntime {self.n_devices} devices | " + ", ".join(parts) + ">"


def _align_down(nbytes: int, alignment: int = 256) -> int:
    return (nbytes // alignment) * alignment


def _check_unsupported_features() -> None:
    """Warn about cuDF settings that are not device-aware."""
    try:
        from cudf.core.buffer.spill_manager import get_global_manager
    except Exception:  # pragma: no cover - older layouts
        return
    if get_global_manager() is not None:
        warnings.warn(
            "cuDF spilling is enabled (CUDF_SPILL=1). The spill manager tracks "
            "one flat set of buffers with no notion of which GPU they live on, "
            "so unspilling can restore a buffer onto the wrong device. Set "
            "CUDF_SPILL=0 when using cudf.multigpu.",
            RuntimeWarning,
            stacklevel=3,
        )


def _pin_thread_to_device(device: int) -> None:
    """Thread initializer: bind this worker thread to ``device`` forever."""
    _chk(cudart.cudaSetDevice(device), "cudaSetDevice")
    # Establish the primary context now so the first real call is not slow.
    _chk(cudart.cudaFree(0), "cudaFree(0)")
    try:
        import cupy as cp

        cp.cuda.Device(device).use()
    except Exception:  # pragma: no cover - cupy is optional at this layer
        pass


def _current_device() -> int:
    return int(_chk(cudart.cudaGetDevice(), "cudaGetDevice"))


def _device_synchronize() -> None:
    _chk(cudart.cudaDeviceSynchronize(), "cudaDeviceSynchronize")


# ----------------------------------------------------------------------
# process-wide singleton
# ----------------------------------------------------------------------
_RUNTIME: DeviceRuntime | None = None
_RUNTIME_LOCK = threading.Lock()


def init(devices: Sequence[int] | None = None, **kwargs: Any) -> DeviceRuntime:
    """Initialize (or return) the process-wide multi-GPU runtime."""
    global _RUNTIME
    with _RUNTIME_LOCK:
        if _RUNTIME is not None:
            return _RUNTIME
        if devices is None:
            env = os.environ.get("CUDF_MULTIGPU_DEVICES")
            if env:
                devices = [int(x) for x in env.replace(" ", "").split(",") if x]
        _RUNTIME = DeviceRuntime(devices, **kwargs)
        return _RUNTIME


def get_runtime() -> DeviceRuntime:
    """Return the process-wide runtime, initializing it on first use."""
    if _RUNTIME is None:
        return init()
    return _RUNTIME


def is_initialized() -> bool:
    return _RUNTIME is not None


def shutdown() -> None:
    """Tear down the process-wide runtime."""
    global _RUNTIME
    with _RUNTIME_LOCK:
        if _RUNTIME is not None:
            _RUNTIME.shutdown()
            _RUNTIME = None
