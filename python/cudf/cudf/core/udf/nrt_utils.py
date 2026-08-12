# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import contextvars
import os
import threading
from contextlib import contextmanager
from functools import cache
from importlib.util import find_spec

from numba.cuda import config as numba_config

_current_nrt_context: contextvars.ContextVar = contextvars.ContextVar(
    "current_nrt_context"
)


class CaptureNRTUsage:
    """
    Context manager for determining if NRT is needed.
    Managed types may set use_nrt to be true during
    instantiation to signal that NRT must be enabled
    during code generation.
    """

    def __init__(self):
        self.use_nrt = False

    def __enter__(self):
        self._token = _current_nrt_context.set(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        _current_nrt_context.reset(self._token)


@cache
def _get_libcudf_rapids_include_dir():
    spec = find_spec("libcudf")
    if spec is None or spec.submodule_search_locations is None:
        return None

    for package_dir in spec.submodule_search_locations:
        include_dir = os.path.join(package_dir, "include", "rapids")
        if os.path.isfile(os.path.join(include_dir, "cuda", "atomic")):
            return include_dir

    return None


def _append_search_path(search_paths, path):
    paths = [p for p in search_paths.split(os.pathsep) if p]
    if path not in paths:
        paths.append(path)
    return os.pathsep.join(paths)


#: ``numba_config`` is process-global, so concurrent users of ``nrt_enabled``
#: must not each save-and-restore it independently: whichever thread exits
#: first would switch NRT off underneath the others, and their kernels would
#: fail to link with "Unresolved extern function 'NRT_decref'". Nesting is
#: tracked with a depth count so only the outermost exit restores.
_nrt_lock = threading.RLock()
_nrt_depth = 0
_nrt_saved: tuple | None = None


@contextmanager
def nrt_enabled():
    """
    Context manager for enabling NRT via the numba
    config. CUDA_ENABLE_NRT may be toggled dynamically
    for a single kernel launch, so we use this context
    to enable it for those that we know need it.

    Reentrant and thread-safe: the config stays enabled as long as any thread
    is inside the context, which matters when several CUDA devices are being
    driven from separate threads at once.
    """
    global _nrt_depth, _nrt_saved

    with _nrt_lock:
        if _nrt_depth == 0:
            # Discovery happens before anything global is touched. It can
            # raise, and an exception here escapes before the ``try`` below,
            # so the ``finally`` that restores the config never runs -- any
            # mutation made first would leak NRT on permanently.
            include_dir = _get_libcudf_rapids_include_dir()
            _nrt_saved = (
                getattr(numba_config, "CUDA_ENABLE_NRT", False),
                getattr(numba_config, "CUDA_NVRTC_EXTRA_SEARCH_PATHS", ""),
            )
            numba_config.CUDA_ENABLE_NRT = True
            if include_dir is not None:
                numba_config.CUDA_NVRTC_EXTRA_SEARCH_PATHS = (
                    _append_search_path(_nrt_saved[1] or "", include_dir)
                )
        _nrt_depth += 1

    try:
        yield
    finally:
        with _nrt_lock:
            _nrt_depth -= 1
            if _nrt_depth == 0 and _nrt_saved is not None:
                (
                    numba_config.CUDA_ENABLE_NRT,
                    numba_config.CUDA_NVRTC_EXTRA_SEARCH_PATHS,
                ) = _nrt_saved
                _nrt_saved = None
