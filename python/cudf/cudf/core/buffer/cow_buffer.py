# Copyright (c) 2022-2023, NVIDIA CORPORATION.

from __future__ import annotations

import weakref
from collections import defaultdict
from typing import Any, DefaultDict, Tuple, Type, TypeVar
from weakref import WeakSet

import rmm

from cudf.core.buffer.buffer import Buffer, cuda_array_interface_wrapper

T = TypeVar("T", bound="CopyOnWriteBuffer")


def _keys_cleanup(ptr):
    weak_set_values = CopyOnWriteBuffer._instances[ptr]
    if (
        len(weak_set_values) == 1
        and next(iter(weak_set_values.data))() is None
    ):
        # When the last remaining reference is being cleaned up we will still
        # have a dead weak-reference in `weak_set_values`, if that is the case
        # we are good to perform the key's cleanup
        del CopyOnWriteBuffer._instances[ptr]


class CopyOnWriteBuffer(Buffer):
    """A Buffer represents device memory.

    Use the factory function `as_buffer` to create a Buffer instance.
    """

    # This dict keeps track of all instances that have the same `ptr`
    # and `size` attributes.  Each key of the dict is a `(ptr, size)`
    # tuple and the corresponding value is a set of weak references to
    # instances with that `ptr` and `size`.
    _instances: DefaultDict[Tuple, WeakSet] = defaultdict(WeakSet)

    # TODO: This is synonymous to SpillableBuffer._exposed attribute
    # and has to be merged.
    _zero_copied: bool

    def _finalize_init(self):
        self.__class__._instances[self._ptr].add(self)
        self._instances = self.__class__._instances[self._ptr]
        self._zero_copied = False
        weakref.finalize(self, _keys_cleanup, self._ptr)

    @classmethod
    def _from_device_memory(
        cls: Type[T], data: Any, *, exposed: bool = False
    ) -> T:
        """Create a Buffer from an object exposing `__cuda_array_interface__`.

        No data is being copied.

        Parameters
        ----------
        data : device-buffer-like
            An object implementing the CUDA Array Interface.
        exposed : bool, optional
            Mark the buffer as zero copied.

        Returns
        -------
        Buffer
            Buffer representing the same device memory as `data`
        """

        # Bypass `__init__` and initialize attributes manually
        ret = super()._from_device_memory(data)
        ret._finalize_init()
        ret._zero_copied = exposed
        return ret

    @classmethod
    def _from_host_memory(cls: Type[T], data: Any) -> T:
        ret = super()._from_host_memory(data)
        ret._finalize_init()
        return ret

    @property
    def _is_shared(self):
        """
        Return `True` if `self`'s memory is shared with other columns.
        """
        return len(self._instances) > 1

    @property
    def ptr(self) -> int:
        """Device pointer to the start of the buffer."""
        self._unlink_shared_buffers()
        self._zero_copied = True
        return self._ptr

    @property
    def mutable_ptr(self) -> int:
        """Device pointer to the start of the buffer."""
        self._unlink_shared_buffers()
        return self._ptr

    def _getitem(self, offset: int, size: int) -> Buffer:
        """
        Helper for `__getitem__`
        """
        return self._from_device_memory(
            cuda_array_interface_wrapper(
                ptr=self._ptr + offset, size=size, owner=self.owner
            )
        )

    def copy(self, deep: bool = True):
        """
        Return a copy of Buffer.

        Parameters
        ----------
        deep : bool, default True
            If True, returns a deep-copy of the underlying Buffer data.
            If False, returns a shallow-copy of the Buffer pointing to
            the same underlying data.

        Returns
        -------
        Buffer
        """
        if not deep and not self._zero_copied:
            copied_buf = CopyOnWriteBuffer.__new__(CopyOnWriteBuffer)
            copied_buf._ptr = self._ptr
            copied_buf._size = self._size
            copied_buf._owner = self._owner
            copied_buf._finalize_init()
            return copied_buf
        else:
            return self._from_device_memory(
                rmm.DeviceBuffer(ptr=self._ptr, size=self.size)
            )

    @property
    def __cuda_array_interface__(self) -> dict:
        # Unlink if there are any weak references.
        self._unlink_shared_buffers()
        # Mark the Buffer as ``zero_copied=True``,
        # which will prevent any copy-on-write
        # mechanism post this operation.
        # This is done because we don't have any
        # control over knowing if a third-party library
        # has modified the data this Buffer is
        # pointing to.
        self._zero_copied = True
        return self._get_cuda_array_interface(readonly=False)

    def _get_cuda_array_interface(self, readonly=False):
        return {
            "data": (self._ptr, readonly),
            "shape": (self.size,),
            "strides": None,
            "typestr": "|u1",
            "version": 0,
        }

    @property
    def _get_readonly_proxy_obj(self) -> dict:
        """
        Internal Implementation for the CUDA Array Interface which is
        read-only.
        """
        return cuda_array_interface_wrapper(
            ptr=self._ptr,
            size=self.size,
            owner=self,
            readonly=True,
            typestr="|u1",
            version=0,
        )

    def _unlink_shared_buffers(self):
        """
        Unlinks a Buffer if it is shared with other buffers by
        making a true deep-copy.
        """
        if not self._zero_copied and self._is_shared:
            # make a deep copy of existing DeviceBuffer
            # and replace pointer to it.
            current_buf = rmm.DeviceBuffer(ptr=self._ptr, size=self._size)
            new_buf = current_buf.copy()
            self._ptr = new_buf.ptr
            self._size = new_buf.size
            self._owner = new_buf
            self._finalize_init()
