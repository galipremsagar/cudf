# Copyright (c) 2021, NVIDIA CORPORATION.

from __future__ import annotations

import weakref
from typing import Dict, Optional, Tuple, TypeVar

from cudf._typing import Dtype, DtypeObj, ScalarLike
from cudf.core.buffer import DeviceBufferLike
from cudf.core.column import ColumnBase

T = TypeVar("T")

class Column:
    _data: Optional[DeviceBufferLike]
    _mask: Optional[DeviceBufferLike]
    _base_data: Optional[DeviceBufferLike]
    _base_mask: Optional[DeviceBufferLike]
    _dtype: DtypeObj
    _size: int
    _offset: int
    _null_count: int
    _children: Tuple[ColumnBase, ...]
    _base_children: Tuple[ColumnBase, ...]
    _distinct_count: Dict[bool, int]

    def __init__(
        self,
        data: Optional[DeviceBufferLike],
        size: int,
        dtype: Dtype,
        mask: Optional[DeviceBufferLike] = None,
        offset: int = None,
        null_count: int = None,
        children: Tuple[ColumnBase, ...] = (),
    ) -> None: ...
    @property
    def base_size(self) -> int: ...
    @property
    def dtype(self) -> DtypeObj: ...
    @property
    def size(self) -> int: ...
    @property
    def base_data(self) -> Optional[DeviceBufferLike]: ...
    @property
    def base_data_ptr(self) -> int: ...
    @property
    def data(self) -> Optional[DeviceBufferLike]: ...
    @property
    def data_ptr(self) -> int: ...
    def set_base_data(self, value: DeviceBufferLike) -> None: ...
    @property
    def nullable(self) -> bool: ...
    def has_nulls(self, include_nan: bool = False) -> bool: ...
    @property
    def base_mask(self) -> Optional[DeviceBufferLike]: ...
    @property
    def base_mask_ptr(self) -> int: ...
    @property
    def mask(self) -> Optional[DeviceBufferLike]: ...
    @property
    def mask_ptr(self) -> int: ...
    def set_base_mask(self, value: Optional[DeviceBufferLike]) -> None: ...
    def set_mask(self: T, value: Optional[DeviceBufferLike]) -> T: ...
    @property
    def null_count(self) -> int: ...
    @property
    def offset(self) -> int: ...
    @property
    def base_children(self) -> Tuple[ColumnBase, ...]: ...
    @property
    def children(self) -> Tuple[ColumnBase, ...]: ...
    def set_base_children(self, value: Tuple[ColumnBase, ...]) -> None: ...
    def _detach_refs(self) -> None: ...
    def has_a_weakref(self) -> bool: ...
    def _is_cai_zero_copied(self) -> bool: ...
    def _mimic_inplace(
        self, other_col: ColumnBase, inplace=False
    ) -> Optional[ColumnBase]: ...
    # TODO: The val parameter should be Scalar, not ScalarLike
    @staticmethod
    def from_scalar(val: ScalarLike, size: int) -> ColumnBase: ...
