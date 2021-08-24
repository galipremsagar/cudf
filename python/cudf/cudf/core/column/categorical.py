# Copyright (c) 2018-2021, NVIDIA CORPORATION.

from __future__ import annotations

import pickle
from collections.abc import MutableSequence
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    cast,
)

import numpy as np
import pandas as pd
import pyarrow as pa
from numba import cuda

import cudf
from cudf import _lib as libcudf
from cudf._lib.transform import bools_to_mask
from cudf._typing import ColumnLike, Dtype, ScalarLike
from cudf.core.buffer import Buffer
from cudf.core.column import column
from cudf.core.column.methods import ColumnMethods
from cudf.core.dtypes import CategoricalDtype
from cudf.utils.dtypes import (
    is_categorical_dtype,
    is_interval_dtype,
    is_mixed_with_object_dtype,
    min_signed_type,
    min_unsigned_type,
)

if TYPE_CHECKING:
    from cudf._typing import SeriesOrIndex
    from cudf.core.column import (
        ColumnBase,
        DatetimeColumn,
        NumericalColumn,
        StringColumn,
        TimeDeltaColumn,
    )


class CategoricalAccessor(ColumnMethods):
    """
    Accessor object for categorical properties of the Series values.
    Be aware that assigning to `categories` is a inplace operation,
    while all methods return new categorical data per default.

    Parameters
    ----------
    column : Column
    parent : Series or CategoricalIndex

    Examples
    --------
    >>> s = cudf.Series([1,2,3], dtype='category')
    >>> s
    >>> s
    0    1
    1    2
    2    3
    dtype: category
    Categories (3, int64): [1, 2, 3]
    >>> s.cat.categories
    Int64Index([1, 2, 3], dtype='int64')
    >>> s.cat.reorder_categories([3,2,1])
    0    1
    1    2
    2    3
    dtype: category
    Categories (3, int64): [3, 2, 1]
    >>> s.cat.remove_categories([1])
    0    <NA>
    1       2
    2       3
    dtype: category
    Categories (2, int64): [2, 3]
    >>> s.cat.set_categories(list('abcde'))
    0    <NA>
    1    <NA>
    2    <NA>
    dtype: category
    Categories (5, object): ['a', 'b', 'c', 'd', 'e']
    >>> s.cat.as_ordered()
    0    1
    1    2
    2    3
    dtype: category
    Categories (3, int64): [1 < 2 < 3]
    >>> s.cat.as_unordered()
    0    1
    1    2
    2    3
    dtype: category
    Categories (3, int64): [1, 2, 3]
    """

    _column: CategoricalColumn

    def __init__(self, parent: SeriesOrIndex):
        if not is_categorical_dtype(parent.dtype):
            raise AttributeError(
                "Can only use .cat accessor with a 'category' dtype"
            )
        super().__init__(parent=parent)

    @property
    def categories(self) -> "cudf.core.index.BaseIndex":
        """
        The categories of this categorical.
        """
        return cudf.core.index.as_index(self._column.categories)

    @property
    def codes(self) -> "cudf.Series":
        """
        Return Series of codes as well as the index.
        """
        index = (
            self._parent.index
            if isinstance(self._parent, cudf.Series)
            else None
        )
        return cudf.Series(self._column.codes, index=index)

    @property
    def ordered(self) -> Optional[bool]:
        """
        Whether the categories have an ordered relationship.
        """
        return self._column.ordered

    def as_ordered(self, inplace: bool = False) -> Optional[SeriesOrIndex]:
        """
        Set the Categorical to be ordered.

        Parameters
        ----------

        inplace : bool, default False
            Whether or not to add the categories inplace
            or return a copy of this categorical with
            added categories.

        Returns
        -------
        Categorical
            Ordered Categorical or None if inplace.

        Examples
        --------
        >>> import cudf
        >>> s = cudf.Series([10, 1, 1, 2, 10, 2, 10], dtype="category")
        >>> s
        0    10
        1     1
        2     1
        3     2
        4    10
        5     2
        6    10
        dtype: category
        Categories (3, int64): [1, 2, 10]
        >>> s.cat.as_ordered()
        0    10
        1     1
        2     1
        3     2
        4    10
        5     2
        6    10
        dtype: category
        Categories (3, int64): [1 < 2 < 10]
        >>> s.cat.as_ordered(inplace=True)
        >>> s
        0    10
        1     1
        2     1
        3     2
        4    10
        5     2
        6    10
        dtype: category
        Categories (3, int64): [1 < 2 < 10]
        """
        return self._return_or_inplace(
            self._column.as_ordered(), inplace=inplace
        )

    def as_unordered(self, inplace: bool = False) -> Optional[SeriesOrIndex]:
        """
        Set the Categorical to be unordered.

        Parameters
        ----------

        inplace : bool, default False
            Whether or not to set the ordered attribute
            in-place or return a copy of this
            categorical with ordered set to False.

        Returns
        -------
        Categorical
            Unordered Categorical or None if inplace.

        Examples
        --------
        >>> import cudf
        >>> s = cudf.Series([10, 1, 1, 2, 10, 2, 10], dtype="category")
        >>> s
        0    10
        1     1
        2     1
        3     2
        4    10
        5     2
        6    10
        dtype: category
        Categories (3, int64): [1, 2, 10]
        >>> s = s.cat.as_ordered()
        >>> s
        0    10
        1     1
        2     1
        3     2
        4    10
        5     2
        6    10
        dtype: category
        Categories (3, int64): [1 < 2 < 10]
        >>> s.cat.as_unordered()
        0    10
        1     1
        2     1
        3     2
        4    10
        5     2
        6    10
        dtype: category
        Categories (3, int64): [1, 2, 10]
        >>> s.cat.as_unordered(inplace=True)
        >>> s
        0    10
        1     1
        2     1
        3     2
        4    10
        5     2
        6    10
        dtype: category
        Categories (3, int64): [1, 2, 10]
        """
        return self._return_or_inplace(
            self._column.as_unordered(), inplace=inplace
        )

    def add_categories(
        self, new_categories: Any, inplace: bool = False
    ) -> Optional[SeriesOrIndex]:
        """
        Add new categories.

        `new_categories` will be included at the last/highest
        place in the categories and will be unused directly
        after this call.

        Parameters
        ----------

        new_categories : category or list-like of category
            The new categories to be included.

        inplace : bool, default False
            Whether or not to add the categories inplace
            or return a copy of this categorical with
            added categories.

        Returns
        -------
        cat
            Categorical with new categories added or
            None if inplace.

        Examples
        --------
        >>> import cudf
        >>> s = cudf.Series([1, 2], dtype="category")
        >>> s
        0    1
        1    2
        dtype: category
        Categories (2, int64): [1, 2]
        >>> s.cat.add_categories([0, 3, 4])
        0    1
        1    2
        dtype: category
        Categories (5, int64): [1, 2, 0, 3, 4]
        >>> s
        0    1
        1    2
        dtype: category
        Categories (2, int64): [1, 2]
        >>> s.cat.add_categories([0, 3, 4], inplace=True)
        >>> s
        0    1
        1    2
        dtype: category
        Categories (5, int64): [1, 2, 0, 3, 4]
        """

        old_categories = self._column.categories
        new_categories = column.as_column(
            new_categories,
            dtype=old_categories.dtype if len(new_categories) == 0 else None,
        )

        if is_mixed_with_object_dtype(old_categories, new_categories):
            raise TypeError(
                f"cudf does not support adding categories with existing "
                f"categories of dtype `{old_categories.dtype}` and new "
                f"categories of dtype `{new_categories.dtype}`, please "
                f"type-cast new_categories to the same type as "
                f"existing categories."
            )
        common_dtype = np.find_common_type(
            [old_categories.dtype, new_categories.dtype], []
        )

        new_categories = new_categories.astype(common_dtype)
        old_categories = old_categories.astype(common_dtype)

        if old_categories.isin(new_categories).any():
            raise ValueError("new categories must not include old categories")

        new_categories = old_categories.append(new_categories)
        out_col = self._column
        if not out_col._categories_equal(new_categories):
            out_col = out_col._set_categories(new_categories)

        return self._return_or_inplace(out_col, inplace=inplace)

    def remove_categories(
        self, removals: Any, inplace: bool = False,
    ) -> Optional[SeriesOrIndex]:
        """
        Remove the specified categories.

        `removals` must be included in the
        old categories. Values which were in the
        removed categories will be set to null.

        Parameters
        ----------

        removals : category or list-like of category
            The categories which should be removed.

        inplace : bool, default False
            Whether or not to remove the categories
            inplace or return a copy of this categorical
            with removed categories.

        Returns
        -------
        cat
            Categorical with removed categories or None
            if inplace.

        Examples
        --------
        >>> import cudf
        >>> s = cudf.Series([10, 1, 1, 2, 10, 2, 10], dtype="category")
        >>> s
        0    10
        1     1
        2     1
        3     2
        4    10
        5     2
        6    10
        dtype: category
        Categories (3, int64): [1, 2, 10]
        >>> s.cat.remove_categories([1])
        0      10
        1    <NA>
        2    <NA>
        3       2
        4      10
        5       2
        6      10
        dtype: category
        Categories (2, int64): [2, 10]
        >>> s
        0    10
        1     1
        2     1
        3     2
        4    10
        5     2
        6    10
        dtype: category
        Categories (3, int64): [1, 2, 10]
        >>> s.cat.remove_categories([10], inplace=True)
        >>> s
        0    <NA>
        1       1
        2       1
        3       2
        4    <NA>
        5       2
        6    <NA>
        dtype: category
        Categories (2, int64): [1, 2]
        """
        cats = self.categories.to_series()
        removals = cudf.Series(removals, dtype=cats.dtype)
        removals_mask = removals.isin(cats)

        # ensure all the removals are in the current categories
        # list. If not, raise an error to match Pandas behavior
        if not removals_mask.all():
            vals = removals[~removals_mask].to_array()
            raise ValueError(f"removals must all be in old categories: {vals}")

        new_categories = cats[~cats.isin(removals)]._column
        out_col = self._column
        if not out_col._categories_equal(new_categories):
            out_col = out_col._set_categories(new_categories)

        return self._return_or_inplace(out_col, inplace=inplace)

    def set_categories(
        self,
        new_categories: Any,
        ordered: bool = False,
        rename: bool = False,
        inplace: bool = False,
    ) -> Optional[SeriesOrIndex]:
        """
        Set the categories to the specified new_categories.


        `new_categories` can include new categories (which
        will result in unused categories) or remove old categories
        (which results in values set to null). If `rename==True`,
        the categories will simple be renamed (less or more items
        than in old categories will result in values set to null or
        in unused categories respectively).

        This method can be used to perform more than one action
        of adding, removing, and reordering simultaneously and
        is therefore faster than performing the individual steps
        via the more specialised methods.

        On the other hand this methods does not do checks
        (e.g., whether the old categories are included in the
        new categories on a reorder), which can result in
        surprising changes.

        Parameters
        ----------

        new_categories : list-like
            The categories in new order.

        ordered : bool, default None
            Whether or not the categorical is treated as
            a ordered categorical. If not given, do
            not change the ordered information.

        rename : bool, default False
            Whether or not the `new_categories` should be
            considered as a rename of the old categories
            or as reordered categories.

        inplace : bool, default False
            Whether or not to reorder the categories in-place
            or return a copy of this categorical with
            reordered categories.

        Returns
        -------
        cat
            Categorical with reordered categories
            or None if inplace.

        Examples
        --------
        >>> import cudf
        >>> s = cudf.Series([1, 1, 2, 10, 2, 10], dtype='category')
        >>> s
        0     1
        1     1
        2     2
        3    10
        4     2
        5    10
        dtype: category
        Categories (3, int64): [1, 2, 10]
        >>> s.cat.set_categories([1, 10])
        0       1
        1       1
        2    <NA>
        3      10
        4    <NA>
        5      10
        dtype: category
        Categories (2, int64): [1, 10]
        >>> s.cat.set_categories([1, 10], inplace=True)
        >>> s
        0       1
        1       1
        2    <NA>
        3      10
        4    <NA>
        5      10
        dtype: category
        Categories (2, int64): [1, 10]
        """
        return self._return_or_inplace(
            self._column.set_categories(
                new_categories=new_categories, ordered=ordered, rename=rename
            ),
            inplace=inplace,
        )

    def reorder_categories(
        self,
        new_categories: Any,
        ordered: bool = False,
        inplace: bool = False,
    ) -> Optional[SeriesOrIndex]:
        """
        Reorder categories as specified in new_categories.

        `new_categories` need to include all old categories
        and no new category items.

        Parameters
        ----------

        new_categories : Index-like
            The categories in new order.

        ordered : bool, optional
            Whether or not the categorical is treated
            as a ordered categorical. If not given, do
            not change the ordered information.


        inplace : bool, default False
            Whether or not to reorder the categories
            inplace or return a copy of this categorical
            with reordered categories.


        Returns
        -------
        cat
            Categorical with reordered categories or
            None if inplace.

        Raises
        ------
        ValueError
            If the new categories do not contain all old
            category items or any new ones.


        Examples
        --------
        >>> import cudf
        >>> s = cudf.Series([10, 1, 1, 2, 10, 2, 10], dtype="category")
        >>> s
        0    10
        1     1
        2     1
        3     2
        4    10
        5     2
        6    10
        dtype: category
        Categories (3, int64): [1, 2, 10]
        >>> s.cat.reorder_categories([10, 1, 2])
        0    10
        1     1
        2     1
        3     2
        4    10
        5     2
        6    10
        dtype: category
        Categories (3, int64): [10, 1, 2]
        >>> s.cat.reorder_categories([10, 1])
        ValueError: items in new_categories are not the same as in
        old categories
        """
        return self._return_or_inplace(
            self._column.reorder_categories(new_categories, ordered=ordered),
            inplace=inplace,
        )


class CategoricalColumn(column.ColumnBase):
    """
    Implements operations for Columns of Categorical type

    Parameters
    ----------
    dtype : CategoricalDtype
    mask : Buffer
        The validity mask
    offset : int
        Data offset
    children : Tuple[ColumnBase]
        Two non-null columns containing the categories and codes
        respectively
    """

    dtype: cudf.core.dtypes.CategoricalDtype
    _codes: Optional[NumericalColumn]
    _children: Tuple[NumericalColumn]

    def __init__(
        self,
        dtype: CategoricalDtype,
        mask: Buffer = None,
        size: int = None,
        offset: int = 0,
        null_count: int = None,
        children: Tuple["column.ColumnBase", ...] = (),
    ):

        if size is None:
            for child in children:
                assert child.offset == 0
                assert child.base_mask is None
            size = children[0].size
            size = size - offset
        if isinstance(dtype, pd.api.types.CategoricalDtype):
            dtype = CategoricalDtype.from_pandas(dtype)
        if not isinstance(dtype, CategoricalDtype):
            raise ValueError("dtype must be instance of CategoricalDtype")
        super().__init__(
            data=None,
            size=size,
            dtype=dtype,
            mask=mask,
            offset=offset,
            null_count=null_count,
            children=children,
        )
        self._codes = None

    @property
    def base_size(self) -> int:
        return int(
            (self.base_children[0].size) / self.base_children[0].dtype.itemsize
        )

    def __contains__(self, item: ScalarLike) -> bool:
        try:
            self._encode(item)
        except ValueError:
            return False
        return self._encode(item) in self.as_numerical

    def serialize(self) -> Tuple[dict, list]:
        header: Dict[Any, Any] = {}
        frames = []
        header["type-serialized"] = pickle.dumps(type(self))
        header["dtype"], dtype_frames = self.dtype.serialize()
        header["dtype_frames_count"] = len(dtype_frames)
        frames.extend(dtype_frames)
        header["data"], data_frames = self.codes.serialize()
        header["data_frames_count"] = len(data_frames)
        frames.extend(data_frames)
        if self.mask is not None:
            mask_header, mask_frames = self.mask.serialize()
            header["mask"] = mask_header
            frames.extend(mask_frames)
        header["frame_count"] = len(frames)
        return header, frames

    @classmethod
    def deserialize(cls, header: dict, frames: list) -> CategoricalColumn:
        n_dtype_frames = header["dtype_frames_count"]
        dtype = CategoricalDtype.deserialize(
            header["dtype"], frames[:n_dtype_frames]
        )
        n_data_frames = header["data_frames_count"]

        column_type = pickle.loads(header["data"]["type-serialized"])
        data = column_type.deserialize(
            header["data"],
            frames[n_dtype_frames : n_dtype_frames + n_data_frames],
        )
        mask = None
        if "mask" in header:
            mask = Buffer.deserialize(
                header["mask"], [frames[n_dtype_frames + n_data_frames]]
            )
        return cast(
            CategoricalColumn,
            column.build_column(
                data=None,
                dtype=dtype,
                mask=mask,
                children=(column.as_column(data.base_data, dtype=data.dtype),),
            ),
        )

    def set_base_data(self, value):
        if value is not None:
            raise RuntimeError(
                "CategoricalColumns do not use data attribute of Column, use "
                "`set_base_children` instead"
            )
        else:
            super().set_base_data(value)

    def _process_values_for_isin(
        self, values: Sequence
    ) -> Tuple[ColumnBase, ColumnBase]:
        lhs = self
        # We need to convert values to same type as self,
        # hence passing dtype=self.dtype
        rhs = cudf.core.column.as_column(values, dtype=self.dtype)
        return lhs, rhs

    def set_base_mask(self, value: Optional[Buffer]):
        super().set_base_mask(value)
        self._codes = None

    def set_base_children(self, value: Tuple[ColumnBase, ...]):
        super().set_base_children(value)
        self._codes = None

    @property
    def children(self) -> Tuple[NumericalColumn]:
        if self._children is None:
            codes_column = self.base_children[0]

            buf = Buffer(codes_column.base_data)
            buf.ptr = buf.ptr + (self.offset * codes_column.dtype.itemsize)
            buf.size = self.size * codes_column.dtype.itemsize

            codes_column = cast(
                cudf.core.column.NumericalColumn,
                column.build_column(
                    data=buf, dtype=codes_column.dtype, size=self.size,
                ),
            )
            self._children = (codes_column,)
        return self._children

    @property
    def as_numerical(self) -> NumericalColumn:
        return cast(
            cudf.core.column.NumericalColumn,
            column.build_column(
                data=self.codes.data, dtype=self.codes.dtype, mask=self.mask
            ),
        )

    @property
    def categories(self) -> ColumnBase:
        return self.dtype.categories._values

    @categories.setter
    def categories(self, value):
        self.dtype = CategoricalDtype(
            categories=value, ordered=self.dtype.ordered
        )

    @property
    def codes(self) -> NumericalColumn:
        if self._codes is None:
            self._codes = self.children[0].set_mask(self.mask)
        return cast(cudf.core.column.NumericalColumn, self._codes)

    @property
    def ordered(self) -> Optional[bool]:
        return self.dtype.ordered

    @ordered.setter
    def ordered(self, value: bool):
        self.dtype.ordered = value

    def unary_operator(self, unaryop: str):
        raise TypeError(
            f"Series of dtype `category` cannot perform the operation: "
            f"{unaryop}"
        )

    def __setitem__(self, key, value):
        if cudf.utils.dtypes.is_scalar(
            value
        ) and cudf._lib.scalar._is_null_host_scalar(value):
            to_add_categories = 0
        else:
            to_add_categories = len(
                cudf.Index(value).difference(self.categories)
            )

        if to_add_categories > 0:
            raise ValueError(
                "Cannot setitem on a Categorical with a new "
                "category, set the categories first"
            )

        if cudf.utils.dtypes.is_scalar(value):
            value = self._encode(value) if value is not None else value
        else:
            value = cudf.core.column.as_column(value).astype(self.dtype)
            value = value.codes
        codes = self.codes
        codes[key] = value
        out = cudf.core.column.build_categorical_column(
            categories=self.categories,
            codes=codes,
            mask=codes.base_mask,
            size=codes.size,
            offset=self.offset,
            ordered=self.ordered,
        )
        self._mimic_inplace(out, inplace=True)

    def _fill(
        self,
        fill_value: ScalarLike,
        begin: int,
        end: int,
        inplace: bool = False,
    ) -> "column.ColumnBase":
        if end <= begin or begin >= self.size:
            return self if inplace else self.copy()

        fill_code = self._encode(fill_value)
        fill_scalar = cudf._lib.scalar.as_device_scalar(
            fill_code, self.codes.dtype
        )

        result = self if inplace else self.copy()

        libcudf.filling.fill_in_place(result.codes, begin, end, fill_scalar)
        return result

    def slice(
        self, start: int, stop: int, stride: int = None
    ) -> "column.ColumnBase":
        codes = self.codes.slice(start, stop, stride)
        return cudf.core.column.build_categorical_column(
            categories=self.categories,
            codes=cudf.core.column.as_column(
                codes.base_data, dtype=codes.dtype
            ),
            mask=codes.base_mask,
            ordered=self.ordered,
            size=codes.size,
            offset=codes.offset,
        )

    def binary_operator(
        self, op: str, rhs, reflect: bool = False
    ) -> ColumnBase:
        if not (self.ordered and rhs.ordered) and op not in (
            "eq",
            "ne",
            "NULL_EQUALS",
        ):
            if op in ("lt", "gt", "le", "ge"):
                raise TypeError(
                    "Unordered Categoricals can only compare equality or not"
                )
            raise TypeError(
                f"Series of dtype `{self.dtype}` cannot perform the "
                f"operation: {op}"
            )
        if self.dtype != rhs.dtype:
            raise TypeError("Categoricals can only compare with the same type")
        return self.as_numerical.binary_operator(op, rhs.as_numerical)

    def normalize_binop_value(self, other: ScalarLike) -> CategoricalColumn:

        if isinstance(other, np.ndarray) and other.ndim == 0:
            other = other.item()

        ary = cudf.utils.utils.scalar_broadcast_to(
            self._encode(other), size=len(self), dtype=self.codes.dtype
        )
        col = column.build_categorical_column(
            categories=self.dtype.categories._values,
            codes=column.as_column(ary),
            mask=self.base_mask,
            ordered=self.dtype.ordered,
        )
        return col

    def sort_by_values(
        self, ascending: bool = True, na_position="last"
    ) -> Tuple[CategoricalColumn, NumericalColumn]:
        codes, inds = self.as_numerical.sort_by_values(ascending, na_position)
        col = column.build_categorical_column(
            categories=self.dtype.categories._values,
            codes=column.as_column(codes.base_data, dtype=codes.dtype),
            mask=codes.base_mask,
            size=codes.size,
            ordered=self.dtype.ordered,
        )
        return col, inds

    def element_indexing(self, index: int) -> ScalarLike:
        val = self.as_numerical.element_indexing(index)
        return self._decode(int(val)) if val is not None else val

    @property
    def __cuda_array_interface__(self) -> Mapping[str, Any]:
        raise TypeError(
            "Categorical does not support `__cuda_array_interface__`."
            " Please consider using `.codes` or `.categories`"
            " if you need this functionality."
        )

    def to_pandas(self, index: pd.Index = None, **kwargs) -> pd.Series:
        if self.categories.dtype.kind == "f":
            new_mask = bools_to_mask(self.notnull())
            col = column.build_categorical_column(
                categories=self.categories,
                codes=column.as_column(self.codes, dtype=self.codes.dtype),
                mask=new_mask,
                ordered=self.dtype.ordered,
                size=self.codes.size,
            )
        else:
            col = self

        signed_dtype = min_signed_type(len(col.categories))
        codes = col.codes.astype(signed_dtype).fillna(-1).to_array()
        if is_interval_dtype(col.categories.dtype):
            # leaving out dropna because it temporarily changes an interval
            # index into a struct and throws off results.
            # TODO: work on interval index dropna
            categories = col.categories.to_pandas()
        else:
            categories = col.categories.dropna(drop_nan=True).to_pandas()
        data = pd.Categorical.from_codes(
            codes, categories=categories, ordered=col.ordered
        )
        return pd.Series(data, index=index)

    def to_arrow(self) -> pa.Array:
        """Convert to PyArrow Array."""
        # arrow doesn't support unsigned codes
        signed_type = (
            min_signed_type(self.codes.max())
            if self.codes.size > 0
            else np.int8
        )
        codes = self.codes.astype(signed_type)
        categories = self.categories

        out_indices = codes.to_arrow()
        out_dictionary = categories.to_arrow()

        return pa.DictionaryArray.from_arrays(
            out_indices, out_dictionary, ordered=self.ordered,
        )

    @property
    def values_host(self) -> np.ndarray:
        """
        Return a numpy representation of the CategoricalColumn.
        """
        return self.to_pandas().values

    @property
    def values(self):
        """
        Return a CuPy representation of the CategoricalColumn.
        """
        raise NotImplementedError("cudf.Categorical is not yet implemented")

    def clip(self, lo: ScalarLike, hi: ScalarLike) -> "column.ColumnBase":
        return (
            self.astype(self.categories.dtype).clip(lo, hi).astype(self.dtype)
        )

    @property
    def data_array_view(self) -> cuda.devicearray.DeviceNDArray:
        return self.codes.data_array_view

    def unique(self) -> CategoricalColumn:
        codes = self.as_numerical.unique()
        return column.build_categorical_column(
            categories=self.categories,
            codes=column.as_column(codes.base_data, dtype=codes.dtype),
            mask=codes.base_mask,
            offset=codes.offset,
            size=codes.size,
            ordered=self.ordered,
        )

    def _encode(self, value) -> ScalarLike:
        return self.categories.find_first_value(value)

    def _decode(self, value: int) -> ScalarLike:
        if value == self.default_na_value():
            return None
        return self.categories.element_indexing(value)

    def default_na_value(self) -> ScalarLike:
        return -1

    def find_and_replace(
        self,
        to_replace: ColumnLike,
        replacement: ColumnLike,
        all_nan: bool = False,
    ) -> CategoricalColumn:
        """
        Return col with *to_replace* replaced with *replacement*.
        """
        to_replace_col = column.as_column(to_replace)
        if len(to_replace_col) == to_replace_col.null_count:
            to_replace_col = to_replace_col.astype(self.categories.dtype)
        replacement_col = column.as_column(replacement)
        if len(replacement_col) == replacement_col.null_count:
            replacement_col = replacement_col.astype(self.categories.dtype)

        if type(to_replace_col) != type(replacement_col):
            raise TypeError(
                f"to_replace and value should be of same types,"
                f"got to_replace dtype: {to_replace_col.dtype} and "
                f"value dtype: {replacement_col.dtype}"
            )
        df = cudf.DataFrame({"old": to_replace_col, "new": replacement_col})
        df = df.drop_duplicates(subset=["old"], keep="last", ignore_index=True)
        if df._data["old"].null_count == 1:
            fill_value = df._data["new"][df._data["old"].isna()][0]
            if fill_value in self.categories:
                replaced = self.fillna(fill_value)
            else:
                new_categories = self.categories.append(
                    column.as_column([fill_value])
                )
                replaced = self.copy()
                replaced = replaced._set_categories(new_categories)
                replaced = replaced.fillna(fill_value)
            df = df.dropna(subset=["old"])
            to_replace_col = df._data["old"]
            replacement_col = df._data["new"]
        else:
            replaced = self
        if df._data["new"].null_count > 0:
            drop_values = df._data["old"][df._data["new"].isna()]
            cur_categories = replaced.categories
            new_categories = cur_categories[
                ~cudf.Series(cur_categories.isin(drop_values))
            ]
            replaced = replaced._set_categories(new_categories)
            df = df.dropna(subset=["new"])
            to_replace_col = df._data["old"]
            replacement_col = df._data["new"]

        # create a dataframe containing the pre-replacement categories
        # and a copy of them to work with. The index of this dataframe
        # represents the original ints that map to the categories
        old_cats = cudf.DataFrame()
        old_cats["cats"] = column.as_column(replaced.dtype.categories)
        new_cats = old_cats.copy(deep=True)

        # Create a column with the appropriate labels replaced
        old_cats["cats_replace"] = old_cats["cats"].replace(
            to_replace_col, replacement_col
        )

        # Construct the new categorical labels
        # If a category is being replaced by an existing one, we
        # want to map it to None. If it's totally new, we want to
        # map it to the new label it is to be replaced by
        dtype_replace = cudf.Series(replacement_col)
        dtype_replace[dtype_replace.isin(old_cats["cats"])] = None
        new_cats["cats"] = new_cats["cats"].replace(
            to_replace_col, dtype_replace
        )

        # anything we mapped to None, we want to now filter out since
        # those categories don't exist anymore
        # Resetting the index creates a column 'index' that associates
        # the original integers to the new labels
        bmask = new_cats._data["cats"].notna()
        new_cats = cudf.DataFrame(
            {"cats": new_cats._data["cats"].apply_boolean_mask(bmask)}
        ).reset_index()

        # old_cats contains replaced categories and the ints that
        # previously mapped to those categories and the index of
        # new_cats is a RangeIndex that contains the new ints
        catmap = old_cats.merge(
            new_cats, left_on="cats_replace", right_on="cats", how="inner"
        )

        # The index of this frame is now the old ints, but the column
        # named 'index', which came from the filtered categories,
        # contains the new ints that we need to map to
        to_replace_col = column.as_column(catmap.index).astype(
            replaced.codes.dtype
        )
        replacement_col = catmap._data["index"].astype(replaced.codes.dtype)

        replaced = column.as_column(replaced.codes)
        output = libcudf.replace.replace(
            replaced, to_replace_col, replacement_col
        )

        return column.build_categorical_column(
            categories=new_cats["cats"],
            codes=column.as_column(output.base_data, dtype=output.dtype),
            mask=output.base_mask,
            offset=output.offset,
            size=output.size,
            ordered=self.dtype.ordered,
        )

    def isnull(self) -> ColumnBase:
        """
        Identify missing values in a CategoricalColumn.
        """
        result = libcudf.unary.is_null(self)

        if self.categories.dtype.kind == "f":
            # Need to consider `np.nan` values incase
            # of an underlying float column
            categories = libcudf.unary.is_nan(self.categories)
            if categories.any():
                code = self._encode(np.nan)
                result = result | (self.codes == cudf.Scalar(code))

        return result

    def notnull(self) -> ColumnBase:
        """
        Identify non-missing values in a CategoricalColumn.
        """
        result = libcudf.unary.is_valid(self)

        if self.categories.dtype.kind == "f":
            # Need to consider `np.nan` values incase
            # of an underlying float column
            categories = libcudf.unary.is_nan(self.categories)
            if categories.any():
                code = self._encode(np.nan)
                result = result & (self.codes != cudf.Scalar(code))

        return result

    def fillna(
        self, fill_value: Any = None, method: Any = None, dtype: Dtype = None
    ) -> CategoricalColumn:
        """
        Fill null values with *fill_value*
        """
        if not self.nullable:
            return self

        if fill_value is not None:
            fill_is_scalar = np.isscalar(fill_value)

            if fill_is_scalar:
                if fill_value == self.default_na_value():
                    fill_value = self.codes.dtype.type(fill_value)
                else:
                    try:
                        fill_value = self._encode(fill_value)
                        fill_value = self.codes.dtype.type(fill_value)
                    except (ValueError) as err:
                        err_msg = "fill value must be in categories"
                        raise ValueError(err_msg) from err
            else:
                fill_value = column.as_column(fill_value, nan_as_null=False)
                if isinstance(fill_value, CategoricalColumn):
                    if self.dtype != fill_value.dtype:
                        raise ValueError(
                            "Cannot set a Categorical with another, "
                            "without identical categories"
                        )
                # TODO: only required if fill_value has a subset of the
                # categories:
                fill_value = fill_value._set_categories(
                    self.categories, is_unique=True,
                )
                fill_value = column.as_column(fill_value.codes).astype(
                    self.codes.dtype
                )

        result = super().fillna(value=fill_value, method=method)

        result = column.build_categorical_column(
            categories=self.dtype.categories._values,
            codes=column.as_column(result.base_data, dtype=result.dtype),
            offset=result.offset,
            size=result.size,
            mask=result.base_mask,
            ordered=self.dtype.ordered,
        )

        return result

    def find_first_value(
        self, value: ScalarLike, closest: bool = False
    ) -> int:
        """
        Returns offset of first value that matches
        """
        return self.as_numerical.find_first_value(self._encode(value))

    def find_last_value(self, value: ScalarLike, closest: bool = False) -> int:
        """
        Returns offset of last value that matches
        """
        return self.as_numerical.find_last_value(self._encode(value))

    @property
    def is_monotonic_increasing(self) -> bool:
        return bool(self.ordered) and self.as_numerical.is_monotonic_increasing

    @property
    def is_monotonic_decreasing(self) -> bool:
        return bool(self.ordered) and self.as_numerical.is_monotonic_decreasing

    def as_categorical_column(
        self, dtype: Dtype, **kwargs
    ) -> CategoricalColumn:
        if isinstance(dtype, str) and dtype == "category":
            return self
        if (
            isinstance(
                dtype, (cudf.core.dtypes.CategoricalDtype, pd.CategoricalDtype)
            )
            and (dtype.categories is None)
            and (dtype.ordered is None)
        ):
            return self

        if isinstance(dtype, pd.CategoricalDtype):
            dtype = CategoricalDtype(
                categories=dtype.categories, ordered=dtype.ordered
            )

        if not isinstance(dtype, CategoricalDtype):
            raise ValueError("dtype must be CategoricalDtype")

        if not isinstance(self.categories, type(dtype.categories._values)):
            # If both categories are of different Column types,
            # return a column full of Nulls.
            return _create_empty_categorical_column(self, dtype)

        return self.set_categories(
            new_categories=dtype.categories, ordered=bool(dtype.ordered)
        )

    def as_numerical_column(self, dtype: Dtype, **kwargs) -> NumericalColumn:
        return self._get_decategorized_column().as_numerical_column(dtype)

    def as_string_column(self, dtype, format=None, **kwargs) -> StringColumn:
        return self._get_decategorized_column().as_string_column(
            dtype, format=format
        )

    def as_datetime_column(self, dtype, **kwargs) -> DatetimeColumn:
        return self._get_decategorized_column().as_datetime_column(
            dtype, **kwargs
        )

    def as_timedelta_column(self, dtype, **kwargs) -> TimeDeltaColumn:
        return self._get_decategorized_column().as_timedelta_column(
            dtype, **kwargs
        )

    def _get_decategorized_column(self) -> ColumnBase:
        if self.null_count == len(self):
            # self.categories is empty; just return codes
            return self.codes
        gather_map = self.codes.astype("int32").fillna(0)
        out = self.categories.take(gather_map)
        out = out.set_mask(self.mask)
        return out

    def copy(self, deep: bool = True) -> CategoricalColumn:
        if deep:
            copied_col = libcudf.copying.copy_column(self)
            copied_cat = libcudf.copying.copy_column(self.dtype._categories)

            return column.build_categorical_column(
                categories=copied_cat,
                codes=column.as_column(
                    copied_col.base_data, dtype=copied_col.dtype
                ),
                offset=copied_col.offset,
                size=copied_col.size,
                mask=copied_col.base_mask,
                ordered=self.dtype.ordered,
            )
        else:
            return column.build_categorical_column(
                categories=self.dtype.categories._values,
                codes=column.as_column(
                    self.codes.base_data, dtype=self.codes.dtype
                ),
                mask=self.base_mask,
                ordered=self.dtype.ordered,
                offset=self.offset,
                size=self.size,
            )

    def __sizeof__(self) -> int:
        return self.categories.__sizeof__() + self.codes.__sizeof__()

    def _memory_usage(self, **kwargs) -> int:
        deep = kwargs.get("deep", False)
        if deep:
            return self.__sizeof__()
        else:
            return self.categories._memory_usage() + self.codes._memory_usage()

    def _mimic_inplace(
        self, other_col: ColumnBase, inplace: bool = False
    ) -> Optional[ColumnBase]:
        out = super()._mimic_inplace(other_col, inplace=inplace)
        if inplace and isinstance(other_col, CategoricalColumn):
            self._codes = other_col._codes
        return out

    def view(self, dtype: Dtype) -> ColumnBase:
        raise NotImplementedError(
            "Categorical column views are not currently supported"
        )

    @staticmethod
    def _concat(objs: MutableSequence[CategoricalColumn]) -> CategoricalColumn:
        # TODO: This function currently assumes it is being called from
        # column.concat_columns, at least to the extent that all the
        # preprocessing in that function has already been done. That should be
        # improved as the concatenation API is solidified.

        # Find the first non-null column:
        head = next((obj for obj in objs if obj.valid_count), objs[0])

        # Combine and de-dupe the categories
        cats = column.concat_columns([o.categories for o in objs]).unique()
        objs = [o._set_categories(cats, is_unique=True) for o in objs]
        codes = [o.codes for o in objs]

        newsize = sum(map(len, codes))
        if newsize > libcudf.MAX_COLUMN_SIZE:
            raise MemoryError(
                f"Result of concat cannot have "
                f"size > {libcudf.MAX_COLUMN_SIZE_STR}"
            )
        elif newsize == 0:
            codes_col = column.column_empty(0, head.codes.dtype, masked=True)
        else:
            # Filter out inputs that have 0 length, then concatenate.
            codes = [o for o in codes if len(o)]
            codes_col = libcudf.concat.concat_columns(objs)

        return column.build_categorical_column(
            categories=column.as_column(cats),
            codes=column.as_column(codes_col.base_data, dtype=codes_col.dtype),
            mask=codes_col.base_mask,
            size=codes_col.size,
            offset=codes_col.offset,
        )

    def _with_type_metadata(
        self: CategoricalColumn, dtype: Dtype
    ) -> CategoricalColumn:
        if isinstance(dtype, CategoricalDtype):
            return column.build_categorical_column(
                categories=dtype.categories._values,
                codes=column.as_column(
                    self.codes.base_data, dtype=self.codes.dtype
                ),
                mask=self.codes.base_mask,
                ordered=dtype.ordered,
                size=self.codes.size,
                offset=self.codes.offset,
                null_count=self.codes.null_count,
            )
        return self

    def set_categories(
        self, new_categories: Any, ordered: bool = False, rename: bool = False,
    ) -> CategoricalColumn:
        # See CategoricalAccessor.set_categories.

        ordered = ordered if ordered is not None else self.ordered
        new_categories = column.as_column(new_categories)

        if isinstance(new_categories, CategoricalColumn):
            new_categories = new_categories.categories

        # when called with rename=True, the pandas behavior is
        # to replace the current category values with the new
        # categories.
        if rename:
            # enforce same length
            if len(new_categories) != len(self.categories):
                raise ValueError(
                    "new_categories must have the same "
                    "number of items as old categories"
                )

            out_col = column.build_categorical_column(
                categories=new_categories,
                codes=self.base_children[0],
                mask=self.base_mask,
                size=self.size,
                offset=self.offset,
                ordered=ordered,
            )
        else:
            out_col = self
            if not (type(out_col.categories) is type(new_categories)):
                # If both categories are of different Column types,
                # return a column full of Nulls.
                out_col = _create_empty_categorical_column(
                    self,
                    CategoricalDtype(
                        categories=new_categories, ordered=ordered
                    ),
                )
            elif (
                not out_col._categories_equal(new_categories, ordered=ordered)
                or not self.ordered == ordered
            ):
                out_col = out_col._set_categories(
                    new_categories, ordered=ordered,
                )
        return out_col

    def _categories_equal(
        self, new_categories: ColumnBase, ordered=False
    ) -> bool:
        cur_categories = self.categories
        if len(new_categories) != len(cur_categories):
            return False
        if new_categories.dtype != cur_categories.dtype:
            return False
        # if order doesn't matter, sort before the equals call below
        if not ordered:
            cur_categories = cudf.Series(cur_categories).sort_values(
                ignore_index=True
            )
            new_categories = cudf.Series(new_categories).sort_values(
                ignore_index=True
            )
        return cur_categories.equals(new_categories)

    def _set_categories(
        self,
        new_categories: Any,
        is_unique: bool = False,
        ordered: bool = False,
    ) -> CategoricalColumn:
        """Returns a new CategoricalColumn with the categories set to the
        specified *new_categories*.

        Notes
        -----
        Assumes ``new_categories`` is the same dtype as the current categories
        """

        cur_cats = column.as_column(self.categories)
        new_cats = column.as_column(new_categories)

        # Join the old and new categories to build a map from
        # old to new codes, inserting na_sentinel for any old
        # categories that don't exist in the new categories

        # Ensure new_categories is unique first
        if not (is_unique or new_cats.is_unique):
            # drop_duplicates() instead of unique() to preserve order
            new_cats = (
                cudf.Series(new_cats)
                .drop_duplicates(ignore_index=True)
                ._column
            )

        cur_codes = self.codes
        max_cat_size = (
            len(cur_cats) if len(cur_cats) > len(new_cats) else len(new_cats)
        )
        out_code_dtype = min_unsigned_type(max_cat_size)

        cur_order = column.arange(len(cur_codes))
        old_codes = column.arange(len(cur_cats), dtype=out_code_dtype)
        new_codes = column.arange(len(new_cats), dtype=out_code_dtype)

        new_df = cudf.DataFrame({"new_codes": new_codes, "cats": new_cats})
        old_df = cudf.DataFrame({"old_codes": old_codes, "cats": cur_cats})
        cur_df = cudf.DataFrame({"old_codes": cur_codes, "order": cur_order})

        # Join the old and new categories and line up their codes
        df = old_df.merge(new_df, on="cats", how="left")
        # Join the old and new codes to "recode" the codes data buffer
        df = cur_df.merge(df, on="old_codes", how="left")
        df = df.sort_values(by="order")
        df.reset_index(drop=True, inplace=True)

        ordered = ordered if ordered is not None else self.ordered
        new_codes = df["new_codes"]._column

        # codes can't have masks, so take mask out before moving in
        return column.build_categorical_column(
            categories=new_cats,
            codes=column.as_column(new_codes.base_data, dtype=new_codes.dtype),
            mask=new_codes.base_mask,
            size=new_codes.size,
            offset=new_codes.offset,
            ordered=ordered,
        )

    def reorder_categories(
        self, new_categories: Any, ordered: bool = False,
    ) -> CategoricalColumn:
        new_categories = column.as_column(new_categories)
        # Compare new_categories against current categories.
        # Ignore order for comparison because we're only interested
        # in whether new_categories has all the same values as the
        # current set of categories.
        if not self._categories_equal(new_categories, ordered=False):
            raise ValueError(
                "items in new_categories are not the same as in "
                "old categories"
            )
        return self._set_categories(new_categories, ordered=ordered)

    def as_ordered(self):
        out_col = self
        if not out_col.ordered:
            out_col = column.build_categorical_column(
                categories=self.categories,
                codes=self.codes,
                mask=self.base_mask,
                size=self.base_size,
                offset=self.offset,
                ordered=True,
            )
        return out_col

    def as_unordered(self):
        out_col = self
        if out_col.ordered:
            out_col = column.build_categorical_column(
                categories=self.categories,
                codes=self.codes,
                mask=self.base_mask,
                size=self.base_size,
                offset=self.offset,
                ordered=False,
            )
        return out_col


def _create_empty_categorical_column(
    categorical_column: CategoricalColumn, dtype: "CategoricalDtype"
) -> CategoricalColumn:
    return column.build_categorical_column(
        categories=column.as_column(dtype.categories),
        codes=column.as_column(
            cudf.utils.utils.scalar_broadcast_to(
                categorical_column.default_na_value(),
                categorical_column.size,
                categorical_column.codes.dtype,
            )
        ),
        offset=categorical_column.offset,
        size=categorical_column.size,
        mask=categorical_column.base_mask,
        ordered=dtype.ordered,
    )


def pandas_categorical_as_column(
    categorical: ColumnLike, codes: ColumnLike = None
) -> CategoricalColumn:

    """Creates a CategoricalColumn from a pandas.Categorical

    If ``codes`` is defined, use it instead of ``categorical.codes``
    """
    codes = categorical.codes if codes is None else codes
    codes = column.as_column(codes)

    valid_codes = codes != codes.dtype.type(-1)

    mask = None
    if not valid_codes.all():
        mask = bools_to_mask(valid_codes)

    return column.build_categorical_column(
        categories=categorical.categories,
        codes=column.as_column(codes.base_data, dtype=codes.dtype),
        size=codes.size,
        mask=mask,
        ordered=categorical.ordered,
    )
