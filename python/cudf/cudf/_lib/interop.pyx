# Copyright (c) 2020-2022, NVIDIA CORPORATION.

import cudf

from cpython cimport pycapsule
from libcpp cimport bool
from libcpp.memory cimport shared_ptr, unique_ptr
from libcpp.string cimport string
from libcpp.utility cimport move
from libcpp.vector cimport vector
from pyarrow.lib cimport CTable, pyarrow_unwrap_table, pyarrow_wrap_table

from cudf._lib.cpp.interop cimport (
    DLManagedTensor,
    column_metadata,
    from_arrow as cpp_from_arrow,
    from_dlpack as cpp_from_dlpack,
    to_arrow as cpp_to_arrow,
    to_dlpack as cpp_to_dlpack,
)
from cudf._lib.cpp.table.table cimport table
from cudf._lib.cpp.table.table_view cimport table_view
from cudf._lib.utils cimport columns_from_unique_ptr, table_view_from_columns
from cudf._lib.cpp.io.types cimport (
    column_in_metadata,
)
from cudf.api.types import is_list_dtype, is_struct_dtype

def from_dlpack(dlpack_capsule):
    """
    Converts a DLPack Tensor PyCapsule into a list of columns.

    DLPack Tensor PyCapsule is expected to have the name "dltensor".
    """
    cdef DLManagedTensor* dlpack_tensor = <DLManagedTensor*>pycapsule.\
        PyCapsule_GetPointer(dlpack_capsule, 'dltensor')
    pycapsule.PyCapsule_SetName(dlpack_capsule, 'used_dltensor')

    cdef unique_ptr[table] c_result

    with nogil:
        c_result = move(
            cpp_from_dlpack(dlpack_tensor)
        )

    res = columns_from_unique_ptr(move(c_result))
    dlpack_tensor.deleter(dlpack_tensor)
    return res


def to_dlpack(list source_columns):
    """
    Converts a list of columns into a DLPack Tensor PyCapsule.

    DLPack Tensor PyCapsule will have the name "dltensor".
    """
    if any(column.null_count for column in source_columns):
        raise ValueError(
            "Cannot create a DLPack tensor with null values. \
                Input is required to have null count as zero."
        )

    cdef DLManagedTensor *dlpack_tensor
    cdef table_view source_table_view = table_view_from_columns(source_columns)

    with nogil:
        dlpack_tensor = cpp_to_dlpack(
            source_table_view
        )

    return pycapsule.PyCapsule_New(
        dlpack_tensor,
        'dltensor',
        dlmanaged_tensor_pycapsule_deleter
    )


cdef void dlmanaged_tensor_pycapsule_deleter(object pycap_obj):
    cdef DLManagedTensor* dlpack_tensor = <DLManagedTensor*>0
    try:
        dlpack_tensor = <DLManagedTensor*>pycapsule.PyCapsule_GetPointer(
            pycap_obj, 'used_dltensor')
        return  # we do not call a used capsule's deleter
    except Exception:
        dlpack_tensor = <DLManagedTensor*>pycapsule.PyCapsule_GetPointer(
            pycap_obj, 'dltensor')
    dlpack_tensor.deleter(dlpack_tensor)


cdef vector[column_metadata] gather_metadata(object metadata, object dtype=None) except *:
    """
    Metadata is stored as lists, and expected format is as follows,
    [["a", [["b"], ["c"], ["d"]]],       [["e"]],        ["f", ["", ""]]].
    First value signifies name of the main parent column,
    and adjacent list will signify child column.
    """
    cdef vector[column_metadata] cpp_metadata
    print("inside 95", dtype)
    if dtype is not None:
        if is_struct_dtype(dtype):
            print("100", dtype.fields)
            print("10011", dtype.fields)
            cpp_metadata.reserve(len(dtype.fields))
            for i, name in enumerate(dtype.fields):
                print("102")
                value = dtype.fields[name]
                cpp_metadata.push_back(column_metadata(name.encode()))
                if is_struct_dtype(value):
                    print("104", name)
                    _set_col_children_metadata(value, cpp_metadata[i])
                else:
                    print("else", name)
    else:
        print("110")
    return cpp_metadata

cdef _set_col_children_metadata(dtype,
                                column_metadata& col_meta):
    print("118")
    if is_struct_dtype(dtype):
        print("120")
        col_meta.children_meta.reserve(len(dtype.fields))
        print("122")
        for i, name in enumerate(dtype.fields):
            print("123")
            value = dtype.fields[name]
            col_meta.children_meta.push_back(column_metadata(name.encode()))
            print("127")
            _set_col_children_metadata(
                value, col_meta.children_meta[i]
            )
    elif is_list_dtype(dtype):
        pass
    else:
        return


def to_arrow(list source_columns, object metadata, object dtype=None):
    """Convert a list of columns from
    cudf Frame to a PyArrow Table.

    Parameters
    ----------
    source_columns : a list of columns to convert
    metadata : a list of metadata, see `gather_metadata` for layout

    Returns
    -------
    pyarrow table
    """

    cdef vector[column_metadata] cpp_metadata = gather_metadata(metadata, dtype)
    cdef table_view input_table_view = table_view_from_columns(source_columns)
    print("148")
    print("148", cpp_metadata.size())
    cdef shared_ptr[CTable] cpp_arrow_table
    with nogil:
        cpp_arrow_table = cpp_to_arrow(
            input_table_view, cpp_metadata
        )

    return pyarrow_wrap_table(cpp_arrow_table)


def from_arrow(object input_table):
    """Convert from PyArrow Table to a list of columns.

    Parameters
    ----------
    input_table : PyArrow table

    Returns
    -------
    A list of columns to construct Frame object
    """
    cdef shared_ptr[CTable] cpp_arrow_table = (
        pyarrow_unwrap_table(input_table)
    )
    cdef unique_ptr[table] c_result

    with nogil:
        c_result = move(cpp_from_arrow(cpp_arrow_table.get()[0]))

    return columns_from_unique_ptr(move(c_result))
