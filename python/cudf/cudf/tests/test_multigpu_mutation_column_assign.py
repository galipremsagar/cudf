# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""Column creation / replacement / deletion on the multi-GPU cudf.pandas backend.

Every test compares the multi-GPU-backed object against the same mutation run
on a real pandas copy of itself (``obj._fsproxy_slow``).  pandas is the oracle:
where pandas raises, the backend is expected to raise too.

The frames are deliberately large enough to be spread over every GPU;
``assert_partitioned`` records that the multi-GPU path -- and not a degenerate
single chunk -- is what actually ran.
"""

import warnings

import pytest

pytest.importorskip("cudf.multigpu")

import cudf.multigpu.pandas_compat as _pandas_compat

if not _pandas_compat.is_installed():
    # install() warns that the backend is experimental, and cudf's pytest
    # config turns warnings into errors at import time.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        _pandas_compat.install(
            initial_pool_fraction=0.05, max_pool_fraction=0.30
        )

import numpy as np

import pandas as pd  # noqa: E402  -- must come after install()

#: the backend warns whenever an op has no distributed implementation and
#: gathers onto one GPU. That is a coverage signal, not a failure of these
#: tests, which only care about the answer.
pytestmark = pytest.mark.filterwarnings("ignore::UserWarning")

N_ROWS = 4096


# ----------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------
def assert_partitioned(obj) -> None:
    """Fail unless ``obj`` really is spread over more than one GPU."""
    fast = obj._fsproxy_fast
    assert fast.nchunks > 1, (
        f"expected a partitioned frame, got {fast.nchunks} chunk(s); "
        "this test would not exercise the multi-GPU path"
    )


def make_frame(n: int = N_ROWS):
    """A frame with a plain RangeIndex, a group key and a unique sort key."""
    i = np.arange(n)
    return pd.DataFrame(
        {
            "a": i,
            "b": i * 2.0,
            "k": i % 16,
            # 7919 is coprime with 4096, so this is a permutation of 0..n-1:
            # sorting by it gives a deterministic, tie-free order.
            "u": (i * 7919) % n,
        }
    )


def frames(n: int = N_ROWS):
    """``(multi-GPU frame, equivalent real-pandas frame)``."""
    got = make_frame(n)
    assert_partitioned(got)
    return got, got._fsproxy_slow.copy()


def assert_same(got, expected) -> None:
    pd.testing.assert_frame_equal(got._fsproxy_slow, expected)


def sorted_by_columns(frame):
    """Row-order-insensitive view, for ops whose output order is unspecified."""
    return frame.sort_values(list(frame.columns)).reset_index(drop=True)


# ----------------------------------------------------------------------
# new column from a plain value
# ----------------------------------------------------------------------
def test_assign_new_column_from_python_list():
    got, expected = frames()
    got["c"] = list(range(N_ROWS))
    expected["c"] = list(range(N_ROWS))
    assert_same(got, expected)


def test_assign_new_column_from_numpy_array():
    got, expected = frames()
    got["c"] = np.arange(N_ROWS) * 3
    expected["c"] = np.arange(N_ROWS) * 3
    assert_same(got, expected)


def test_assign_new_column_from_cupy_array():
    """A device array is the natural input for a GPU backend.

    Single-GPU ``cudf.pandas`` accepts it and produces exactly what pandas
    produces from the equivalent host array.
    """
    cp = pytest.importorskip("cupy")
    got, expected = frames()
    got["c"] = cp.arange(N_ROWS) * 3
    expected["c"] = np.arange(N_ROWS) * 3
    assert_same(got, expected)


@pytest.mark.parametrize("value", [7, 2.5, "hi", True])
def test_assign_new_column_from_scalar(value):
    got, expected = frames()
    got["c"] = value
    expected["c"] = value
    assert_same(got, expected)


def test_assign_new_column_from_boolean_array():
    got, expected = frames()
    got["c"] = np.arange(N_ROWS) % 2 == 0
    expected["c"] = np.arange(N_ROWS) % 2 == 0
    assert_same(got, expected)


def test_assign_new_column_returns_none():
    got, expected = frames()
    assert got.__setitem__("c", 1) is None
    assert expected.__setitem__("c", 1) is None


def test_assign_scalar_reaches_every_partition():
    """A broadcast scalar must land on the last GPU as well as the first."""
    got, expected = frames()
    got["c"] = 9
    expected["c"] = 9
    assert_partitioned(got)
    assert_same(got, expected)


# ----------------------------------------------------------------------
# new column from a Series -- index alignment
# ----------------------------------------------------------------------
def test_assign_series_with_identical_index():
    got, expected = frames()
    value = pd.Series(np.arange(N_ROWS) * 3)
    got["c"] = value
    expected["c"] = value._fsproxy_slow
    assert_same(got, expected)


def test_assign_series_with_shifted_index():
    """pandas aligns on labels; only the overlapping labels get values."""
    got, expected = frames()
    value = pd.Series(np.arange(N_ROWS) * 3, index=np.arange(N_ROWS) + 100)
    got["c"] = value
    expected["c"] = value._fsproxy_slow
    assert_same(got, expected)


def test_assign_series_with_reversed_index():
    """Same labels, opposite order: pandas realigns, it does not zip by position."""
    got, expected = frames()
    value = pd.Series(
        np.arange(N_ROWS) * 3, index=np.arange(N_ROWS)[::-1].copy()
    )
    got["c"] = value
    expected["c"] = value._fsproxy_slow
    assert_same(got, expected)


def test_assign_series_covering_only_a_few_rows():
    got, expected = frames()
    value = pd.Series([10, 20, 30], index=[0, 1, 2])
    got["c"] = value
    expected["c"] = value._fsproxy_slow
    assert_same(got, expected)


def test_assign_column_of_another_dataframe():
    got, expected = frames()
    other = make_frame()
    got["c"] = other["a"] * 5
    expected["c"] = other._fsproxy_slow["a"] * 5
    assert_same(got, expected)


def test_assign_column_of_a_reordered_dataframe():
    """``df["c"] = df.sort_values(...)["a"]`` must realign back to df's order."""
    got, expected = frames()
    reordered_got = got.sort_values("u")
    reordered_expected = expected.sort_values("u")
    got["c"] = reordered_got["a"]
    expected["c"] = reordered_expected["a"]
    assert_same(got, expected)


def test_assign_series_from_a_filtered_view():
    got, expected = frames()
    got["c"] = got[got["a"] > 4000]["a"]
    expected["c"] = expected[expected["a"] > 4000]["a"]
    assert_same(got, expected)


# ----------------------------------------------------------------------
# replacing an existing column
# ----------------------------------------------------------------------
def test_replace_existing_column_with_same_dtype():
    got, expected = frames()
    got["a"] = np.arange(N_ROWS) * 10
    expected["a"] = np.arange(N_ROWS) * 10
    assert_same(got, expected)


@pytest.mark.parametrize("dtype", ["float32", "float64", "int32", "str"])
def test_replace_existing_column_with_a_different_dtype(dtype):
    got, expected = frames()
    got["a"] = got["a"].astype(dtype)
    expected["a"] = expected["a"].astype(dtype)
    assert_same(got, expected)


def test_replace_existing_column_updates_dtypes():
    got, expected = frames()
    assert list(got.dtypes) == list(expected.dtypes)
    got["a"] = got["a"].astype("float32")
    expected["a"] = expected["a"].astype("float32")
    assert list(got.dtypes) == list(expected.dtypes)


def test_replace_existing_column_keeps_column_order():
    got, expected = frames()
    got["a"] = 0
    expected["a"] = 0
    assert list(got.columns) == list(expected.columns)
    assert_same(got, expected)


def test_assign_column_built_from_the_frame_itself():
    got, expected = frames()
    got["c"] = got["a"] + got["b"]
    expected["c"] = expected["a"] + expected["b"]
    assert_same(got, expected)


def test_replace_column_in_place_from_the_frame_itself():
    got, expected = frames()
    got["b"] = got["a"] * got["b"]
    expected["b"] = expected["a"] * expected["b"]
    assert_same(got, expected)


# ----------------------------------------------------------------------
# multi-column keys
# ----------------------------------------------------------------------
def test_assign_existing_columns_from_scalar():
    got, expected = frames()
    got[["a", "b"]] = 0
    expected[["a", "b"]] = 0
    assert_same(got, expected)


def test_assign_existing_columns_from_dataframe():
    got, expected = frames()
    got[["a", "b"]] = got[["b", "a"]]
    expected[["a", "b"]] = expected[["b", "a"]]
    assert_same(got, expected)


def test_assign_two_new_columns_from_2d_array():
    """A two-column array fills the two keys column-wise, one value per cell."""
    got, expected = frames()
    value = np.arange(N_ROWS * 2).reshape(N_ROWS, 2) * 1.0
    got[["c", "d"]] = value
    expected[["c", "d"]] = value
    assert_same(got, expected)


def test_assign_two_columns_from_wider_array_raises():
    got, expected = frames()
    value = np.arange(N_ROWS * 3).reshape(N_ROWS, 3) * 1.0
    with pytest.raises(ValueError):
        expected[["c", "d"]] = value
    with pytest.raises(ValueError):
        got[["c", "d"]] = value


# ----------------------------------------------------------------------
# deletion
# ----------------------------------------------------------------------
def test_del_removes_the_column():
    got, expected = frames()
    del got["a"]
    del expected["a"]
    assert_same(got, expected)


def test_del_updates_the_column_index():
    got, expected = frames()
    assert list(got.columns) == list(expected.columns)
    del got["a"]
    del expected["a"]
    assert list(got.columns) == list(expected.columns)
    assert ("a" in got) == ("a" in expected) is False
    assert got.shape == expected.shape


def test_del_missing_column_raises_keyerror():
    got, expected = frames()
    with pytest.raises(KeyError):
        del expected["nope"]
    with pytest.raises(KeyError):
        del got["nope"]


def test_del_then_readd_column():
    got, expected = frames()
    del got["a"]
    got["a"] = 1
    del expected["a"]
    expected["a"] = 1
    assert_same(got, expected)


def test_pop_returns_the_removed_column():
    got, expected = frames()
    popped_got = got.pop("a")
    popped_expected = expected.pop("a")
    pd.testing.assert_series_equal(
        popped_got._fsproxy_slow, popped_expected
    )


def test_pop_removes_the_column_from_the_frame():
    got, expected = frames()
    got.pop("a")
    expected.pop("a")
    assert_same(got, expected)


def test_pop_updates_the_column_index():
    got, expected = frames()
    assert list(got.columns) == list(expected.columns)
    got.pop("a")
    expected.pop("a")
    assert list(got.columns) == list(expected.columns)


def test_pop_updates_shape_and_containment():
    got, expected = frames()
    assert got.shape == expected.shape
    got.pop("a")
    expected.pop("a")
    assert got.shape == expected.shape
    assert ("a" in got) == ("a" in expected) is False


def test_pop_missing_column_raises_keyerror():
    got, expected = frames()
    with pytest.raises(KeyError):
        expected.pop("nope")
    with pytest.raises(KeyError):
        got.pop("nope")


# ----------------------------------------------------------------------
# insert
# ----------------------------------------------------------------------
@pytest.mark.parametrize("loc", [0, 1, 4])
def test_insert_places_the_column_at_the_given_position(loc):
    got, expected = frames()
    got.insert(loc, "c", 9)
    expected.insert(loc, "c", 9)
    assert_same(got, expected)


def test_insert_returns_none():
    got, expected = frames()
    assert expected.insert(1, "c", 9) is None
    assert got.insert(1, "c", 9) is None


def test_insert_updates_the_column_index():
    got, expected = frames()
    assert list(got.columns) == list(expected.columns)
    got.insert(1, "c", 9)
    expected.insert(1, "c", 9)
    assert list(got.columns) == list(expected.columns)
    assert ("c" in got) == ("c" in expected) is True
    assert got.shape == expected.shape


def test_insert_from_list():
    got, expected = frames()
    got.insert(0, "z", list(range(N_ROWS)))
    expected.insert(0, "z", list(range(N_ROWS)))
    assert_same(got, expected)


def test_insert_from_series():
    got, expected = frames()
    got.insert(1, "c", got["a"] * 2)
    expected.insert(1, "c", expected["a"] * 2)
    assert_same(got, expected)


def test_insert_duplicate_name_raises():
    got, expected = frames()
    with pytest.raises(ValueError):
        expected.insert(0, "a", 1)
    with pytest.raises(ValueError):
        got.insert(0, "a", 1)


def test_insert_out_of_range_loc_raises():
    got, expected = frames()
    with pytest.raises(IndexError):
        expected.insert(99, "c", 1)
    with pytest.raises(IndexError):
        got.insert(99, "c", 1)


# ----------------------------------------------------------------------
# empty frames
# ----------------------------------------------------------------------
def test_add_column_to_empty_frame():
    got = pd.DataFrame()
    expected = got._fsproxy_slow.copy()
    # an empty frame is still spread over every GPU, as zero-row chunks
    assert_partitioned(got)
    got["a"] = [1, 2, 3]
    expected["a"] = [1, 2, 3]
    assert_same(got, expected)


def test_add_column_to_empty_frame_does_not_repeat_the_values():
    """The values are the whole column, not one copy per GPU."""
    got = pd.DataFrame()
    expected = got._fsproxy_slow.copy()
    assert_partitioned(got)
    got["a"] = [1, 2, 3]
    expected["a"] = [1, 2, 3]
    assert len(got) == len(expected) == 3


def test_add_column_to_frame_with_columns_but_no_rows():
    got = pd.DataFrame(columns=["a", "b"])
    expected = got._fsproxy_slow.copy()
    got["c"] = 1
    expected["c"] = 1
    assert_same(got, expected)


def test_add_column_to_frame_with_index_but_no_columns():
    got = pd.DataFrame(index=range(5))
    expected = got._fsproxy_slow.copy()
    got["c"] = 1
    expected["c"] = 1
    assert_same(got, expected)


# ----------------------------------------------------------------------
# length mismatches must raise, exactly as pandas does
# ----------------------------------------------------------------------
@pytest.mark.parametrize("delta", [-1, +5])
def test_assign_wrong_length_list_raises(delta):
    got, expected = frames()
    value = list(range(N_ROWS + delta))
    with pytest.raises(ValueError):
        expected["c"] = value
    with pytest.raises(ValueError):
        got["c"] = value


def test_assign_wrong_length_numpy_array_raises():
    got, expected = frames()
    value = np.arange(N_ROWS + 5)
    with pytest.raises(ValueError):
        expected["c"] = value
    with pytest.raises(ValueError):
        got["c"] = value


def test_failed_assignment_leaves_the_frame_unchanged():
    got, expected = frames()
    with pytest.raises(ValueError):
        got["c"] = list(range(N_ROWS - 1))
    assert_same(got, expected)


def test_insert_with_wrong_length_raises():
    got, expected = frames()
    with pytest.raises(ValueError):
        expected.insert(1, "c", [1, 2, 3])
    with pytest.raises(ValueError):
        got.insert(1, "c", [1, 2, 3])


# ----------------------------------------------------------------------
# frames produced by a shuffle-backed op
# ----------------------------------------------------------------------
def test_assign_derived_column_to_sort_values_result():
    got, expected = frames()
    got = got.sort_values("u")
    expected = expected.sort_values("u")
    assert_partitioned(got)
    got["z"] = got["a"] * 2 + 1
    expected["z"] = expected["a"] * 2 + 1
    assert_same(got, expected)


def test_assign_scalar_to_sort_values_result():
    got, expected = frames()
    got = got.sort_values("u")
    expected = expected.sort_values("u")
    got["z"] = 5
    expected["z"] = 5
    assert_same(got, expected)


def test_delete_column_of_sort_values_result():
    got, expected = frames()
    got = got.sort_values("u")
    expected = expected.sort_values("u")
    del got["a"]
    del expected["a"]
    assert_same(got, expected)


def test_assign_numpy_column_to_sorted_and_reindexed_frame():
    got, expected = frames()
    got = got.sort_values("u").reset_index(drop=True)
    expected = expected.sort_values("u").reset_index(drop=True)
    assert_partitioned(got)
    got["z"] = np.arange(N_ROWS)
    expected["z"] = np.arange(N_ROWS)
    assert_same(got, expected)


def test_assign_derived_column_to_groupby_result():
    got, expected = frames()
    got = got.groupby("k").sum()
    expected = expected.groupby("k").sum()
    got["ratio"] = got["a"] / got["b"]
    expected["ratio"] = expected["a"] / expected["b"]
    pd.testing.assert_frame_equal(
        got._fsproxy_slow.sort_index(), expected.sort_index()
    )


def test_delete_column_of_groupby_result():
    got, expected = frames()
    got = got.groupby("k").sum()
    expected = expected.groupby("k").sum()
    del got["a"]
    del expected["a"]
    pd.testing.assert_frame_equal(
        got._fsproxy_slow.sort_index(), expected.sort_index()
    )


def test_assign_derived_column_to_merge_result():
    got, expected = frames()
    right = pd.DataFrame({"k": np.arange(16), "w": np.arange(16) * 1.5})
    got = got.merge(right, on="k")
    expected = expected.merge(right._fsproxy_slow, on="k")
    assert_partitioned(got)
    got["z"] = got["b"] + got["w"]
    expected["z"] = expected["b"] + expected["w"]
    pd.testing.assert_frame_equal(
        sorted_by_columns(got._fsproxy_slow), sorted_by_columns(expected)
    )


def test_assign_derived_column_to_set_index_result():
    got, expected = frames()
    got = got.set_index("u")
    expected = expected.set_index("u")
    got["z"] = got["a"] + 1
    expected["z"] = expected["a"] + 1
    pd.testing.assert_frame_equal(
        got._fsproxy_slow.sort_index(), expected.sort_index()
    )


# ----------------------------------------------------------------------
# the assignment has to happen on the GPUs, not on a host copy
# ----------------------------------------------------------------------
def test_array_assignment_runs_on_the_partitioned_frame():
    """The multi-GPU frame itself must accept a full-length array column.

    cudf.pandas hides a failure here by re-running on host pandas, which for a
    frame that only fits in aggregate GPU memory is fatal rather than slow --
    so this pokes the fast object directly.
    """
    got, _ = frames()
    fast = got._fsproxy_fast
    assert fast.nchunks > 1
    fast["c"] = np.arange(N_ROWS) * 3
    np.testing.assert_array_equal(
        fast.to_pandas()["c"].to_numpy(), np.arange(N_ROWS) * 3
    )
