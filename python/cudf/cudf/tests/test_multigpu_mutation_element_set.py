# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""Mutability: element, row, slice and boolean-mask assignment.

Every test writes into a frame that is really spread over several GPUs and
compares the result against the same write performed by pandas itself.  The
failure this file is built to catch is the *silent* one: an in-place write
that lands on a gathered copy instead of the caller's own object, so nothing
raises and the value is simply gone.
"""

from __future__ import annotations

import warnings

import pytest

pytest.importorskip("cudf.multigpu")

import cudf.multigpu.pandas_compat as _pandas_compat  # noqa: E402

if not _pandas_compat.is_installed():
    # The backend announces itself with a UserWarning, and this repository
    # turns warnings into errors, which would fail collection.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        _pandas_compat.install(initial_pool_fraction=0.05, max_pool_fraction=0.30)

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402  (the multi-GPU-backed pandas)

#: The backend warns whenever an operation has no distributed implementation
#: and gathers onto one GPU.  Under this repository's ``filterwarnings=error``
#: that performance warning would become an exception inside the fast path,
#: which is not what an ordinary user runs into, so it is ignored here.  (It
#: hides nothing: the pass/fail outcome of every test below is the same with
#: and without these two filters.)
pytestmark = [
    pytest.mark.filterwarnings(
        "ignore:.*has no distributed implementation.*:UserWarning"
    ),
    pytest.mark.filterwarnings("ignore:.*cudf.multigpu.*:UserWarning"),
]

# ----------------------------------------------------------------------
# frame sizing: big enough that the rows really span every GPU
# ----------------------------------------------------------------------
_PROBE = pd.DataFrame({"a": np.arange(4096, dtype="int64")})
NCHUNKS = int(_PROBE._fsproxy_fast.nchunks)
del _PROBE

if NCHUNKS < 2:
    pytest.skip(
        "multi-GPU mutation tests need a frame split over at least two chunks",
        allow_module_level=True,
    )

#: rows per chunk, and a total that divides evenly over the GPUs
CHUNK = 512
N = CHUNK * NCHUNKS

#: positions worth writing to: chunk starts, chunk ends, both boundaries of an
#: interior chunk, and the very last row (the chunk furthest from GPU 0).
ROWS = [0, 1, CHUNK - 1, CHUNK, CHUNK + 1, 2 * CHUNK + 3, N - 1]


def make_df(index=None):
    """A numeric frame partitioned across the GPUs."""
    return pd.DataFrame(
        {
            "a": np.arange(N, dtype="int64"),
            "b": np.arange(N, dtype="float64") / 2,
            "c": np.arange(N, dtype="int64") % 7,
        },
        index=index,
    )


def make_str_df():
    return pd.DataFrame(
        {
            "k": np.array([f"v{i % 5}" for i in range(N)]),
            "c": np.arange(N, dtype="int64") % 7,
        }
    )


def make_series(index=None):
    return pd.Series(np.arange(N, dtype="float64"), name="s", index=index)


def labelled_index():
    """Labels that are deliberately not equal to positions."""
    return np.arange(0, N * 10, 10)


def snapshot(obj):
    """A real-pandas copy of ``obj``, leaving ``obj`` on the multi-GPU path.

    Also asserts the object under test is genuinely partitioned, so a green
    test cannot mean "ran on a single chunk".
    """
    expected = obj._fsproxy_slow.copy()
    fast = obj._fsproxy_fast
    assert fast.nchunks > 1, "frame is not partitioned; test proves nothing"
    return expected


def check_df(got, expected):
    pd.testing.assert_frame_equal(got._fsproxy_slow, expected)


def check_series(got, expected):
    pd.testing.assert_series_equal(got._fsproxy_slow, expected)


# ----------------------------------------------------------------------
# single cells: .loc / .iloc / .at / .iat
# ----------------------------------------------------------------------
@pytest.mark.parametrize("row", ROWS)
def test_df_loc_sets_one_cell(row):
    got = make_df()
    expected = snapshot(got)
    got.loc[row, "a"] = -11
    expected.loc[row, "a"] = -11
    check_df(got, expected)


@pytest.mark.parametrize("row", ROWS)
def test_df_iloc_sets_one_cell(row):
    got = make_df()
    expected = snapshot(got)
    got.iloc[row, 1] = -1.5
    expected.iloc[row, 1] = -1.5
    check_df(got, expected)


@pytest.mark.parametrize("row", ROWS)
def test_df_at_sets_one_cell(row):
    got = make_df()
    expected = snapshot(got)
    got.at[row, "b"] = 3.25
    expected.at[row, "b"] = 3.25
    check_df(got, expected)


@pytest.mark.parametrize("row", ROWS)
def test_df_iat_sets_one_cell(row):
    got = make_df()
    expected = snapshot(got)
    got.iat[row, 0] = -7
    expected.iat[row, 0] = -7
    check_df(got, expected)


def test_df_iloc_sets_one_cell_by_negative_position():
    got = make_df()
    expected = snapshot(got)
    got.iloc[-1, 0] = -13
    expected.iloc[-1, 0] = -13
    check_df(got, expected)


def test_df_loc_sets_one_cell_on_a_non_default_index():
    got = make_df(index=labelled_index())
    expected = snapshot(got)
    got.loc[(N - 1) * 10, "a"] = -17
    expected.loc[(N - 1) * 10, "a"] = -17
    check_df(got, expected)


def test_df_at_sets_one_cell_on_a_non_default_index():
    got = make_df(index=labelled_index())
    expected = snapshot(got)
    got.at[3210, "b"] = -0.5
    expected.at[3210, "b"] = -0.5
    check_df(got, expected)


def test_df_at_sets_a_string_cell():
    got = make_str_df()
    expected = snapshot(got)
    got.at[N - 1, "k"] = "zz"
    expected.at[N - 1, "k"] = "zz"
    check_df(got, expected)


def test_repeated_cell_writes_all_survive():
    got = make_df()
    expected = snapshot(got)
    for frame in (got, expected):
        frame.iat[0, 0] = -1
        frame.iat[CHUNK, 0] = -2
        frame.iat[N - 1, 0] = -3
        frame.at[7, "b"] = 0.5
    check_df(got, expected)


def test_cell_write_is_visible_through_an_alias():
    got = make_df()
    expected = snapshot(got)
    alias = got
    got.iat[N - 1, 0] = -19
    expected.iat[N - 1, 0] = -19
    assert alias is got
    check_df(alias, expected)


def test_cell_write_is_readable_back_through_the_proxy():
    got = make_df()
    snapshot(got)
    got.iat[N - 1, 0] = -23
    assert got.iat[N - 1, 0] == -23
    assert got.iloc[N - 1, 0] == -23
    assert got.at[N - 1, "a"] == -23


def test_cell_assignment_returns_none():
    got = make_df()
    snapshot(got)
    assert got.__setitem__("a", 1) is None
    assert got.loc.__setitem__((slice(None), "b"), 1.0) is None
    assert got.iloc.__setitem__((0, 0), 1) is None
    assert got.at.__setitem__((0, "a"), 1) is None
    assert got.iat.__setitem__((0, 0), 1) is None


def test_out_of_bounds_iat_write_raises_and_leaves_the_frame_alone():
    got = make_df()
    expected = snapshot(got)
    with pytest.raises(IndexError):
        expected.iat[N + 10, 0] = 1
    with pytest.raises(IndexError):
        got.iat[N + 10, 0] = 1
    check_df(got, expected)


# ----------------------------------------------------------------------
# whole rows
# ----------------------------------------------------------------------
@pytest.mark.parametrize("row", [0, CHUNK, N - 1])
def test_df_loc_sets_a_whole_row_from_a_list(row):
    got = make_df()
    expected = snapshot(got)
    got.loc[row] = [1, 2.0, 3]
    expected.loc[row] = [1, 2.0, 3]
    check_df(got, expected)


@pytest.mark.parametrize("row", [0, CHUNK + 1, N - 1])
def test_df_iloc_sets_a_whole_row_from_a_list(row):
    got = make_df()
    expected = snapshot(got)
    got.iloc[row] = [4, 5.0, 6]
    expected.iloc[row] = [4, 5.0, 6]
    check_df(got, expected)


def test_df_loc_sets_a_whole_row_from_a_series():
    got = make_df()
    expected = snapshot(got)
    row = pd.Series({"a": 8, "b": 9.0, "c": 10})
    row_slow = row._fsproxy_slow.copy()
    got.loc[CHUNK] = row
    expected.loc[CHUNK] = row_slow
    check_df(got, expected)


# ----------------------------------------------------------------------
# slices
# ----------------------------------------------------------------------
def test_df_iloc_sets_a_row_slice_of_one_column():
    got = make_df()
    expected = snapshot(got)
    got.iloc[1:3, 0] = -1
    expected.iloc[1:3, 0] = -1
    check_df(got, expected)


def test_df_iloc_sets_a_row_slice_that_spans_several_chunks():
    got = make_df()
    expected = snapshot(got)
    lo, hi = CHUNK - 5, 3 * CHUNK + 5
    got.iloc[lo:hi, 0] = -2
    expected.iloc[lo:hi, 0] = -2
    check_df(got, expected)


def test_df_iloc_sets_a_row_slice_from_a_list():
    got = make_df()
    expected = snapshot(got)
    got.iloc[1:4, 0] = [10, 20, 30]
    expected.iloc[1:4, 0] = [10, 20, 30]
    check_df(got, expected)


def test_df_iloc_sets_a_chunk_sized_slice_from_an_array():
    got = make_df()
    expected = snapshot(got)
    values = np.arange(CHUNK, dtype="int64") * -1
    got.iloc[CHUNK : 2 * CHUNK, 0] = values
    expected.iloc[CHUNK : 2 * CHUNK, 0] = values
    check_df(got, expected)


def test_df_iloc_sets_a_whole_column_by_position():
    got = make_df()
    expected = snapshot(got)
    got.iloc[:, 0] = 4
    expected.iloc[:, 0] = 4
    check_df(got, expected)


def test_df_iloc_sets_a_block_of_rows_and_columns():
    got = make_df()
    expected = snapshot(got)
    got.iloc[1:3, 0:2] = 0
    expected.iloc[1:3, 0:2] = 0
    check_df(got, expected)


def test_df_loc_sets_a_whole_column_with_a_full_slice():
    got = make_df()
    expected = snapshot(got)
    got.loc[:, "a"] = 3
    expected.loc[:, "a"] = 3
    check_df(got, expected)


def test_series_iloc_sets_a_slice():
    got = make_series()
    expected = snapshot(got)
    got.iloc[CHUNK - 2 : CHUNK + 2] = 8.0
    expected.iloc[CHUNK - 2 : CHUNK + 2] = 8.0
    check_series(got, expected)


def test_series_slice_assignment_from_an_array():
    got = make_series()
    expected = snapshot(got)
    values = np.arange(10, dtype="float64") * -1
    got.iloc[10:20] = values
    expected.iloc[10:20] = values
    check_series(got, expected)


def test_df_iloc_slice_with_a_wrong_length_list_raises():
    got = make_df()
    expected = snapshot(got)
    with pytest.raises(ValueError):
        expected.iloc[1:4, 0] = [1, 2]
    with pytest.raises(ValueError):
        got.iloc[1:4, 0] = [1, 2]
    check_df(got, expected)


# ----------------------------------------------------------------------
# boolean masks
# ----------------------------------------------------------------------
def test_df_loc_mask_sets_one_column():
    got = make_df()
    expected = snapshot(got)
    got.loc[got["c"] == 3, "a"] = -5
    expected.loc[expected["c"] == 3, "a"] = -5
    check_df(got, expected)


def test_df_loc_mask_sets_several_columns():
    got = make_df()
    expected = snapshot(got)
    got.loc[got["c"] == 3, ["a", "b"]] = 0
    expected.loc[expected["c"] == 3, ["a", "b"]] = 0
    check_df(got, expected)


def test_df_loc_mask_sets_every_column():
    got = make_df()
    expected = snapshot(got)
    got.loc[got["c"] == 3] = 0
    expected.loc[expected["c"] == 3] = 0
    check_df(got, expected)


def test_df_loc_mask_from_a_numpy_array():
    got = make_df()
    expected = snapshot(got)
    mask = (np.arange(N) % CHUNK) == 0
    got.loc[mask, "a"] = -3
    expected.loc[mask, "a"] = -3
    check_df(got, expected)


def test_df_loc_mask_from_an_unrelated_series():
    got = make_df()
    expected = snapshot(got)
    keys = pd.Series(np.arange(N, dtype="int64") % 5)
    keys_slow = keys._fsproxy_slow.copy()
    got.loc[keys == 2, "a"] = -9
    expected.loc[keys_slow == 2, "a"] = -9
    check_df(got, expected)


def test_df_loc_mask_that_selects_nothing_changes_nothing():
    got = make_df()
    expected = snapshot(got)
    got.loc[got["c"] > 100, "a"] = -1
    expected.loc[expected["c"] > 100, "a"] = -1
    check_df(got, expected)


def test_df_loc_mask_that_selects_everything():
    got = make_df()
    expected = snapshot(got)
    got.loc[got["c"] >= 0, "a"] = -1
    expected.loc[expected["c"] >= 0, "a"] = -1
    check_df(got, expected)


def test_df_setitem_with_a_row_mask():
    got = make_df()
    expected = snapshot(got)
    got[got["c"] == 3] = 0
    expected[expected["c"] == 3] = 0
    check_df(got, expected)


def test_df_setitem_with_an_elementwise_mask():
    got = make_df()
    expected = snapshot(got)
    got[got > N // 2] = 0
    expected[expected > N // 2] = 0
    check_df(got, expected)


def test_df_loc_mask_sets_a_string_column():
    got = make_str_df()
    expected = snapshot(got)
    got.loc[got["c"] == 3, "k"] = "zz"
    expected.loc[expected["c"] == 3, "k"] = "zz"
    check_df(got, expected)


def test_series_mask_assignment():
    got = make_series()
    expected = snapshot(got)
    got[got > N - 100] = 0.0
    expected[expected > N - 100] = 0.0
    check_series(got, expected)


def test_series_loc_mask_assignment():
    got = make_series()
    expected = snapshot(got)
    got.loc[got > N - 100] = 0.0
    expected.loc[expected > N - 100] = 0.0
    check_series(got, expected)


def test_series_mask_assignment_from_a_series():
    got = make_series()
    expected = snapshot(got)
    got[got > N - 100] = got * -1
    expected[expected > N - 100] = expected * -1
    check_series(got, expected)


# ----------------------------------------------------------------------
# right-hand side is a Series (pandas aligns on the index, not on position)
# ----------------------------------------------------------------------
def test_column_assignment_from_an_independent_series():
    got = make_df()
    expected = snapshot(got)
    rhs = pd.Series(np.arange(N, dtype="float64") * 3)
    rhs_slow = rhs._fsproxy_slow.copy()
    got["b"] = rhs
    expected["b"] = rhs_slow
    check_df(got, expected)


def test_column_assignment_aligns_on_the_index_not_the_position():
    got = make_df()
    expected = snapshot(got)
    rhs = pd.Series(np.arange(N, dtype="float64"), index=np.arange(N)[::-1])
    rhs_slow = rhs._fsproxy_slow.copy()
    got["b"] = rhs
    expected["b"] = rhs_slow
    check_df(got, expected)


def test_column_assignment_from_a_sorted_view_of_itself():
    # pandas re-aligns on the index, so the column comes back unchanged.
    got = make_df()
    expected = snapshot(got)
    got["b"] = got["b"].sort_values(ascending=False)
    expected["b"] = expected["b"].sort_values(ascending=False)
    check_df(got, expected)


def test_column_assignment_from_a_shorter_series_fills_nan():
    got = make_df()
    expected = snapshot(got)
    got["z"] = got["b"].head(10)
    expected["z"] = expected["b"].head(10)
    check_df(got, expected)


def test_loc_mask_assignment_from_a_series():
    got = make_df()
    expected = snapshot(got)
    got.loc[got["c"] == 3, "b"] = got["b"] * 10
    expected.loc[expected["c"] == 3, "b"] = expected["b"] * 10
    check_df(got, expected)


def test_column_assignment_from_a_wrong_length_array_raises():
    got = make_df()
    expected = snapshot(got)
    values = np.arange(CHUNK, dtype="int64")
    with pytest.raises(ValueError):
        expected["a"] = values
    with pytest.raises(ValueError):
        got["a"] = values
    check_df(got, expected)


def test_column_assignment_from_a_wrong_length_list_raises():
    got = make_df()
    expected = snapshot(got)
    values = list(range(CHUNK))
    with pytest.raises(ValueError):
        expected["a"] = values
    with pytest.raises(ValueError):
        got["a"] = values
    check_df(got, expected)


def test_loc_full_slice_assignment_from_a_wrong_length_array_raises():
    got = make_df()
    expected = snapshot(got)
    values = np.arange(CHUNK, dtype="int64")
    with pytest.raises(ValueError):
        expected.loc[:, "a"] = values
    with pytest.raises(ValueError):
        got.loc[:, "a"] = values
    check_df(got, expected)


def test_series_slice_assignment_from_a_wrong_length_array_raises():
    got = make_series()
    expected = snapshot(got)
    values = np.arange(CHUNK, dtype="float64")
    with pytest.raises(ValueError):
        expected.iloc[:] = values
    with pytest.raises(ValueError):
        got.iloc[:] = values
    check_series(got, expected)


# ----------------------------------------------------------------------
# Series element assignment
# ----------------------------------------------------------------------
@pytest.mark.parametrize("row", ROWS)
def test_series_loc_sets_one_element(row):
    got = make_series()
    expected = snapshot(got)
    got.loc[row] = -1.0
    expected.loc[row] = -1.0
    check_series(got, expected)


@pytest.mark.parametrize("row", ROWS)
def test_series_iloc_sets_one_element(row):
    got = make_series()
    expected = snapshot(got)
    got.iloc[row] = -2.0
    expected.iloc[row] = -2.0
    check_series(got, expected)


@pytest.mark.parametrize("row", ROWS)
def test_series_setitem_sets_one_element(row):
    got = make_series()
    expected = snapshot(got)
    got[row] = -3.0
    expected[row] = -3.0
    check_series(got, expected)


def test_series_at_sets_one_element():
    got = make_series()
    expected = snapshot(got)
    got.at[N - 1] = -4.0
    expected.at[N - 1] = -4.0
    check_series(got, expected)


def test_series_iat_sets_one_element():
    got = make_series()
    expected = snapshot(got)
    got.iat[N - 1] = -5.0
    expected.iat[N - 1] = -5.0
    check_series(got, expected)


def test_series_iloc_sets_one_element_by_negative_position():
    got = make_series()
    expected = snapshot(got)
    got.iloc[-1] = -6.0
    expected.iloc[-1] = -6.0
    check_series(got, expected)


def test_series_loc_sets_one_element_on_a_non_default_index():
    got = make_series(index=labelled_index())
    expected = snapshot(got)
    got.loc[(N - 1) * 10] = -7.0
    expected.loc[(N - 1) * 10] = -7.0
    check_series(got, expected)


def test_series_element_write_is_readable_back_through_the_proxy():
    got = make_series()
    snapshot(got)
    got.iloc[N - 1] = -8.0
    assert got.iloc[N - 1] == -8.0
    assert got.iat[N - 1] == -8.0


# ----------------------------------------------------------------------
# enlargement
# ----------------------------------------------------------------------
def test_df_loc_adds_a_row():
    got = make_df()
    expected = snapshot(got)
    got.loc[N] = [1, 2.0, 3]
    expected.loc[N] = [1, 2.0, 3]
    check_df(got, expected)


def test_df_loc_adds_a_row_by_setting_one_cell():
    got = make_df()
    expected = snapshot(got)
    got.loc[N, "a"] = 1234
    expected.loc[N, "a"] = 1234
    check_df(got, expected)


def test_df_at_adds_a_row():
    got = make_df()
    expected = snapshot(got)
    got.at[N, "a"] = 55
    expected.at[N, "a"] = 55
    check_df(got, expected)


def test_df_at_adds_a_column():
    got = make_df()
    expected = snapshot(got)
    got.at[0, "new"] = 2.0
    expected.at[0, "new"] = 2.0
    check_df(got, expected)


def test_df_loc_adds_a_column():
    got = make_df()
    expected = snapshot(got)
    got.loc[0, "new"] = 2.0
    expected.loc[0, "new"] = 2.0
    check_df(got, expected)


def test_df_at_adds_a_column_then_fills_it():
    got = make_df()
    expected = snapshot(got)
    for frame in (got, expected):
        frame.at[0, "new"] = 1.0
        frame.at[N - 1, "new"] = 2.0
    check_df(got, expected)


def test_series_loc_adds_an_element():
    got = make_series()
    expected = snapshot(got)
    got.loc[N] = 3.0
    expected.loc[N] = 3.0
    check_series(got, expected)


def test_series_at_adds_an_element():
    got = make_series()
    expected = snapshot(got)
    got.at[N] = 4.0
    expected.at[N] = 4.0
    check_series(got, expected)


# ----------------------------------------------------------------------
# nulls
# ----------------------------------------------------------------------
def test_df_iloc_sets_nan():
    got = make_df()
    expected = snapshot(got)
    got.iloc[CHUNK + 5, 1] = np.nan
    expected.iloc[CHUNK + 5, 1] = np.nan
    check_df(got, expected)


def test_df_iat_sets_none_in_a_float_column():
    got = make_df()
    expected = snapshot(got)
    got.iat[N - 1, 1] = None
    expected.iat[N - 1, 1] = None
    check_df(got, expected)


def test_df_loc_sets_none_in_an_int_column_and_upcasts():
    got = make_df()
    expected = snapshot(got)
    got.loc[0, "a"] = None
    expected.loc[0, "a"] = None
    check_df(got, expected)


def test_df_loc_mask_sets_nan():
    got = make_df()
    expected = snapshot(got)
    got.loc[got["c"] == 5, "b"] = np.nan
    expected.loc[expected["c"] == 5, "b"] = np.nan
    check_df(got, expected)


def test_df_column_set_to_none_entirely():
    got = make_df()
    expected = snapshot(got)
    got["b"] = None
    expected["b"] = None
    check_df(got, expected)


def test_series_element_set_to_nan():
    got = make_series()
    expected = snapshot(got)
    got.iloc[N - 1] = np.nan
    expected.iloc[N - 1] = np.nan
    check_series(got, expected)


def test_str_column_cell_set_to_none():
    got = make_str_df()
    expected = snapshot(got)
    got.iloc[N - 1, 0] = None
    expected.iloc[N - 1, 0] = None
    check_df(got, expected)


# ----------------------------------------------------------------------
# a write must not leak into (or out of) a copy
# ----------------------------------------------------------------------
def test_copy_is_unaffected_by_a_write_to_the_original():
    got = make_df()
    expected = snapshot(got)
    got_copy, expected_copy = got.copy(), expected.copy()
    got.iat[0, 0] = 999
    expected.iat[0, 0] = 999
    check_df(got, expected)
    check_df(got_copy, expected_copy)


def test_original_is_unaffected_by_a_write_to_a_copy():
    got = make_df()
    expected = snapshot(got)
    got_copy, expected_copy = got.copy(), expected.copy()
    got_copy.iat[0, 0] = 999
    expected_copy.iat[0, 0] = 999
    check_df(got, expected)
    check_df(got_copy, expected_copy)


def test_column_replacement_does_not_disturb_an_earlier_reference():
    got = make_df()
    expected = snapshot(got)
    got_col, expected_col = got["a"], expected["a"]
    got["a"] = got["a"] + 100
    expected["a"] = expected["a"] + 100
    check_df(got, expected)
    check_series(got_col, expected_col)


def test_write_to_a_filtered_frame_leaves_its_parent_alone():
    got = make_df()
    expected = snapshot(got)
    got_sub = got[got["c"] == 3]
    expected_sub = expected[expected["c"] == 3]
    got_sub.iat[0, 0] = -1
    expected_sub.iat[0, 0] = -1
    check_df(got_sub, expected_sub)
    check_df(got, expected)


# ----------------------------------------------------------------------
# frames whose partitioning is not the plain "row i lives in chunk i//CHUNK"
# ----------------------------------------------------------------------
def test_loc_write_hits_every_row_sharing_a_duplicated_label():
    # the two rows labelled 7 are half a frame apart, so they live on
    # different GPUs
    index = np.arange(N, dtype="int64") % (N // 2)
    got = make_df(index=index)
    expected = snapshot(got)
    got.loc[7, "a"] = -1
    expected.loc[7, "a"] = -1
    check_df(got, expected)


def test_mask_write_on_a_filtered_frame():
    got = make_df()
    expected = snapshot(got)
    got_sub = got[got["c"] == 3]
    expected_sub = expected[expected["c"] == 3]
    got_sub.loc[got_sub["a"] > N // 2, "b"] = -1.0
    expected_sub.loc[expected_sub["a"] > N // 2, "b"] = -1.0
    check_df(got_sub, expected_sub)


def test_cell_write_on_a_reordered_frame():
    got = make_df().sort_values("a", ascending=False)
    expected = snapshot(got)
    got.iat[0, 0] = -1
    expected.iat[0, 0] = -1
    check_df(got, expected)


def test_column_assignment_from_a_filtered_column_aligns():
    got = make_df()
    expected = snapshot(got)
    got["z"] = got.loc[got["c"] == 3, "b"]
    expected["z"] = expected.loc[expected["c"] == 3, "b"]
    check_df(got, expected)
