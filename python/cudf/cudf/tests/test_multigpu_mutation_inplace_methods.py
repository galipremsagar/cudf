# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""``inplace=True`` under the multi-GPU ``cudf.pandas`` backend.

Every distributed operation in ``cudf.multigpu`` builds a *new* chunked frame,
so ``inplace=True`` can only work if that result is adopted back into the
object the caller is holding.  When it is not, the write is lost and nothing
raises -- the failure mode these tests exist to catch.

pandas is the oracle: the same mutation is run on a real pandas copy taken
from ``obj._fsproxy_slow`` and the two are compared.  Each test asserts three
things, because any one of them alone can pass while the backend is wrong:

* the call returned ``None`` (what pandas' ``inplace=True`` returns),
* the receiving object now equals the pandas result,
* the oracle actually moved, so the comparison is not vacuous.

The non-inplace form of each method gets its own test asserting the *opposite*:
the receiver must be untouched.
"""

from __future__ import annotations

import pytest

pytest.importorskip("cudf.multigpu")

import cudf.multigpu.pandas_compat as _pandas_compat  # noqa: E402

if not _pandas_compat.is_installed():
    import warnings

    with warnings.catch_warnings():
        # install() warns that the backend is experimental; this module's
        # pyproject turns warnings into errors, which would abort collection.
        warnings.simplefilter("ignore")
        _pandas_compat.install(
            initial_pool_fraction=0.05, max_pool_fraction=0.30
        )

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402  -- the multi-GPU-backed pandas

# The backend warns loudly when an operation has no distributed implementation
# and the frame has to be gathered onto one GPU, and numba warns about small
# grids inside .query().  The repo default (`filterwarnings = ["error"]`) would
# turn those into exceptions, which is exactly what hides a silent lost write:
# the test would "fail" on the warning long before it could compare values.
pytestmark = pytest.mark.filterwarnings("ignore")

N_ROWS = 4096


# ----------------------------------------------------------------------
# fixtures / helpers
# ----------------------------------------------------------------------
def frame(seed: int = 0, n: int = N_ROWS):
    """An all-numeric frame big enough to be spread over every GPU."""
    rng = np.random.default_rng(seed)
    return pd.DataFrame(
        {
            "a": np.arange(n, dtype="int64"),
            "b": rng.normal(size=n),
            "c": rng.integers(0, 7, n).astype("int64"),
        }
    )


def nan_frame(seed: int = 1, n: int = N_ROWS):
    """Like :func:`frame` but with holes in two float columns.

    ``b[0]`` is null on purpose: forward-filling can never repair the first
    row, so ffill and bfill give measurably different answers.
    """
    rng = np.random.default_rng(seed)
    b = rng.normal(size=n)
    b[::10] = np.nan
    d = rng.normal(size=n)
    d[3::7] = np.nan
    return pd.DataFrame(
        {"a": np.arange(n, dtype="int64"), "b": b, "d": d}
    )


def oracle(obj):
    """Two detached real-pandas copies of ``obj``; ``obj`` stays on the GPUs.

    Reading ``_fsproxy_slow`` moves the proxy into its pandas state, so the
    read of ``_fsproxy_fast`` afterwards is load-bearing: it puts the object
    back on the GPUs *and* proves the frame really is partitioned, without
    which none of this would exercise the multi-GPU path at all.
    """
    slow = obj._fsproxy_slow
    expected, before = slow.copy(), slow.copy()
    nchunks = obj._fsproxy_fast.nchunks
    assert nchunks > 1, f"frame lives in {nchunks} chunk(s); test is vacuous"
    return expected, before


def check_frame(got, expected, before=None):
    if before is not None:
        assert not expected.equals(before), (
            "the pandas oracle did not change, so this test proves nothing"
        )
    pd.testing.assert_frame_equal(got._fsproxy_slow, expected)


def check_series(got, expected, before=None):
    if before is not None:
        assert not expected.equals(before), (
            "the pandas oracle did not change, so this test proves nothing"
        )
    pd.testing.assert_series_equal(got._fsproxy_slow, expected)


# ----------------------------------------------------------------------
# sanity: the fixtures really are distributed
# ----------------------------------------------------------------------
def test_fixture_frame_is_partitioned_over_several_chunks():
    df = frame()
    assert df._fsproxy_fast.nchunks > 1
    assert len(set(df._fsproxy_fast.devices)) > 1


def test_fixture_series_is_partitioned_over_several_chunks():
    s = frame()["b"]
    assert s._fsproxy_fast.nchunks > 1


# ----------------------------------------------------------------------
# fillna
# ----------------------------------------------------------------------
def test_dataframe_fillna_inplace_mutates_and_returns_none():
    got = nan_frame()
    expected, before = oracle(got)

    assert got.fillna(0.0, inplace=True) is None
    expected.fillna(0.0, inplace=True)

    check_frame(got, expected, before)


def test_series_fillna_inplace_mutates_and_returns_none():
    got = nan_frame()["b"]
    expected, before = oracle(got)

    assert got.fillna(-1.0, inplace=True) is None
    expected.fillna(-1.0, inplace=True)

    check_series(got, expected, before)


def test_dataframe_fillna_without_inplace_leaves_receiver_alone():
    got = nan_frame()
    _, before = oracle(got)

    out = got.fillna(0.0)

    assert out is not None
    assert not out._fsproxy_slow.equals(before)
    check_frame(got, before)


def test_series_fillna_without_inplace_leaves_receiver_alone():
    got = nan_frame()["b"]
    _, before = oracle(got)

    out = got.fillna(-1.0)

    assert not out._fsproxy_slow.equals(before)
    check_series(got, before)


# ----------------------------------------------------------------------
# dropna
# ----------------------------------------------------------------------
def test_dataframe_dropna_inplace_mutates_and_returns_none():
    got = nan_frame()
    expected, before = oracle(got)

    assert got.dropna(inplace=True) is None
    expected.dropna(inplace=True)

    check_frame(got, expected, before)


def test_dataframe_dropna_subset_inplace_mutates_and_returns_none():
    got = nan_frame()
    expected, before = oracle(got)

    assert got.dropna(subset=["b"], inplace=True) is None
    expected.dropna(subset=["b"], inplace=True)

    check_frame(got, expected, before)


def test_series_dropna_inplace_mutates_and_returns_none():
    got = nan_frame()["b"]
    expected, before = oracle(got)

    assert got.dropna(inplace=True) is None
    expected.dropna(inplace=True)

    check_series(got, expected, before)


def test_dataframe_dropna_without_inplace_leaves_receiver_alone():
    got = nan_frame()
    _, before = oracle(got)

    out = got.dropna()

    assert len(out) < len(before)
    check_frame(got, before)


# ----------------------------------------------------------------------
# drop
# ----------------------------------------------------------------------
def test_dataframe_drop_columns_inplace_mutates_and_returns_none():
    got = frame()
    expected, before = oracle(got)

    assert got.drop(columns=["c"], inplace=True) is None
    expected.drop(columns=["c"], inplace=True)

    check_frame(got, expected, before)


def test_dataframe_drop_rows_inplace_mutates_and_returns_none():
    got = frame()
    expected, before = oracle(got)
    labels = [0, 1, 2, 1000, 2000, N_ROWS - 1]

    assert got.drop(index=labels, inplace=True) is None
    expected.drop(index=labels, inplace=True)

    check_frame(got, expected, before)


def test_series_drop_labels_inplace_mutates_and_returns_none():
    got = frame()["b"]
    expected, before = oracle(got)
    labels = [0, 1, 2, 1000, 2000, N_ROWS - 1]

    assert got.drop(labels, inplace=True) is None
    expected.drop(labels, inplace=True)

    check_series(got, expected, before)


def test_dataframe_drop_without_inplace_leaves_receiver_alone():
    got = frame()
    _, before = oracle(got)

    out = got.drop(columns=["c"])

    assert list(out._fsproxy_slow.columns) == ["a", "b"]
    check_frame(got, before)


def test_series_drop_without_inplace_leaves_receiver_alone():
    got = frame()["b"]
    _, before = oracle(got)

    out = got.drop([0, 1, 2])

    assert len(out) == len(before) - 3
    check_series(got, before)


# ----------------------------------------------------------------------
# rename
# ----------------------------------------------------------------------
def test_dataframe_rename_columns_inplace_mutates_and_returns_none():
    got = frame()
    expected, before = oracle(got)

    assert got.rename(columns={"a": "A", "b": "B"}, inplace=True) is None
    expected.rename(columns={"a": "A", "b": "B"}, inplace=True)

    assert list(got._fsproxy_slow.columns) == ["A", "B", "c"]
    check_frame(got, expected, before)


def test_dataframe_rename_index_inplace_mutates_and_returns_none():
    got = frame()
    expected, before = oracle(got)

    assert got.rename(index={0: 9999}, inplace=True) is None
    expected.rename(index={0: 9999}, inplace=True)

    check_frame(got, expected, before)


def test_series_rename_inplace_mutates_and_returns_what_pandas_returns():
    got = frame()["b"]
    expected, before = oracle(got)
    assert before.name == "b"

    # pandas 3 renames a Series with a scalar through ``Series._set_name``,
    # which returns the receiver rather than None (the mapper form still
    # returns None). Ask the oracle rather than hardcoding either answer.
    got_ret = got.rename("renamed", inplace=True)
    expected_ret = expected.rename("renamed", inplace=True)
    assert (got_ret is None) == (expected_ret is None)

    # ``Series.equals`` ignores the name, so the explicit check is the one
    # that makes this test non-vacuous; assert_series_equal compares names.
    assert got._fsproxy_slow.name == "renamed"
    check_series(got, expected)


def test_dataframe_rename_without_inplace_leaves_receiver_alone():
    got = frame()
    _, before = oracle(got)

    out = got.rename(columns={"a": "A"})

    assert list(out._fsproxy_slow.columns) == ["A", "b", "c"]
    check_frame(got, before)


# ----------------------------------------------------------------------
# reset_index
# ----------------------------------------------------------------------
def test_dataframe_reset_index_drop_inplace_mutates_and_returns_none():
    got = frame().sort_values("b")
    expected, before = oracle(got)

    assert got.reset_index(drop=True, inplace=True) is None
    expected.reset_index(drop=True, inplace=True)

    check_frame(got, expected, before)


def test_dataframe_reset_index_keeping_index_inplace_mutates_and_returns_none():
    got = frame().sort_values("b")
    expected, before = oracle(got)

    assert got.reset_index(inplace=True) is None
    expected.reset_index(inplace=True)

    assert list(got._fsproxy_slow.columns) == ["index", "a", "b", "c"]
    check_frame(got, expected, before)


def test_dataframe_reset_index_of_named_index_inplace_mutates_and_returns_none():
    got = frame()
    got.index = got.index.rename("row")
    expected, before = oracle(got)

    assert got.reset_index(inplace=True) is None
    expected.reset_index(inplace=True)

    assert list(got._fsproxy_slow.columns) == ["row", "a", "b", "c"]
    check_frame(got, expected, before)


def test_series_reset_index_drop_inplace_mutates_and_returns_none():
    got = frame()["b"].sort_values()
    expected, before = oracle(got)

    assert got.reset_index(drop=True, inplace=True) is None
    expected.reset_index(drop=True, inplace=True)

    check_series(got, expected, before)


def test_series_reset_index_inplace_without_drop_raises_like_pandas():
    got = frame()["b"]
    expected, _ = oracle(got)

    with pytest.raises(TypeError):
        expected.reset_index(inplace=True)
    with pytest.raises(TypeError):
        got.reset_index(inplace=True)


def test_dataframe_reset_index_without_inplace_leaves_receiver_alone():
    got = frame().sort_values("b")
    _, before = oracle(got)

    out = got.reset_index(drop=True)

    assert out._fsproxy_slow.index.tolist() == list(range(N_ROWS))
    check_frame(got, before)


# ----------------------------------------------------------------------
# set_index
# ----------------------------------------------------------------------
def test_dataframe_set_index_inplace_mutates_and_returns_none():
    got = frame()
    expected, before = oracle(got)

    assert got.set_index("a", inplace=True) is None
    expected.set_index("a", inplace=True)

    check_frame(got, expected, before)


def test_dataframe_set_index_inplace_preserves_row_order():
    got = frame()
    expected, _ = oracle(got)

    got.set_index("a", inplace=True)
    expected.set_index("a", inplace=True)

    assert got._fsproxy_slow.index.tolist() == expected.index.tolist()


def test_dataframe_set_index_without_inplace_leaves_receiver_alone():
    got = frame()
    _, before = oracle(got)

    out = got.set_index("a")

    assert "a" not in out._fsproxy_slow.columns
    check_frame(got, before)


# ----------------------------------------------------------------------
# sort_values
# ----------------------------------------------------------------------
def test_dataframe_sort_values_inplace_mutates_and_returns_none():
    got = frame()
    expected, before = oracle(got)

    assert got.sort_values("b", inplace=True) is None
    expected.sort_values("b", inplace=True)

    check_frame(got, expected, before)


def test_dataframe_sort_values_descending_inplace_mutates_and_returns_none():
    got = frame()
    expected, before = oracle(got)

    assert got.sort_values("b", ascending=False, inplace=True) is None
    expected.sort_values("b", ascending=False, inplace=True)

    check_frame(got, expected, before)


def test_series_sort_values_inplace_mutates_and_returns_none():
    got = frame()["b"]
    expected, before = oracle(got)

    assert got.sort_values(inplace=True) is None
    expected.sort_values(inplace=True)

    check_series(got, expected, before)


def test_dataframe_sort_values_without_inplace_leaves_receiver_alone():
    got = frame()
    _, before = oracle(got)

    out = got.sort_values("b")

    assert out._fsproxy_slow["b"].is_monotonic_increasing
    check_frame(got, before)


def test_series_sort_values_without_inplace_leaves_receiver_alone():
    got = frame()["b"]
    _, before = oracle(got)

    out = got.sort_values()

    assert out._fsproxy_slow.is_monotonic_increasing
    check_series(got, before)


# ----------------------------------------------------------------------
# sort_index
# ----------------------------------------------------------------------
def test_dataframe_sort_index_inplace_mutates_and_returns_none():
    got = frame().sort_values("b")
    expected, before = oracle(got)

    assert got.sort_index(inplace=True) is None
    expected.sort_index(inplace=True)

    check_frame(got, expected, before)


def test_dataframe_sort_index_descending_inplace_mutates_and_returns_none():
    got = frame()
    expected, before = oracle(got)

    assert got.sort_index(ascending=False, inplace=True) is None
    expected.sort_index(ascending=False, inplace=True)

    check_frame(got, expected, before)


def test_series_sort_index_inplace_mutates_and_returns_none():
    got = frame()["b"].sort_values()
    expected, before = oracle(got)

    assert got.sort_index(inplace=True) is None
    expected.sort_index(inplace=True)

    check_series(got, expected, before)


def test_dataframe_sort_index_without_inplace_leaves_receiver_alone():
    got = frame().sort_values("b")
    _, before = oracle(got)

    out = got.sort_index()

    assert out._fsproxy_slow.index.is_monotonic_increasing
    check_frame(got, before)


# ----------------------------------------------------------------------
# replace
# ----------------------------------------------------------------------
def test_dataframe_replace_inplace_mutates_and_returns_none():
    got = frame()
    expected, before = oracle(got)

    assert got.replace({0: -1}, inplace=True) is None
    expected.replace({0: -1}, inplace=True)

    check_frame(got, expected, before)


def test_series_replace_inplace_mutates_and_returns_none():
    got = frame()["c"]
    expected, before = oracle(got)

    assert got.replace(0, -1, inplace=True) is None
    expected.replace(0, -1, inplace=True)

    check_series(got, expected, before)


def test_dataframe_replace_without_inplace_leaves_receiver_alone():
    got = frame()
    _, before = oracle(got)

    out = got.replace({0: -1})

    assert not out._fsproxy_slow.equals(before)
    check_frame(got, before)


# ----------------------------------------------------------------------
# drop_duplicates
# ----------------------------------------------------------------------
def test_dataframe_drop_duplicates_inplace_mutates_and_returns_none():
    got = frame()[["c"]]
    expected, before = oracle(got)

    assert got.drop_duplicates(inplace=True) is None
    expected.drop_duplicates(inplace=True)

    check_frame(got, expected, before)


def test_dataframe_drop_duplicates_subset_inplace_mutates_and_returns_none():
    got = frame()
    expected, before = oracle(got)

    assert got.drop_duplicates(subset=["c"], inplace=True) is None
    expected.drop_duplicates(subset=["c"], inplace=True)

    check_frame(got, expected, before)


def test_series_drop_duplicates_inplace_mutates_and_returns_none():
    got = frame()["c"]
    expected, before = oracle(got)

    assert got.drop_duplicates(inplace=True) is None
    expected.drop_duplicates(inplace=True)

    check_series(got, expected, before)


def test_dataframe_drop_duplicates_without_inplace_leaves_receiver_alone():
    got = frame()
    _, before = oracle(got)

    out = got.drop_duplicates(subset=["c"])

    assert len(out) == 7
    check_frame(got, before)


# ----------------------------------------------------------------------
# query
# ----------------------------------------------------------------------
def test_dataframe_query_inplace_mutates_and_returns_none():
    got = frame()
    expected, before = oracle(got)

    assert got.query("c > 3", inplace=True) is None
    expected.query("c > 3", inplace=True)

    check_frame(got, expected, before)


def test_dataframe_query_without_inplace_leaves_receiver_alone():
    got = frame()
    _, before = oracle(got)

    out = got.query("c > 3")

    assert 0 < len(out) < N_ROWS
    check_frame(got, before)


# ----------------------------------------------------------------------
# eval
# ----------------------------------------------------------------------
def test_dataframe_eval_inplace_mutates_and_returns_none():
    got = frame()
    expected, before = oracle(got)

    assert got.eval("z = a + c", inplace=True) is None
    expected.eval("z = a + c", inplace=True)

    assert list(got._fsproxy_slow.columns) == ["a", "b", "c", "z"]
    check_frame(got, expected, before)


def test_dataframe_eval_without_inplace_leaves_receiver_alone():
    got = frame()
    _, before = oracle(got)

    out = got.eval("z = a + c")

    assert "z" in out._fsproxy_slow.columns
    check_frame(got, before)


# ----------------------------------------------------------------------
# clip
# ----------------------------------------------------------------------
def test_dataframe_clip_inplace_mutates_and_returns_none():
    got = frame()
    expected, before = oracle(got)

    assert got.clip(lower=-1, upper=1, inplace=True) is None
    expected.clip(lower=-1, upper=1, inplace=True)

    check_frame(got, expected, before)


def test_series_clip_inplace_mutates_and_returns_none():
    got = frame()["b"]
    expected, before = oracle(got)

    assert got.clip(-0.5, 0.5, inplace=True) is None
    expected.clip(-0.5, 0.5, inplace=True)

    check_series(got, expected, before)


def test_dataframe_clip_without_inplace_leaves_receiver_alone():
    got = frame()
    _, before = oracle(got)

    out = got.clip(lower=-1, upper=1)

    assert not out._fsproxy_slow.equals(before)
    check_frame(got, before)


# ----------------------------------------------------------------------
# rename_axis
# ----------------------------------------------------------------------
def test_dataframe_rename_axis_inplace_mutates_and_returns_none():
    got = frame()
    expected, before = oracle(got)
    assert before.index.name is None

    assert got.rename_axis("row", inplace=True) is None
    expected.rename_axis("row", inplace=True)

    # ``DataFrame.equals`` ignores index names, so assert the rename directly.
    assert got._fsproxy_slow.index.name == "row"
    check_frame(got, expected)


def test_series_rename_axis_inplace_mutates_and_returns_none():
    got = frame()["b"]
    expected, before = oracle(got)
    assert before.index.name is None

    assert got.rename_axis("row", inplace=True) is None
    expected.rename_axis("row", inplace=True)

    assert got._fsproxy_slow.index.name == "row"
    check_series(got, expected)


def test_dataframe_rename_axis_without_inplace_leaves_receiver_alone():
    got = frame()
    _, before = oracle(got)

    out = got.rename_axis("row")

    assert out._fsproxy_slow.index.name == "row"
    assert got._fsproxy_slow.index.name is None
    check_frame(got, before)


# ----------------------------------------------------------------------
# ffill / bfill
# ----------------------------------------------------------------------
@pytest.mark.parametrize("how", ["ffill", "bfill"])
def test_dataframe_fill_direction_inplace_returns_the_receiver(how):
    """pandas 3 returns the receiver here, not None.

    ``NDFrame._pad_or_backfill`` ends in ``self._update_inplace(result);
    return self``, so ``is None`` is the pandas-2 contract. Identity is not
    assertable through cudf.pandas -- every returned frame is re-wrapped in
    a fresh proxy -- so compare values instead.
    """
    got = nan_frame()

    out = getattr(got, how)(inplace=True)

    assert out is not None
    pd.testing.assert_frame_equal(out._fsproxy_slow, got._fsproxy_slow)


@pytest.mark.parametrize("how", ["ffill", "bfill"])
def test_dataframe_fill_direction_inplace_mutates(how):
    got = nan_frame()
    expected, before = oracle(got)

    getattr(got, how)(inplace=True)
    getattr(expected, how)(inplace=True)

    check_frame(got, expected, before)


@pytest.mark.parametrize("how", ["ffill", "bfill"])
def test_series_fill_direction_inplace_returns_the_receiver(how):
    got = nan_frame()["b"]

    out = getattr(got, how)(inplace=True)

    assert out is not None
    pd.testing.assert_series_equal(out._fsproxy_slow, got._fsproxy_slow)


@pytest.mark.parametrize("how", ["ffill", "bfill"])
def test_series_fill_direction_inplace_mutates(how):
    got = nan_frame()["b"]
    expected, before = oracle(got)

    getattr(got, how)(inplace=True)
    getattr(expected, how)(inplace=True)

    check_series(got, expected, before)


@pytest.mark.parametrize("how", ["ffill", "bfill"])
def test_dataframe_fill_direction_without_inplace_leaves_receiver_alone(how):
    got = nan_frame()
    _, before = oracle(got)

    out = getattr(got, how)()

    assert out._fsproxy_slow.isna().sum().sum() < before.isna().sum().sum()
    check_frame(got, before)


# ----------------------------------------------------------------------
# interpolate -- same "adopt the result" contract as ffill/bfill
# ----------------------------------------------------------------------
def test_dataframe_interpolate_inplace_returns_the_receiver():
    """pandas 3's ``interpolate(inplace=True)`` also returns the receiver."""
    got = nan_frame()

    out = got.interpolate(inplace=True)

    assert out is not None
    pd.testing.assert_frame_equal(out._fsproxy_slow, got._fsproxy_slow)


def test_dataframe_interpolate_inplace_mutates():
    """Only that the write lands -- interpolate's *values* are another story.

    Comparing against pandas here would confuse two defects, so this asserts
    the weakest thing that a working ``inplace=True`` must satisfy: the
    receiver has fewer holes than it started with.
    """
    got = nan_frame()
    expected, before = oracle(got)
    expected.interpolate(inplace=True)
    assert expected.isna().sum().sum() < before.isna().sum().sum()

    got.interpolate(inplace=True)

    assert (
        got._fsproxy_slow.isna().sum().sum() < before.isna().sum().sum()
    ), "interpolate(inplace=True) left the receiver untouched"


# ----------------------------------------------------------------------
# where / mask
# ----------------------------------------------------------------------
def test_dataframe_where_inplace_mutates_and_returns_none():
    got = frame()
    expected, before = oracle(got)
    cond = got > 0

    assert got.where(cond, inplace=True) is None
    expected.where(before > 0, inplace=True)

    check_frame(got, expected, before)


def test_dataframe_mask_inplace_mutates_and_returns_none():
    got = frame()
    expected, before = oracle(got)
    cond = got > 0

    assert got.mask(cond, inplace=True) is None
    expected.mask(before > 0, inplace=True)

    check_frame(got, expected, before)


def test_series_where_inplace_mutates_and_returns_none():
    got = frame()["b"]
    expected, before = oracle(got)
    cond = got > 0

    assert got.where(cond, inplace=True) is None
    expected.where(before > 0, inplace=True)

    check_series(got, expected, before)


def test_dataframe_where_without_inplace_leaves_receiver_alone():
    got = frame()
    _, before = oracle(got)

    out = got.where(got > 0)

    assert out._fsproxy_slow.isna().sum().sum() > 0
    check_frame(got, before)


# ----------------------------------------------------------------------
# astype -- pandas has no inplace= here, so the backend must not invent one
# ----------------------------------------------------------------------
def test_dataframe_astype_rejects_inplace_like_pandas():
    got = frame()
    expected, _ = oracle(got)

    with pytest.raises(TypeError):
        expected.astype({"c": "float64"}, inplace=True)
    with pytest.raises(TypeError):
        got.astype({"c": "float64"}, inplace=True)


def test_series_astype_rejects_inplace_like_pandas():
    got = frame()["c"]
    expected, _ = oracle(got)

    with pytest.raises(TypeError):
        expected.astype("float64", inplace=True)
    with pytest.raises(TypeError):
        got.astype("float64", inplace=True)


def test_dataframe_astype_without_inplace_leaves_receiver_alone():
    got = frame()
    _, before = oracle(got)

    out = got.astype({"c": "float64"})

    assert out._fsproxy_slow["c"].dtype == np.dtype("float64")
    assert got._fsproxy_slow["c"].dtype == np.dtype("int64")
    check_frame(got, before)


def test_series_astype_without_inplace_leaves_receiver_alone():
    got = frame()["c"]
    _, before = oracle(got)

    out = got.astype("float64")

    assert out._fsproxy_slow.dtype == np.dtype("float64")
    check_series(got, before)


def test_column_assignment_of_astype_result_changes_dtype_in_place():
    """The supported way to retype a column: assign the cast back."""
    got = frame()
    expected, before = oracle(got)

    got["c"] = got["c"].astype("float64")
    expected["c"] = expected["c"].astype("float64")

    assert got._fsproxy_slow["c"].dtype == np.dtype("float64")
    check_frame(got, expected, before)


# ----------------------------------------------------------------------
# set_axis -- pandas 2 dropped inplace=, the backend must reject it too
# ----------------------------------------------------------------------
def test_dataframe_set_axis_rejects_inplace_like_pandas():
    got = frame()
    expected, _ = oracle(got)

    with pytest.raises(TypeError):
        expected.set_axis(["x", "y", "z"], axis=1, inplace=True)
    with pytest.raises(TypeError):
        got.set_axis(["x", "y", "z"], axis=1, inplace=True)
