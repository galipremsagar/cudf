# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""Aliasing, views and write-back for the multi-GPU ``cudf.pandas`` backend.

A row-partitioned frame hands out derived objects -- columns, slices, joins,
concatenations -- that may or may not share chunks with their source.  pandas
(3.x, copy-on-write) has one answer for each of those relationships, and the
accelerated backend has to give the same one.  The dangerous direction is the
silent one: a write that lands on a shared chunk the caller did not name, or a
write that is dropped while pandas would have complained.

Every expectation here is pinned to a real pandas object obtained through the
proxy (``pd.DataFrame._fsproxy_slow`` builds one directly), and the same
mutation is run on both sides wherever that is possible.
"""

from __future__ import annotations

import functools
import json
import subprocess
import sys
import warnings

import numpy as np
import pytest

pytest.importorskip("cudf.multigpu")

import cudf.multigpu.pandas_compat as _pandas_compat  # noqa: E402

if not _pandas_compat.is_installed():
    with warnings.catch_warnings():
        # installing announces that the backend is experimental; this suite's
        # -W error setting would otherwise turn that into a collection error
        warnings.simplefilter("ignore", UserWarning)
        _pandas_compat.install(
            initial_pool_fraction=0.05, max_pool_fraction=0.30
        )

import pandas as pd  # noqa: E402  -- the multi-GPU-backed pandas

#: real pandas constructors, reached through the proxy types
REAL_DATAFRAME = pd.DataFrame._fsproxy_slow
REAL_CONCAT = pd.concat._fsproxy_slow

#: pandas' own answer to chained assignment, collected from an interpreter that
#: has not imported the accelerator: installing it silences
#: ``ChainedAssignmentError`` for *every* object in the process, genuine pandas
#: frames included, so the reference behaviour cannot be observed in here.
_CHAINED_ASSIGNMENT_ORACLE = r"""
import json
import warnings

import numpy as np
import pandas as pd


def _frame():
    values = np.arange(20, dtype="float64")
    values[::7] = np.nan
    return pd.DataFrame({"a": values})


seen = {}

frame = _frame()
with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter("always")
    frame["a"][0] = 5.0
seen["setitem"] = [w.category.__name__ for w in caught]

frame = _frame()
with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter("always")
    frame["a"].iloc[0] = 5.0
seen["iloc_setitem"] = [w.category.__name__ for w in caught]

frame = _frame()
with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter("always")
    frame["a"].fillna(0.0, inplace=True)
seen["fillna"] = [w.category.__name__ for w in caught]

print(json.dumps(seen))
"""


@functools.lru_cache(maxsize=1)
def _plain_pandas_chained_warnings() -> dict:
    finished = subprocess.run(
        [sys.executable, "-c", _CHAINED_ASSIGNMENT_ORACLE],
        capture_output=True,
        text=True,
        timeout=300,
    )
    if finished.returncode != 0:
        pytest.skip(
            "could not collect the plain-pandas oracle: "
            f"{finished.stderr[-400:]}"
        )
    return json.loads(finished.stdout.strip().splitlines()[-1])


#: big enough that ``from_pandas`` spreads it over every visible GPU
N_ROWS = 4000


def _payload(offset: int = 0) -> dict:
    a = np.arange(N_ROWS, dtype="int64") + offset
    return {
        "a": a,
        "b": a.astype("float64") * 2.0,
        "g": (np.arange(N_ROWS) % 4).astype("int64"),
    }


def _nan_payload() -> dict:
    values = np.arange(N_ROWS, dtype="float64")
    values[::7] = np.nan
    return {"a": values}


def _partitioned(frame):
    """Fail loudly if a test frame ended up on a single GPU."""
    assert frame._fsproxy_fast.nchunks > 1, (
        "test frame was not partitioned; it does not exercise the "
        "multi-GPU path"
    )
    return frame


def _twins(offset: int = 0):
    """A multi-GPU-backed frame and an identical *real* pandas frame."""
    return _partitioned(pd.DataFrame(_payload(offset))), REAL_DATAFRAME(
        _payload(offset)
    )


def _nan_twins():
    return _partitioned(pd.DataFrame(_nan_payload())), REAL_DATAFRAME(
        _nan_payload()
    )


def _slow(obj):
    """The pandas value of a proxy.  Leaves the proxy backed by pandas."""
    return obj._fsproxy_slow


def _snapshot(obj):
    """A host copy of a proxy's value that leaves it GPU-backed."""
    return obj._fsproxy_fast.to_pandas()


def _same_frame(got, expected):
    pd.testing.assert_frame_equal(_slow(got), expected)


def _same_series(got, expected):
    pd.testing.assert_series_equal(_slow(got), expected)


def _canonical(frame):
    """Row order is not defined for a joined multi-GPU frame."""
    return frame.sort_values("b").reset_index(drop=True)


# ----------------------------------------------------------------------
# a column reference: s = df["a"]
# ----------------------------------------------------------------------
def test_write_through_column_reference_does_not_reach_the_frame():
    got, expected = _twins()
    got_col, expected_col = got["a"], expected["a"]

    got_col.iloc[0] = 99
    expected_col.iloc[0] = 99

    _same_series(got_col, expected_col)
    _same_frame(got, expected)


def test_inplace_fillna_on_a_column_reference_does_not_reach_the_frame():
    got, expected = _nan_twins()
    got_col, expected_col = got["a"], expected["a"]

    assert got_col.fillna(0.0, inplace=True) is None
    expected_col.fillna(0.0, inplace=True)

    _same_series(got_col, expected_col)
    _same_frame(got, expected)


def test_replacing_a_column_does_not_reach_an_earlier_reference():
    got, expected = _twins()
    got_col, expected_col = got["a"], expected["a"]

    got["a"] = got["a"] + 100
    expected["a"] = expected["a"] + 100

    _same_series(got_col, expected_col)
    _same_frame(got, expected)


def test_two_references_to_the_same_column_are_independent():
    got, expected = _twins()
    got_first, got_second = got["a"], got["a"]
    expected_first, expected_second = expected["a"], expected["a"]

    got_first.iloc[0] = -1
    expected_first.iloc[0] = -1

    _same_series(got_second, expected_second)
    _same_series(got_first, expected_first)


def test_at_write_on_the_frame_does_not_reach_an_earlier_column_reference():
    got, expected = _twins()
    got_col, expected_col = got["a"], expected["a"]

    got.at[0, "a"] = 999
    expected.at[0, "a"] = 999

    _same_series(got_col, expected_col)
    _same_frame(got, expected)


def test_loc_column_write_does_not_reach_an_earlier_column_reference():
    got, expected = _twins()
    got_col, expected_col = got["a"], expected["a"]

    got.loc[:, "a"] = 0
    expected.loc[:, "a"] = 0

    _same_series(got_col, expected_col)
    _same_frame(got, expected)


def test_renaming_a_column_reference_does_not_rename_the_frame_column():
    got, expected = _twins()
    got_col, expected_col = got["a"], expected["a"]

    got_col.name = "renamed"
    expected_col.name = "renamed"

    assert list(_slow(got).columns) == list(expected.columns)
    _same_series(got_col, expected_col)


# ----------------------------------------------------------------------
# a plain alias: df2 = df
# ----------------------------------------------------------------------
def test_alias_sees_a_column_assignment():
    got, expected = _twins()
    got_alias, expected_alias = got, expected

    got_alias["a"] = 0
    expected_alias["a"] = 0

    _same_frame(got, expected)


def test_alias_sees_drop_inplace_which_returns_none():
    got, expected = _twins()
    got_alias, expected_alias = got, expected

    returned = got_alias.drop(columns=["b"], inplace=True)
    expected_alias.drop(columns=["b"], inplace=True)

    _same_frame(got, expected)
    assert returned is None


def test_alias_sees_insert_which_returns_none():
    got, expected = _twins()
    got_alias, expected_alias = got, expected

    returned = got_alias.insert(0, "z", 7)
    expected_alias.insert(0, "z", 7)

    _same_frame(got, expected)
    assert returned is None


def test_alias_sees_rename_inplace_which_returns_none():
    got, expected = _twins()
    got_alias, expected_alias = got, expected

    returned = got_alias.rename(columns={"a": "A"}, inplace=True)
    expected_alias.rename(columns={"a": "A"}, inplace=True)

    _same_frame(got, expected)
    assert returned is None


def test_alias_sees_an_at_write():
    got, expected = _twins()
    got_alias, expected_alias = got, expected

    got_alias.at[0, "a"] = 999
    expected_alias.at[0, "a"] = 999

    _same_frame(got, expected)


def test_alias_sees_a_loc_column_write():
    got, expected = _twins()
    got_alias, expected_alias = got, expected

    got_alias.loc[:, "a"] = -5
    expected_alias.loc[:, "a"] = -5

    _same_frame(got, expected)


def test_alias_sees_inplace_arithmetic():
    got, expected = _twins()
    got_alias, expected_alias = got, expected

    got_alias += 1
    expected_alias += 1

    _same_frame(got, expected)


# ----------------------------------------------------------------------
# copies must be detached in both directions
# ----------------------------------------------------------------------
def test_mutating_a_copy_leaves_the_original_alone():
    got, expected = _twins()
    got_copy, expected_copy = got.copy(), expected.copy()

    got_copy["a"] = -1
    expected_copy["a"] = -1

    _same_frame(got, expected)
    _same_frame(got_copy, expected_copy)


def test_mutating_the_original_leaves_a_copy_alone():
    got, expected = _twins()
    got_copy, expected_copy = got.copy(), expected.copy()

    got["a"] = -1
    expected["a"] = -1

    _same_frame(got_copy, expected_copy)


def test_mutating_a_shallow_copy_leaves_the_original_alone():
    got, expected = _twins()
    got_copy = got.copy(deep=False)
    expected_copy = expected.copy(deep=False)

    got_copy["a"] = -1
    expected_copy["a"] = -1

    _same_frame(got, expected)
    _same_frame(got_copy, expected_copy)


def test_mutating_a_copied_column_leaves_the_frame_alone():
    got, expected = _twins()
    got_col, expected_col = got["a"].copy(), expected["a"].copy()

    got_col.iloc[0] = -1
    expected_col.iloc[0] = -1

    _same_frame(got, expected)
    _same_series(got_col, expected_col)


# ----------------------------------------------------------------------
# loc / iloc derived objects
# ----------------------------------------------------------------------
def test_column_taken_with_loc_is_a_copy():
    got, expected = _twins()
    got_col, expected_col = got.loc[:, "a"], expected.loc[:, "a"]

    got_col.iloc[0] = 77
    expected_col.iloc[0] = 77

    _same_frame(got, expected)
    _same_series(got_col, expected_col)


def test_mutating_the_result_of_loc_full_slice_leaves_the_frame_alone():
    got, expected = _twins()
    got_view, expected_view = got.loc[:], expected.loc[:]

    got_view["a"] = -1
    expected_view["a"] = -1

    _same_frame(got, expected)
    _same_frame(got_view, expected_view)


def test_mutating_the_result_of_iloc_full_slice_leaves_the_frame_alone():
    got, expected = _twins()
    got_view, expected_view = got.iloc[:], expected.iloc[:]

    got_view["a"] = -1
    expected_view["a"] = -1

    _same_frame(got, expected)
    _same_frame(got_view, expected_view)


def test_mutating_an_iloc_row_slice_leaves_the_frame_alone():
    got, expected = _twins()
    got_rows, expected_rows = got.iloc[0:100], expected.iloc[0:100]

    got_rows["a"] = -1
    expected_rows["a"] = -1

    _same_frame(got, expected)
    _same_frame(got_rows, expected_rows)


# ----------------------------------------------------------------------
# plain slices
# ----------------------------------------------------------------------
def test_mutating_a_getitem_row_slice_leaves_the_frame_alone():
    got, expected = _twins()
    got_head, expected_head = got[:2], expected[:2]

    got_head["a"] = -5
    expected_head["a"] = -5

    _same_frame(got, expected)
    _same_frame(got_head, expected_head)


def test_mutating_the_result_of_head_leaves_the_frame_alone():
    got, expected = _twins()
    got_head, expected_head = got.head(3), expected.head(3)

    got_head["a"] = -9
    expected_head["a"] = -9

    _same_frame(got, expected)
    _same_frame(got_head, expected_head)


# ----------------------------------------------------------------------
# chained assignment: pandas refuses to lose the write quietly
# ----------------------------------------------------------------------
def test_chained_setitem_warns_and_leaves_the_frame_unchanged():
    oracle = _plain_pandas_chained_warnings()
    assert "ChainedAssignmentError" in oracle["setitem"]
    got, expected = _twins()

    with pytest.warns(pd.errors.ChainedAssignmentError):
        got["a"][0] = 5
    expected["a"][0] = 5

    _same_frame(got, expected)


def test_chained_iloc_setitem_warns_and_leaves_the_frame_unchanged():
    oracle = _plain_pandas_chained_warnings()
    assert "ChainedAssignmentError" in oracle["iloc_setitem"]
    got, expected = _twins()

    with pytest.warns(pd.errors.ChainedAssignmentError):
        got["a"].iloc[0] = 5
    expected["a"].iloc[0] = 5

    _same_frame(got, expected)


def test_chained_inplace_fillna_warns_and_leaves_the_frame_unchanged():
    oracle = _plain_pandas_chained_warnings()
    assert "ChainedAssignmentError" in oracle["fillna"]
    got, expected = _nan_twins()

    with pytest.warns(pd.errors.ChainedAssignmentError):
        got["a"].fillna(0.0, inplace=True)
    expected["a"].fillna(0.0, inplace=True)

    _same_frame(got, expected)


# ----------------------------------------------------------------------
# exports to host / to one GPU
# ----------------------------------------------------------------------
def test_mutating_the_to_pandas_result_leaves_the_frame_alone():
    got, expected = _twins()

    host = got._fsproxy_fast.to_pandas()
    host["a"] = -3

    assert host["a"].iloc[0] == -3
    _same_frame(got, expected)


def test_mutating_the_compute_result_leaves_the_frame_alone():
    got, expected = _twins()

    gathered = got._fsproxy_fast.compute()
    gathered["a"] = -3

    assert int(gathered["a"].iloc[0]) == -3
    _same_frame(got, expected)


def test_mutating_the_as_cpu_object_result_leaves_the_frame_alone():
    got, expected = _twins()

    host = got.as_cpu_object()
    host["a"] = -3

    assert host["a"].iloc[0] == -3
    _same_frame(got, expected)


def test_mutating_the_as_gpu_object_result_leaves_the_frame_alone():
    got, expected = _twins()

    single = got.as_gpu_object()
    single["a"] = -3

    assert int(single["a"].iloc[0]) == -3
    _same_frame(got, expected)


@pytest.mark.xfail(
    reason="cudf.pandas hands back a writeable array from to_numpy()/values, "
    "so a write that can never reach the frame succeeds silently. Not "
    "multi-GPU specific -- stock single-GPU cudf.pandas behaves identically "
    "(python -m cudf.pandas -c 'import pandas as pd; "
    "print(pd.Series([1]).to_numpy().flags.writeable)' prints True, plain "
    "pandas prints False). ChunkedSeries.to_numpy does return a read-only "
    "array; the flag is lost when cudf.pandas' ndarray proxy re-wraps it.",
    strict=False,
)
@pytest.mark.parametrize(
    "export",
    [lambda column: column.to_numpy(), lambda column: column.values],
    ids=["to_numpy", "values"],
)
def test_a_write_into_an_exported_numpy_array_cannot_be_lost_silently(export):
    got, expected = _twins()
    exported = export(got["a"])

    # pandas hands out a read-only array precisely so that a write which
    # would never reach the frame cannot happen quietly.
    with pytest.raises(ValueError):
        export(expected["a"])[0] = -42
    with pytest.raises(ValueError):
        exported[0] = -42

    _same_frame(got, expected)


# ----------------------------------------------------------------------
# references held across a groupby / merge
# ----------------------------------------------------------------------
def test_groupby_result_is_stable_when_the_source_is_mutated_afterwards():
    got, expected = _twins()
    got_agg = got.groupby("g").sum()
    expected_agg = expected.groupby("g").sum()

    got["a"] = -1
    expected["a"] = -1

    pd.testing.assert_frame_equal(
        _slow(got_agg).sort_index(), expected_agg.sort_index()
    )


def test_unevaluated_groupby_reflects_a_later_source_mutation():
    got, expected = _twins()
    got_grouped = got.groupby("g")
    expected_grouped = expected.groupby("g")

    got["a"] = -1
    expected["a"] = -1

    pd.testing.assert_frame_equal(
        _slow(got_grouped.sum()).sort_index(),
        expected_grouped.sum().sort_index(),
    )


def test_merge_result_is_stable_when_an_input_is_mutated_afterwards():
    got, expected = _twins()
    right = {"g": np.array([0, 1, 2, 3]), "z": np.array([9.0, 8.0, 7.0, 6.0])}
    got_merged = got.merge(pd.DataFrame(right), on="g")
    expected_merged = expected.merge(REAL_DATAFRAME(right), on="g")

    got["a"] = -1
    expected["a"] = -1

    pd.testing.assert_frame_equal(
        _canonical(_slow(got_merged)), _canonical(expected_merged)
    )


def test_materialized_merge_result_is_stable_when_an_input_is_mutated():
    """The same join, read once first: pins the lazy plan as the cause."""
    got, expected = _twins()
    right = {"g": np.array([0, 1, 2, 3]), "z": np.array([9.0, 8.0, 7.0, 6.0])}
    got_merged = got.merge(pd.DataFrame(right), on="g")
    expected_merged = expected.merge(REAL_DATAFRAME(right), on="g")
    assert len(got_merged) == len(expected_merged)

    got["a"] = -1
    expected["a"] = -1

    pd.testing.assert_frame_equal(
        _canonical(_slow(got_merged)), _canonical(expected_merged)
    )


def test_mutating_a_merge_result_leaves_its_inputs_alone():
    got, expected = _twins()
    right = {"g": np.array([0, 1, 2, 3]), "z": np.array([9.0, 8.0, 7.0, 6.0])}
    got_merged = got.merge(pd.DataFrame(right), on="g")
    expected_merged = expected.merge(REAL_DATAFRAME(right), on="g")

    got_merged["a"] = -1
    expected_merged["a"] = -1

    _same_frame(got, expected)


def test_column_reference_taken_before_a_merge_writes_only_to_itself():
    got, expected = _twins()
    got_col, expected_col = got["a"], expected["a"]
    right = {"g": np.array([0, 1, 2, 3]), "z": np.array([9.0, 8.0, 7.0, 6.0])}
    got_merged = got.merge(pd.DataFrame(right), on="g")
    expected_merged = expected.merge(REAL_DATAFRAME(right), on="g")

    got_col.iloc[0] = -7
    expected_col.iloc[0] = -7

    _same_series(got_col, expected_col)
    _same_frame(got, expected)
    pd.testing.assert_frame_equal(
        _canonical(_slow(got_merged)), _canonical(expected_merged)
    )


# ----------------------------------------------------------------------
# concat
# ----------------------------------------------------------------------
def test_concat_result_is_stable_when_an_input_is_mutated_afterwards():
    got_left, expected_left = _twins()
    got_right, expected_right = _twins(offset=100)
    got_all = pd.concat([got_left, got_right], ignore_index=True)
    expected_all = REAL_CONCAT(
        [expected_left, expected_right], ignore_index=True
    )

    got_left["a"] = -1
    expected_left["a"] = -1

    _same_frame(got_all, expected_all)


def test_mutating_a_concat_result_leaves_its_inputs_alone():
    got_left, expected_left = _twins()
    got_right, expected_right = _twins(offset=100)
    got_all = pd.concat([got_left, got_right], ignore_index=True)
    expected_all = REAL_CONCAT(
        [expected_left, expected_right], ignore_index=True
    )

    got_all["a"] = -1
    expected_all["a"] = -1

    _same_frame(got_left, expected_left)
    _same_frame(got_right, expected_right)


# ----------------------------------------------------------------------
# reset_index
# ----------------------------------------------------------------------
def test_mutating_a_reset_index_result_leaves_the_source_alone():
    got, expected = _twins()
    got_reset = got.reset_index(drop=True)
    expected_reset = expected.reset_index(drop=True)

    got_reset["a"] = -1
    expected_reset["a"] = -1

    _same_frame(got, expected)
    _same_frame(got_reset, expected_reset)


def test_a_column_added_to_a_reset_index_result_is_not_added_to_the_source():
    got, expected = _twins()
    got_reset = got.reset_index(drop=True)
    expected_reset = expected.reset_index(drop=True)

    got_reset["added"] = 1.5
    expected_reset["added"] = 1.5

    assert list(_slow(got).columns) == list(expected.columns)
    _same_frame(got_reset, expected_reset)


def test_reset_index_result_is_stable_when_the_source_is_mutated_afterwards():
    got, expected = _twins()
    got_reset = got.reset_index(drop=True)
    expected_reset = expected.reset_index(drop=True)

    got["a"] = -2
    expected["a"] = -2

    _same_frame(got_reset, expected_reset)


# ----------------------------------------------------------------------
# odds and ends
# ----------------------------------------------------------------------
def test_pop_hands_back_a_detached_column():
    got, expected = _twins()
    got_col, expected_col = got.pop("a"), expected.pop("a")

    got_col.iloc[0] = -1
    expected_col.iloc[0] = -1

    _same_frame(got, expected)
    _same_series(got_col, expected_col)


def test_an_inserted_column_does_not_alias_the_series_it_came_from():
    got, expected = _twins()
    got_col, expected_col = got["a"], expected["a"]
    got.insert(0, "z", got_col)
    expected.insert(0, "z", expected_col)

    got_col.iloc[0] = -1
    expected_col.iloc[0] = -1

    _same_frame(got, expected)


def test_a_column_kept_in_a_dict_of_items_does_not_write_back():
    got, expected = _twins()
    got_columns = dict(got.items())
    expected_columns = dict(expected.items())

    got_columns["a"].iloc[0] = -1
    expected_columns["a"].iloc[0] = -1

    _same_frame(got, expected)
    _same_series(got_columns["a"], expected_columns["a"])


def test_a_host_copy_taken_before_a_write_does_not_see_the_write():
    got, expected = _twins()

    before = _snapshot(got)
    assert got._fsproxy_fast.nchunks > 1

    got["a"] = -1
    expected["a"] = -1

    pd.testing.assert_frame_equal(before, REAL_DATAFRAME(_payload()))
    _same_frame(got, expected)
