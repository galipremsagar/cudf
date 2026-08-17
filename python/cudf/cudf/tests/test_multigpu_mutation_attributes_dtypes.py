# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""In-place attribute, dtype and index mutation on the multi-GPU backend.

The backend spreads a frame's rows over every GPU, so an in-place write has to
reach the caller's own object rather than a gathered copy.  Every test here
does the same thing twice -- once on the multi-GPU-backed object and once on a
plain pandas copy of it -- and demands the two agree, including the value the
mutating call returned.

pandas is the oracle throughout: if pandas raises, the backend raising is
correct.  A test that fails here is a backend defect, not a test to soften.
"""

from __future__ import annotations

import warnings

import pytest

pytest.importorskip("cudf.multigpu")

import cudf.multigpu.pandas_compat as _pandas_compat  # noqa: E402

if not _pandas_compat.is_installed():
    # install() warns that the backend is experimental, and the suite turns
    # warnings into errors; the warning is not what is under test here.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        _pandas_compat.install(
            initial_pool_fraction=0.05, max_pool_fraction=0.30
        )

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from cudf.multigpu._runtime import visible_devices  # noqa: E402

if len(visible_devices()) < 2:
    pytest.skip(
        "cudf.multigpu mutation tests need at least 2 CUDA devices",
        allow_module_level=True,
    )


#: An operation with no distributed implementation warns and gathers onto one
#: GPU.  That is a scalability problem, not a correctness one, and the suite
#: turns warnings into errors -- so let it through and judge the result.
pytestmark = pytest.mark.filterwarnings("ignore::UserWarning")

#: Big enough that ``from_pandas`` hands every GPU a slice of its own.
NROWS = 4000


# ----------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------
def real(obj):
    """The plain-pandas object behind a ``cudf.pandas`` proxy."""
    return getattr(obj, "_fsproxy_slow", obj)


def make_frame(nrows: int = NROWS):
    return pd.DataFrame(
        {
            "a": np.arange(nrows, dtype="int64"),
            "b": np.arange(nrows, dtype="float64") / 3.0,
            "c": [f"s{i % 7}" for i in range(nrows)],
        }
    )


def make_series(nrows: int = NROWS):
    return pd.Series(np.arange(nrows, dtype="int64"), name="s")


def oracle(obj):
    """A real-pandas twin of ``obj``, and proof ``obj`` is really partitioned.

    Reading ``_fsproxy_slow`` parks the proxy on the CPU object, so touch
    ``_fsproxy_fast`` afterwards: that puts the multi-GPU frame back in the
    proxy before the mutation under test runs, and asserts the rows are spread
    over more than one chunk.
    """
    expected = obj._fsproxy_slow.copy()
    assert obj._fsproxy_fast.nchunks > 1, "frame landed on a single chunk"
    return expected


def assert_frame(got, expected) -> None:
    pd.testing.assert_frame_equal(got._fsproxy_slow, expected)


def assert_series(got, expected) -> None:
    pd.testing.assert_series_equal(got._fsproxy_slow, expected)


def both_raise(got, expected, mutate, exc):
    """Run ``mutate`` on both; require the same exception type from each."""
    with pytest.raises(exc):
        mutate(expected)
    with pytest.raises(exc):
        mutate(got)


# ----------------------------------------------------------------------
# df.columns = [...]
# ----------------------------------------------------------------------
def test_columns_assignment_renames_every_chunk():
    got = make_frame()
    expected = oracle(got)

    assert (got.__setattr__("columns", ["x", "y", "z"])) is None
    expected.columns = ["x", "y", "z"]

    assert_frame(got, expected)
    assert got._fsproxy_fast.nchunks > 1


def test_columns_assignment_visible_to_getitem():
    got = make_frame()
    expected = oracle(got)

    got.columns = ["x", "y", "z"]
    expected.columns = ["x", "y", "z"]

    assert list(got.columns) == list(expected.columns)
    pd.testing.assert_series_equal(real(got["x"]), expected["x"])
    assert "a" not in got


def test_columns_assignment_visible_to_attribute_access():
    got = make_frame()
    expected = oracle(got)

    got.columns = ["x", "y", "z"]
    expected.columns = ["x", "y", "z"]

    pd.testing.assert_series_equal(real(got.x), expected.x)


def test_columns_assignment_updates_dtypes_index():
    got = make_frame()
    expected = oracle(got)

    got.columns = ["x", "y", "z"]
    expected.columns = ["x", "y", "z"]

    pd.testing.assert_series_equal(real(got.dtypes), expected.dtypes)


def test_columns_assignment_from_numpy_array():
    got = make_frame()
    expected = oracle(got)

    got.columns = np.array(["x", "y", "z"])
    expected.columns = np.array(["x", "y", "z"])

    assert_frame(got, expected)


def test_columns_assignment_allows_duplicate_names():
    got = make_frame()
    expected = oracle(got)

    got.columns = ["dup", "dup", "c"]
    expected.columns = ["dup", "dup", "c"]

    assert_frame(got, expected)


@pytest.mark.parametrize(
    "names", [["x", "y"], ["p", "q", "r", "s"]], ids=["too_few", "too_many"]
)
def test_columns_assignment_wrong_length_raises(names):
    got = make_frame()
    expected = oracle(got)

    both_raise(got, expected, lambda d: setattr(d, "columns", names), ValueError)

    # the rejected write must not have half-renamed the chunks
    assert_frame(got, expected)


def test_columns_assignment_keeps_the_columns_index_name():
    got = make_frame()
    expected = oracle(got)

    got.columns = real(pd.Index(["x", "y", "z"], name="cols"))
    expected.columns = real(pd.Index(["x", "y", "z"], name="cols"))

    assert got._fsproxy_slow.columns.name == "cols"
    assert_frame(got, expected)


def test_columns_name_attribute_assignment():
    got = make_frame()
    expected = oracle(got)

    got.columns.name = "cols"
    expected.columns.name = "cols"

    assert got._fsproxy_slow.columns.name == "cols"
    assert_frame(got, expected)


def test_columns_values_mutation_renames_the_column():
    got = make_frame()
    expected = oracle(got)

    got.columns.values[0] = "renamed"
    expected.columns.values[0] = "renamed"

    assert list(got._fsproxy_slow.columns) == ["renamed", "b", "c"]
    assert_frame(got, expected)


# ----------------------------------------------------------------------
# df.index = [...]
# ----------------------------------------------------------------------
def test_index_assignment_from_list():
    got = make_frame()
    expected = oracle(got)

    labels = list(range(1000, 1000 + NROWS))
    assert (got.__setattr__("index", labels)) is None
    expected.index = labels

    assert_frame(got, expected)


def test_index_assignment_stays_partitioned():
    got = make_frame()
    expected = oracle(got)

    got.index = np.arange(NROWS)[::-1]
    expected.index = np.arange(NROWS)[::-1]

    assert_frame(got, expected)
    assert got._fsproxy_fast.nchunks > 1


def test_index_assignment_from_string_labels():
    got = make_frame()
    expected = oracle(got)

    labels = [f"r{i}" for i in range(NROWS)]
    got.index = labels
    expected.index = labels

    assert_frame(got, expected)


def test_index_assignment_from_datetimeindex():
    got = make_frame()
    expected = oracle(got)

    got.index = real(pd.date_range("2021-01-01", periods=NROWS, freq="min"))
    expected.index = real(
        pd.date_range("2021-01-01", periods=NROWS, freq="min")
    )

    assert_frame(got, expected)


def test_index_assignment_keeps_the_index_name():
    got = make_frame()
    expected = oracle(got)

    got.index = real(pd.Index(np.arange(NROWS) * 2, name="ix"))
    expected.index = real(pd.Index(np.arange(NROWS) * 2, name="ix"))

    assert got._fsproxy_slow.index.name == "ix"
    assert_frame(got, expected)


def test_index_assignment_from_derived_index():
    got = make_frame()
    expected = oracle(got)

    got.index = got.index + 1
    expected.index = expected.index + 1

    assert_frame(got, expected)


def test_index_assignment_allows_duplicate_labels():
    got = make_frame()
    expected = oracle(got)

    got.index = [0] * NROWS
    expected.index = [0] * NROWS

    assert_frame(got, expected)


def test_index_assignment_wrong_length_raises():
    got = make_frame()
    expected = oracle(got)

    both_raise(
        got, expected, lambda d: setattr(d, "index", [1, 2, 3]), ValueError
    )
    assert_frame(got, expected)


def test_index_name_attribute_assignment():
    got = make_frame()
    expected = oracle(got)

    got.index.name = "ix"
    expected.index.name = "ix"

    assert got._fsproxy_slow.index.name == "ix"
    assert_frame(got, expected)


def test_index_names_attribute_assignment():
    got = make_frame()
    expected = oracle(got)

    got.index.names = ["ix"]
    expected.index.names = ["ix"]

    assert got._fsproxy_slow.index.names == ["ix"]
    assert_frame(got, expected)


def test_index_rename_inplace_mutates_the_frame():
    got = make_frame()
    expected = oracle(got)

    assert got.index.rename("ix", inplace=True) is None
    assert expected.index.rename("ix", inplace=True) is None

    assert got._fsproxy_slow.index.name == "ix"
    assert_frame(got, expected)


def test_rename_axis_inplace_sets_the_index_name():
    got = make_frame()
    expected = oracle(got)

    assert got.rename_axis("ix", inplace=True) is None
    assert expected.rename_axis("ix", inplace=True) is None

    assert_frame(got, expected)


def test_rename_columns_inplace():
    got = make_frame()
    expected = oracle(got)

    assert got.rename(columns={"a": "A"}, inplace=True) is None
    assert expected.rename(columns={"a": "A"}, inplace=True) is None

    assert_frame(got, expected)


# ----------------------------------------------------------------------
# Series name and index
# ----------------------------------------------------------------------
def test_series_name_assignment():
    got = make_series()
    expected = oracle(got)

    got.name = "renamed"
    expected.name = "renamed"

    assert got._fsproxy_slow.name == "renamed"
    assert_series(got, expected)


def test_series_name_assignment_to_none():
    got = make_series()
    expected = oracle(got)

    got.name = None
    expected.name = None

    assert_series(got, expected)


def test_series_name_assignment_non_string_label():
    got = make_series()
    expected = oracle(got)

    got.name = 42
    expected.name = 42

    assert_series(got, expected)


def test_series_rename_inplace_returns_what_pandas_returns():
    got = make_series()
    expected = oracle(got)

    got_ret = got.rename("renamed", inplace=True)
    expected_ret = expected.rename("renamed", inplace=True)

    assert_series(got, expected)
    assert (got_ret is None) == (expected_ret is None)
    assert real(got_ret).name == expected_ret.name


def test_series_index_assignment():
    got = make_series()
    expected = oracle(got)

    labels = np.arange(NROWS) * 3
    assert (got.__setattr__("index", labels)) is None
    expected.index = labels

    assert_series(got, expected)
    assert got._fsproxy_fast.nchunks > 1


def test_series_index_assignment_from_string_labels():
    got = make_series()
    expected = oracle(got)

    labels = [f"r{i}" for i in range(NROWS)]
    got.index = labels
    expected.index = labels

    assert_series(got, expected)


def test_series_index_assignment_wrong_length_raises():
    got = make_series()
    expected = oracle(got)

    both_raise(
        got, expected, lambda s: setattr(s, "index", [1, 2, 3]), ValueError
    )
    assert_series(got, expected)


def test_series_index_name_attribute_assignment():
    got = make_series()
    expected = oracle(got)

    got.index.name = "ix"
    expected.index.name = "ix"

    assert got._fsproxy_slow.index.name == "ix"
    assert_series(got, expected)


def test_series_rename_axis_inplace():
    got = make_series()
    expected = oracle(got)

    assert got.rename_axis("ix", inplace=True) is None
    assert expected.rename_axis("ix", inplace=True) is None

    assert_series(got, expected)


# ----------------------------------------------------------------------
# dtype changes through assignment
# ----------------------------------------------------------------------
@pytest.mark.parametrize("dtype", ["float32", "int32", "uint8", "int16", "str"])
def test_astype_assignment_changes_the_column_dtype(dtype):
    got = make_frame()
    expected = oracle(got)

    got["a"] = (got["a"] % 200).astype(dtype)
    expected["a"] = (expected["a"] % 200).astype(dtype)

    assert str(got._fsproxy_slow["a"].dtype) == str(expected["a"].dtype)
    assert_frame(got, expected)


def test_astype_category_assignment_on_int_column():
    got = make_frame()
    expected = oracle(got)

    got["a"] = got["a"].astype("category")
    expected["a"] = expected["a"].astype("category")

    assert isinstance(got._fsproxy_slow["a"].dtype, pd.CategoricalDtype)
    assert_frame(got, expected)


def test_astype_category_assignment_on_string_column():
    got = make_frame()
    expected = oracle(got)

    got["c"] = got["c"].astype("category")
    expected["c"] = expected["c"].astype("category")

    assert_frame(got, expected)


def test_int_column_assigned_float_array():
    got = make_frame()
    expected = oracle(got)

    values = np.linspace(0.0, 1.0, NROWS)
    got["a"] = values
    expected["a"] = values

    assert got._fsproxy_slow["a"].dtype == np.dtype("float64")
    assert_frame(got, expected)


def test_int_column_assigned_string_list():
    got = make_frame()
    expected = oracle(got)

    values = [f"v{i}" for i in range(NROWS)]
    got["a"] = values
    expected["a"] = values

    assert_frame(got, expected)


def test_int_column_assigned_bool_array():
    got = make_frame()
    expected = oracle(got)

    values = np.arange(NROWS) % 2 == 0
    got["a"] = values
    expected["a"] = values

    assert_frame(got, expected)


@pytest.mark.parametrize("scalar", [1.5, "zz", True], ids=["float", "str", "bool"])
def test_int_column_assigned_scalar_of_another_type(scalar):
    got = make_frame()
    expected = oracle(got)

    got["a"] = scalar
    expected["a"] = scalar

    assert_frame(got, expected)


def test_int_column_assigned_another_column():
    got = make_frame()
    expected = oracle(got)

    got["a"] = got["b"]
    expected["a"] = expected["b"]

    assert_frame(got, expected)


def test_column_assigned_nullable_int64_array():
    got = make_frame()
    expected = oracle(got)

    got["a"] = real(pd.array(np.arange(NROWS), dtype="Int64"))
    expected["a"] = real(pd.array(np.arange(NROWS), dtype="Int64"))

    assert str(got._fsproxy_slow["a"].dtype) == "Int64"
    assert_frame(got, expected)


def test_column_assignment_wrong_length_raises():
    got = make_frame()
    expected = oracle(got)

    both_raise(
        got, expected, lambda d: d.__setitem__("new", [1, 2, 3]), ValueError
    )
    assert_frame(got, expected)


def test_assign_categorical_column():
    got = make_frame()
    expected = oracle(got)

    got["cat"] = real(pd.Categorical(["p", "q"] * (NROWS // 2)))
    expected["cat"] = real(pd.Categorical(["p", "q"] * (NROWS // 2)))

    assert_frame(got, expected)
    assert got._fsproxy_fast.nchunks > 1


def test_assign_datetime_column():
    got = make_frame()
    expected = oracle(got)

    got["dt"] = real(pd.date_range("2020-01-01", periods=NROWS, freq="s"))
    expected["dt"] = real(
        pd.date_range("2020-01-01", periods=NROWS, freq="s")
    )

    assert str(got._fsproxy_slow["dt"].dtype).startswith("datetime64")
    assert_frame(got, expected)


@pytest.mark.parametrize(
    "setter",
    [
        pytest.param(lambda d, v: d.loc.__setitem__((3500, "dt"), v), id="loc"),
        pytest.param(lambda d, v: d.iloc.__setitem__((3500, 3), v), id="iloc"),
        pytest.param(lambda d, v: d.at.__setitem__((3500, "dt"), v), id="at"),
        pytest.param(lambda d, v: d.iat.__setitem__((3500, 3), v), id="iat"),
    ],
)
def test_assign_datetime_column_then_mutate_one_element(setter):
    got = make_frame()
    expected = oracle(got)

    stamps = pd.date_range("2020-01-01", periods=NROWS, freq="s")
    got["dt"] = real(stamps)
    expected["dt"] = real(stamps)

    new = real(pd.Timestamp("1999-01-01"))
    assert setter(got, new) is None
    assert setter(expected, new) is None

    assert got._fsproxy_slow["dt"].iloc[3500] == pd.Timestamp("1999-01-01")
    assert_frame(got, expected)


def test_assign_datetime_column_then_shift_it_by_a_timedelta():
    got = make_frame()
    expected = oracle(got)

    stamps = pd.date_range("2020-01-01", periods=NROWS, freq="s")
    got["dt"] = real(stamps)
    expected["dt"] = real(stamps)

    got["dt"] = got["dt"] + real(pd.Timedelta(days=1))
    expected["dt"] = expected["dt"] + real(pd.Timedelta(days=1))

    assert_frame(got, expected)
