# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0
"""Tests for the chunked multi-GPU layer (``cudf.multigpu``).

Every test compares a multi-GPU result against pandas on the same data.  The
module is skipped unless at least two CUDA devices are visible, since a single
device cannot exercise any of the cross-device paths.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import cudf

mgpu = pytest.importorskip("cudf.multigpu")

from cudf.multigpu._runtime import visible_devices  # noqa: E402

if len(visible_devices()) < 2:
    pytest.skip(
        "cudf.multigpu requires at least 2 CUDA devices", allow_module_level=True
    )

NPARTS = 4


@pytest.fixture(scope="module")
def runtime():
    return mgpu.init(devices=visible_devices()[:NPARTS])


@pytest.fixture(scope="module")
def pdf():
    rng = np.random.default_rng(7)
    n = 50_000
    frame = pd.DataFrame(
        {
            "k": rng.integers(0, 97, n),
            "g": pd.Series(rng.integers(0, 5, n)).map(lambda i: f"grp{i}"),
            "a": rng.normal(size=n),
            "b": rng.integers(0, 1000, n).astype("int64"),
            "c": rng.random(n),
        }
    )
    frame.loc[rng.choice(n, 100, replace=False), "a"] = np.nan
    return frame


@pytest.fixture(scope="module")
def mdf(runtime, pdf):
    return mgpu.from_pandas(pdf, npartitions=NPARTS)


# ----------------------------------------------------------------------
# placement and structure
# ----------------------------------------------------------------------
def test_spreads_over_every_device(runtime, mdf):
    assert mdf.nchunks == NPARTS
    assert len(set(mdf.devices)) == min(NPARTS, runtime.n_devices)
    assert len(mdf.memory_usage_per_device()) == len(set(mdf.devices))


def test_len_and_shape(mdf, pdf):
    assert len(mdf) == len(pdf)
    assert mdf.shape == pdf.shape
    assert list(mdf.columns) == list(pdf.columns)


def test_to_pandas_roundtrip(mdf, pdf):
    pd.testing.assert_frame_equal(
        mdf.to_pandas().reset_index(drop=True), pdf.reset_index(drop=True)
    )


def test_compute_returns_single_gpu_frame(mdf, pdf):
    gathered = mdf.compute()
    assert isinstance(gathered, cudf.DataFrame)
    assert len(gathered) == len(pdf)


# ----------------------------------------------------------------------
# transfer fidelity
# ----------------------------------------------------------------------
@pytest.mark.parametrize(
    "values",
    [
        pytest.param(lambda: cudf.Series(np.arange(8)), id="int64"),
        pytest.param(lambda: cudf.Series([1.5] * 8), id="float64"),
        pytest.param(lambda: cudf.Series(list("abcdefgh")), id="string"),
        pytest.param(
            lambda: cudf.Series([1.0, None, 3.0, None, 5.0, 6.0, 7.0, 8.0]),
            id="nullable",
        ),
        pytest.param(
            lambda: cudf.to_datetime(cudf.Series(np.arange(8)), unit="s"),
            id="datetime",
        ),
        pytest.param(
            lambda: cudf.Series(np.arange(8).astype("timedelta64[s]")),
            id="timedelta",
        ),
        pytest.param(
            lambda: cudf.Series([[1, 2], [3], [], [4, 5, 6], [7], [8], [9], []]),
            id="list",
        ),
        pytest.param(
            lambda: cudf.Series(np.arange(8) / 4).astype(
                cudf.Decimal64Dtype(10, 2)
            ),
            id="decimal",
        ),
        pytest.param(
            lambda: cudf.Series(list("aabbccdd")).astype("category"),
            id="categorical",
        ),
    ],
)
def test_move_preserves_values_and_dtype(runtime, values):
    source, target = runtime.devices[0], runtime.devices[1]
    original = runtime.run(source, lambda: cudf.DataFrame({"x": values()}))
    expected = runtime.run(source, lambda f: f.to_pandas(), original)

    moved = mgpu.move(original, target, source)
    got = runtime.run(target, lambda f: f.to_pandas(), moved)

    pd.testing.assert_frame_equal(got, expected)


def test_move_places_memory_on_the_target_device(runtime):
    from cuda.bindings import runtime as cudart

    source, target = runtime.devices[0], runtime.devices[1]
    original = runtime.run(
        source, lambda: cudf.DataFrame({"x": np.arange(1024)})
    )
    moved = mgpu.move(original, target, source)

    def resident_device(frame):
        ptr = frame["x"]._column.data.ptr
        return cudart.cudaPointerGetAttributes(ptr)[1].device

    assert runtime.run(target, resident_device, moved) == target


# ----------------------------------------------------------------------
# reductions
# ----------------------------------------------------------------------
@pytest.mark.parametrize(
    "method", ["sum", "min", "max", "count", "mean", "std", "var", "prod"]
)
def test_reductions_match_pandas(mdf, pdf, method):
    columns = ["a", "b", "c"]
    got = getattr(mdf[columns], method)()
    expected = getattr(pdf[columns], method)()
    np.testing.assert_allclose(
        got.astype(float).sort_index().to_numpy(),
        expected.astype(float).sort_index().to_numpy(),
        rtol=1e-9,
    )


def test_nunique_is_exact(mdf, pdf):
    assert mdf["k"].nunique() == pdf["k"].nunique()


def test_idxmax(mdf, pdf):
    assert mdf["b"].idxmax() == pdf["b"].idxmax()


@pytest.mark.parametrize("q", [0.0, 0.25, 0.5, 0.9, 1.0])
def test_quantile_is_exact(mdf, pdf, q):
    np.testing.assert_allclose(mdf["c"].quantile(q), pdf["c"].quantile(q), rtol=1e-6)


# ----------------------------------------------------------------------
# elementwise
# ----------------------------------------------------------------------
def test_arithmetic_and_assignment(mdf, pdf):
    got = (mdf["b"] * 2 + 1).to_pandas().reset_index(drop=True)
    expected = (pdf["b"] * 2 + 1).reset_index(drop=True)
    pd.testing.assert_series_equal(got, expected, check_names=False)


def test_boolean_filter(mdf, pdf):
    assert len(mdf[mdf["b"] > 500]) == int((pdf["b"] > 500).sum())


def test_string_accessor(mdf, pdf):
    assert mdf["g"].str.upper().to_pandas().tolist() == pdf["g"].str.upper().tolist()


def test_fillna_and_isna(mdf, pdf):
    assert mdf["a"].isna().sum() == pdf["a"].isna().sum()
    np.testing.assert_allclose(
        mdf["a"].fillna(0.0).sum(), pdf["a"].fillna(0.0).sum()
    )


def test_map_chunks_fuses_an_expression(mdf, pdf):
    got = mdf.map_chunks(lambda c: (c["b"] * 2 + c["k"]) > 100).sum()
    expected = int(((pdf["b"] * 2 + pdf["k"]) > 100).sum())
    assert got == expected


def test_map_chunks_broadcast(runtime, mdf, pdf):
    lookup = pd.DataFrame({"g": [f"grp{i}" for i in range(5)], "w": np.arange(5.0)})
    got = mdf.map_chunks(
        lambda c, d: c[["g", "b"]].merge(d, on="g")["w"],
        broadcast=[mgpu.from_pandas(lookup, npartitions=1)],
    ).sum()
    expected = pdf[["g", "b"]].merge(lookup, on="g")["w"].sum()
    np.testing.assert_allclose(got, expected)


# ----------------------------------------------------------------------
# shuffle-backed operations
# ----------------------------------------------------------------------
def _sorted_frame(frame, keys):
    return frame.sort_values(keys).reset_index(drop=True)


@pytest.mark.parametrize(
    "spec", [{"b": "sum"}, {"a": "mean"}, {"b": "min", "c": "max"}, {"a": "count"}]
)
def test_groupby_tree_path(mdf, pdf, spec):
    got = mdf.groupby("g").agg(spec).to_pandas().reset_index()
    expected = pdf.groupby("g").agg(spec).reset_index()
    pd.testing.assert_frame_equal(
        _sorted_frame(got, ["g"]), _sorted_frame(expected, ["g"]), rtol=1e-8
    )


@pytest.mark.parametrize("spec", [{"a": "std"}, {"k": "nunique"}])
def test_groupby_shuffle_path(mdf, pdf, spec):
    got = mdf.groupby("g").agg(spec).to_pandas().reset_index()
    expected = pdf.groupby("g").agg(spec).reset_index()
    pd.testing.assert_frame_equal(
        _sorted_frame(got, ["g"]), _sorted_frame(expected, ["g"]), rtol=1e-8
    )


def test_groupby_multiple_keys(mdf, pdf):
    got = mdf.groupby(["g", "k"]).agg({"b": "sum"}).to_pandas().reset_index()
    expected = pdf.groupby(["g", "k"]).agg({"b": "sum"}).reset_index()
    pd.testing.assert_frame_equal(
        _sorted_frame(got, ["g", "k"]), _sorted_frame(expected, ["g", "k"])
    )


def test_groupby_size(mdf, pdf):
    got = mdf.groupby("g").size().to_pandas().sort_index()
    expected = pdf.groupby("g").size().sort_index()
    np.testing.assert_array_equal(got.to_numpy(), expected.to_numpy())


def test_groupby_result_is_partitioned_by_key(mdf):
    grouped = mdf.groupby("g").agg({"b": "sum"})
    # every key must live on exactly one chunk
    per_chunk = grouped._run_chunks(
        lambda c: set(c.reset_index()["g"].to_pandas())
    )
    seen: set = set()
    for keys in per_chunk:
        assert not (seen & keys), "a group-by key appeared on two GPUs"
        seen |= keys


@pytest.mark.parametrize("ascending", [True, False])
def test_sort_values_is_globally_ordered(mdf, pdf, ascending):
    got = mdf.sort_values("b", ascending=ascending)["b"].to_pandas().to_numpy()
    expected = np.sort(pdf["b"].to_numpy())
    if not ascending:
        expected = expected[::-1]
    np.testing.assert_array_equal(got, expected)


def test_series_sort_values(mdf, pdf):
    got = mdf["c"].sort_values().to_pandas().to_numpy()
    np.testing.assert_allclose(got, np.sort(pdf["c"].to_numpy()))


def test_drop_duplicates(mdf, pdf):
    assert len(mdf.drop_duplicates(subset=["k"])) == len(
        pdf.drop_duplicates(subset=["k"])
    )


def test_unique_and_value_counts(mdf, pdf):
    assert sorted(mdf["k"].unique().to_pandas().tolist()) == sorted(
        pdf["k"].unique().tolist()
    )
    got = mdf["g"].value_counts().to_pandas().sort_index()
    expected = pdf["g"].value_counts().sort_index()
    np.testing.assert_array_equal(got.to_numpy(), expected.to_numpy())


@pytest.mark.parametrize("broadcast", [True, False])
def test_merge(mdf, pdf, broadcast):
    right = pd.DataFrame(
        {"g": [f"grp{i}" for i in range(5)], "w": np.arange(5) * 1.5}
    )
    right_m = mgpu.from_pandas(right, npartitions=2)
    got = mdf.merge(right_m, on="g", broadcast=broadcast).to_pandas()
    expected = pdf.merge(right, on="g")
    np.testing.assert_allclose(
        np.sort(got["w"].to_numpy()), np.sort(expected["w"].to_numpy())
    )
    assert len(got) == len(expected)


def test_left_merge_keeps_unmatched_rows(mdf, pdf):
    right = pd.DataFrame({"k": np.arange(0, 97, 2), "z": np.arange(0, 97, 2) * 2.0})
    right_m = mgpu.from_pandas(right, npartitions=2)
    got = mdf.merge(right_m, on="k", how="left").to_pandas()
    expected = pdf.merge(right, on="k", how="left")
    assert len(got) == len(expected)
    assert int(got["z"].isna().sum()) == int(expected["z"].isna().sum())


# ----------------------------------------------------------------------
# repartitioning
# ----------------------------------------------------------------------
def test_rechunk_preserves_rows(runtime, mdf, pdf):
    rechunked = mdf.rechunk(nchunks=3)
    assert rechunked.nchunks == 3
    pd.testing.assert_frame_equal(
        rechunked.to_pandas().reset_index(drop=True), pdf.reset_index(drop=True)
    )


def test_repartition_like_enables_binary_ops(mdf, pdf):
    other = mgpu.from_pandas(pdf["b"], npartitions=NPARTS + 1)
    aligned = other.repartition_like(mdf["b"])
    assert aligned.chunk_lengths == mdf["b"].chunk_lengths
    np.testing.assert_array_equal(
        (mdf["b"] + aligned).to_pandas().to_numpy(), (pdf["b"] * 2).to_numpy()
    )


def test_head_tail_and_iloc(mdf, pdf):
    assert mdf.head(5).to_pandas()["b"].tolist() == pdf.head(5)["b"].tolist()
    assert mdf.tail(5).to_pandas()["b"].tolist() == pdf.tail(5)["b"].tolist()
    assert (
        mdf.iloc[100:150]["b"].to_pandas().tolist() == pdf.iloc[100:150]["b"].tolist()
    )


def test_reset_index_is_globally_consecutive(mdf, pdf):
    got = mdf.reset_index(drop=True)
    np.testing.assert_array_equal(
        got.to_pandas().index.to_numpy(), np.arange(len(pdf))
    )


# ----------------------------------------------------------------------
# IO
# ----------------------------------------------------------------------
def test_parquet_roundtrip_preserves_order(runtime, pdf, tmp_path):
    source = tmp_path / "in"
    source.mkdir()
    for i in range(3):
        piece = pdf.iloc[i * 16_000 : (i + 1) * 16_000]
        cudf.from_pandas(piece).to_parquet(
            source / f"f{i}.parquet", row_group_size_rows=2_000
        )

    frame = mgpu.read_parquet(str(source), npartitions=NPARTS)
    assert frame.nchunks == NPARTS
    expected = pdf.iloc[: 3 * 16_000].reset_index(drop=True)
    pd.testing.assert_frame_equal(
        frame.to_pandas().reset_index(drop=True), expected
    )

    out = tmp_path / "out"
    frame.to_parquet(str(out))
    back = mgpu.read_parquet(str(out), npartitions=NPARTS)
    pd.testing.assert_frame_equal(
        back.to_pandas().reset_index(drop=True), expected
    )


def test_parquet_column_projection(runtime, pdf, tmp_path):
    path = tmp_path / "proj"
    path.mkdir()
    cudf.from_pandas(pdf.head(8_000)).to_parquet(
        path / "f.parquet", row_group_size_rows=1_000
    )
    frame = mgpu.read_parquet(str(path), columns=["k", "b"], npartitions=NPARTS)
    assert list(frame.columns) == ["k", "b"]


def test_parquet_plan_is_balanced_and_complete(pdf, tmp_path):
    from cudf.multigpu._io import parquet_row_group_plan

    path = tmp_path / "plan"
    path.mkdir()
    cudf.from_pandas(pdf).to_parquet(path / "f.parquet", row_group_size_rows=1_000)
    _plans, rows, _bytes = parquet_row_group_plan([str(path / "f.parquet")], 4)
    assert sum(rows) == len(pdf)


# ----------------------------------------------------------------------
# scans and boundary-crossing operations
#
# These run with fallback warnings escalated to errors, so a test only passes
# if the operation really was distributed rather than gathered onto one GPU.
# ----------------------------------------------------------------------
@pytest.fixture
def strict_fallback():
    """Turn the single-GPU fallback warning into an error.

    Matched on message rather than category so that unrelated warnings (Numba
    occupancy hints, for one) do not masquerade as a fallback.
    """
    import warnings

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "error", message=".*no distributed implementation.*"
        )
        yield


@pytest.mark.parametrize("how", ["cumsum", "cumprod", "cummax", "cummin"])
def test_series_scan(strict_fallback, mdf, pdf, how):
    got = getattr(mdf["c"], how)().to_pandas().reset_index(drop=True)
    expected = getattr(pdf["c"], how)().reset_index(drop=True)
    np.testing.assert_allclose(got.to_numpy(), expected.to_numpy(), rtol=1e-9)


@pytest.mark.parametrize("how", ["cumsum", "cummax", "cummin"])
def test_frame_scan(strict_fallback, mdf, pdf, how):
    columns = ["b", "c"]
    got = getattr(mdf[columns], how)().to_pandas().reset_index(drop=True)
    expected = getattr(pdf[columns], how)().reset_index(drop=True)
    np.testing.assert_allclose(got.to_numpy(), expected.to_numpy(), rtol=1e-9)


@pytest.mark.parametrize("periods", [1, 3, -1, -2, 10**9])
def test_shift_across_chunk_boundaries(strict_fallback, mdf, pdf, periods):
    got = mdf["c"].shift(periods).to_pandas().reset_index(drop=True)
    expected = pdf["c"].shift(periods).reset_index(drop=True)
    np.testing.assert_allclose(
        got.to_numpy(dtype=float), expected.to_numpy(dtype=float), equal_nan=True
    )


@pytest.mark.parametrize("periods", [1, -3])
def test_diff(strict_fallback, mdf, pdf, periods):
    got = mdf["c"].diff(periods).to_pandas().reset_index(drop=True)
    expected = pdf["c"].diff(periods).reset_index(drop=True)
    np.testing.assert_allclose(
        got.to_numpy(dtype=float), expected.to_numpy(dtype=float), equal_nan=True
    )


@pytest.mark.parametrize("how", ["ffill", "bfill"])
def test_directional_fill_crosses_chunks(strict_fallback, mdf, pdf, how):
    got = getattr(mdf["a"], how)().to_pandas().reset_index(drop=True)
    expected = getattr(pdf["a"], how)().reset_index(drop=True)
    np.testing.assert_allclose(
        got.to_numpy(dtype=float), expected.to_numpy(dtype=float), equal_nan=True
    )


# ----------------------------------------------------------------------
# distributed statistics
# ----------------------------------------------------------------------
def test_skew_and_kurtosis(strict_fallback, mdf, pdf):
    columns = ["a", "b", "c"]
    np.testing.assert_allclose(
        mdf[columns].skew().to_numpy(dtype=float),
        pdf[columns].skew().to_numpy(dtype=float),
        rtol=1e-6,
    )
    np.testing.assert_allclose(
        mdf[columns].kurtosis().to_numpy(dtype=float),
        pdf[columns].kurtosis().to_numpy(dtype=float),
        rtol=1e-5,
    )


def test_cov_and_corr_use_pairwise_complete_rows(strict_fallback, mdf, pdf):
    # column "a" carries nulls, so a naive per-column mean gives a different
    # (wrong) answer than pandas here.
    columns = ["a", "b", "c"]
    np.testing.assert_allclose(
        mdf[columns].cov().to_numpy(dtype=float),
        pdf[columns].cov().to_numpy(dtype=float),
        rtol=1e-8,
    )
    np.testing.assert_allclose(
        mdf[columns].corr().to_numpy(dtype=float),
        pdf[columns].corr().to_numpy(dtype=float),
        rtol=1e-8,
    )


def test_agg(strict_fallback, mdf, pdf):
    np.testing.assert_allclose(
        mdf[["b", "c"]].agg("sum").to_numpy(dtype=float),
        pdf[["b", "c"]].agg("sum").to_numpy(dtype=float),
    )


def test_memory_usage_sums_across_devices(mdf):
    usage = mdf.memory_usage()
    assert usage.sum() > 0
    assert set(usage.index) >= set(mdf.columns)


# ----------------------------------------------------------------------
# sampling, top-k, UDFs
# ----------------------------------------------------------------------
def test_top_k(strict_fallback, mdf, pdf):
    np.testing.assert_array_equal(
        mdf.nlargest(10, "b")["b"].to_pandas().to_numpy(),
        pdf.nlargest(10, "b")["b"].to_numpy(),
    )
    np.testing.assert_array_equal(
        mdf.nsmallest(7, "b")["b"].to_pandas().to_numpy(),
        pdf.nsmallest(7, "b")["b"].to_numpy(),
    )


def test_sample_stays_distributed(strict_fallback, mdf):
    sampled = mdf.sample(frac=0.1)
    assert sampled.nchunks == mdf.nchunks
    assert len(sampled) == pytest.approx(len(mdf) * 0.1, rel=0.05)
    assert len(mdf.sample(n=1000)) == 1000


def test_numeric_udf_runs_per_chunk(strict_fallback, mdf, pdf):
    got = mdf["b"].apply(lambda x: x * 2 + 1).to_pandas().reset_index(drop=True)
    expected = pdf["b"].apply(lambda x: x * 2 + 1).reset_index(drop=True)
    np.testing.assert_array_equal(got.to_numpy(), expected.to_numpy())


def test_string_udf_is_rejected_not_silently_wrong(mdf):
    with pytest.raises(NotImplementedError, match="string"):
        mdf["g"].apply(lambda s: s.upper())


def test_query_and_eval(strict_fallback, mdf, pdf):
    assert len(mdf.query("b > 500")) == int((pdf["b"] > 500).sum())
    # same-dtype operands: cuDF's AST evaluator rejects mixed int/float here,
    # on one GPU as well as several.
    np.testing.assert_allclose(
        mdf.eval("c + c").sum(), (pdf["c"] + pdf["c"]).sum()
    )


def test_non_rowwise_methods_are_not_mapped_per_chunk():
    """Guard against re-adding methods that would give per-chunk-local answers."""
    from cudf.multigpu._frame import _FallbackMixin

    unsafe = ["duplicated", "factorize", "melt", "interpolate", "convert_dtypes"]
    for name in unsafe:
        owner = next(
            (
                klass
                for klass in mgpu.ChunkedDataFrame.__mro__
                if klass is not _FallbackMixin and name in vars(klass)
            ),
            None,
        )
        assert owner is None, (
            f"{name} is mapped per chunk but is not row-wise; it would compute "
            "a chunk-local answer instead of a global one"
        )


# ----------------------------------------------------------------------
# cudf.pandas backend
#
# install() has to run before anything imports pandas, so these run in a
# subprocess rather than in the test session.
# ----------------------------------------------------------------------
_PANDAS_BACKEND_SCRIPT = '''
import json, warnings
warnings.filterwarnings("ignore")
import cudf.multigpu.pandas_compat as mgp
mgp.install(npartitions={nparts})

import numpy as np
import pandas as pd

rng = np.random.default_rng(3)
n = 20_000
df = pd.DataFrame({{
    "k": rng.integers(0, 30, n),
    "g": [f"g{{i}}" for i in rng.integers(0, 5, n)],
    "v": rng.normal(size=n),
}})

fast = df._fsproxy_fast
out = {{
    "is_proxy": hasattr(df, "_fsproxy_fast"),
    "fast_type": type(fast).__name__,
    "nchunks": getattr(fast, "nchunks", 0),
    "ndevices": len(set(getattr(fast, "devices", ()))),
    "len": len(df),
    "sum": float(df["v"].sum()),
    "mean": float(df["v"].mean()),
    "groupby": {{str(k): float(v) for k, v in df.groupby("g")["v"].sum().to_dict().items()}},
    "groupby_type": type(df.groupby("g")["v"].sum()).__name__,
    "filter_len": len(df[df["v"] > 0]),
    "head_repr_is_pandas_like": not repr(df.head(3)).startswith("<Chunked"),
    "sorted_head": [float(x) for x in df.sort_values("v")["v"].head(3)],
}}
df["w"] = df["v"] * 2
out["assign_sum"] = float(df["w"].sum())
print("RESULT" + json.dumps(out))
'''


def test_cudf_pandas_backend_uses_all_gpus(runtime):
    import json
    import subprocess
    import sys

    script = _PANDAS_BACKEND_SCRIPT.format(nparts=NPARTS)
    proc = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=900,
    )
    assert proc.returncode == 0, proc.stderr[-3000:]
    line = next(
        line for line in proc.stdout.splitlines() if line.startswith("RESULT")
    )
    out = json.loads(line[len("RESULT") :])

    # pandas objects really are backed by the multi-GPU frame
    assert out["is_proxy"]
    assert out["fast_type"] == "ChunkedDataFrame"
    assert out["nchunks"] == NPARTS
    assert out["ndevices"] == NPARTS

    # and the accelerated results are right
    assert out["len"] == 20_000
    assert out["filter_len"] > 0
    assert len(out["groupby"]) == 5
    assert out["groupby_type"] == "Series"
    np.testing.assert_allclose(out["assign_sum"], out["sum"] * 2, rtol=1e-9)
    np.testing.assert_allclose(
        out["mean"], out["sum"] / out["len"], rtol=1e-9
    )
    assert out["sorted_head"] == sorted(out["sorted_head"])

    # a proxied frame must not print like an internal type
    assert out["head_repr_is_pandas_like"]
