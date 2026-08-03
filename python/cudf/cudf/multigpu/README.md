# `cudf.multigpu` — one DataFrame across many GPUs (experimental POC)

cuDF runs on a single GPU, so the largest frame you can hold is bounded by one
device. This package removes that bound by partitioning a frame **by rows**
across every GPU in the box and treating their memory as one pool.

On an 8 × 97 GiB machine that is ~780 GiB of usable capacity. A **500 GiB /
13.4 billion row** frame works; the same frame cannot be gathered onto any
single device.

Everything here is Python built on public pylibcudf/cuDF primitives plus
peer-to-peer copies. **libcudf is not modified.**

## Quick start

```python
import cudf.multigpu as mgpu

mgpu.init()                                  # one pinned worker thread per GPU

df = mgpu.read_parquet("data/*.parquet")     # row groups land on all GPUs
df["z"] = df["x"] * df["y"]                  # per-chunk, no data movement
df.groupby("key").agg({"z": "sum"})          # pre-aggregate, shuffle, combine
df.merge(dim, on="key")                      # broadcast or co-partitioned join
df.sort_values("z")                          # range-partition, then sort locally

df.to_pandas()      # every chunk -> host (never needs to fit on one GPU)
df.compute()        # gather onto one GPU (must fit there)
```

Accelerated pandas, backed by all GPUs:

```python
import cudf.multigpu.pandas_compat as mgpandas
mgpandas.install()          # must precede `import pandas`

import pandas as pd         # unmodified pandas code, now multi-GPU
```

Run the demo and the coverage report:

```
python -m cudf.multigpu.demo --gib 500 --chunks-per-device 4 \
       --initial-pool-fraction 0.80 --pool-fraction 0.95
python -m cudf.multigpu.coverage --list
```

## TPC-H / PDS-H

cuDF's own PDS-H queries (`cudf.pandas._benchmarks.pdsh`) run unmodified on
this backend. Generate data with
[tpchgen-rs](https://github.com/clflushopt/tpchgen-rs), convert the DECIMAL and
DATE columns the benchmarks cannot do arithmetic on, then run:

```
pip install tpchgen-cli
tpchgen-cli parquet -s 100 --output-dir=/data/tpch/sf100
python -m cudf.multigpu.tpch_convert --src /data/tpch/sf100 --dst /data/tpch/sf100c

python -m cudf.multigpu.tpch_pdsh --path /data/tpch/sf100c --scale 100
python -m cudf.multigpu.tpch_pdsh --path /data/tpch/sf1c --scale 1 --validate
```

Results on 8 × 97 GiB, all with `--strict` (any fall-back-to-pandas raises):

| scale | on GPU | on CPU | failed | total |
| --- | --- | --- | --- | --- |
| SF1   | 22/22 (100%) | 0 | 0 | 9.9 s (also 22/22 matching single-GPU cuDF) |
| SF100 | 22/22 (100%) | 0 | 0 | 28.3 s |
| SF300 | 19/22 (86%)  | 0 | 3 (OOM) | 30.8 s |

SF300 is ~1.8 billion lineitem rows; reading it lands 97 GiB across the 8 GPUs
in 1.6 s. The three failures are genuine capacity, not correctness: q8, q9 and
q10 are the widest joins, and with eager evaluation their intermediates exceed
the aggregate 713 GiB. Fixing that needs spilling or lazy/fused execution,
neither of which this POC has. Note also that a `gc.collect()` between queries
is required at this scale -- chunked frames form reference cycles, so without
it the previous query's device memory is still held when the next one starts.

**A fall-back to pandas returns the right answer — it just computes it on the
host.** It is therefore invisible if you only check results, and it was: 18 of
the 22 queries initially "passed", and matched the single-GPU reference, while
never touching the GPUs.

Two detectors, and you need both:

* `--strict` sets `CUDF_PANDAS_FAIL_ON_FALLBACK=1`. This only covers
  cudf.pandas' *function-call* path. **Attribute access falls back through the
  proxy's `__getattr__`, which never consults that variable**, so `--strict`
  alone reported a clean 22/22 at SF100 while q1 was in fact running on the CPU
  for 91 of the run's 117 seconds.
* The runner therefore also measures host RSS growth per query and prints it.
  Any fallback has to copy the frame to host, so a multi-GiB jump gives it away
  no matter which path took it. That is what the `hostRSS` column is for, and
  what the "on GPU: n/n" line is computed from.

`--validate` checks the answers; the two detectors above check that the answers
were computed where you think.

For reference, stock single-GPU `cudf.pandas` also completes all 22 queries at
SF1 with no CPU fallback:

```
CUDF_PANDAS_FAIL_ON_FALLBACK=1 python -m cudf.pandas._benchmarks.pdsh all \
    --executor in-memory --path /data/tpch/sf1c
```

## How it works

| module | role |
| --- | --- |
| `_runtime` | one `cudaSetDevice`-pinned worker thread and RMM pool per GPU |
| `_transfer` | moves chunks between devices via cuDF's device-serialization protocol |
| `_frame` | `ChunkedDataFrame` / `ChunkedSeries` / `ChunkedIndex` and dispatch |
| `_shuffle` | hash and range repartitioning (all-to-all) |
| `_ops` | group-by, join, sort, distinct, quantile |
| `_scan` | `cumsum` family, `shift`, `diff` (carry / boundary exchange) |
| `_stats` | `skew`, `kurtosis`, `cov`, `corr`, `ffill`/`bfill`, `mode` |
| `_io` | parquet row-group and CSV byte-range readers, one share per GPU |

Each operation gets rows that need to meet onto the same GPU, then lets
ordinary single-GPU cuDF do the work. Because pylibcudf releases the GIL
around libcudf calls, the per-device threads genuinely overlap.

Every method falls into one of three buckets, which `coverage.py` reports:

* **distributed** — a real multi-GPU algorithm (shuffle / reduce / scan).
* **per-chunk** — strictly row-wise, applied independently to each chunk.
* **fallback** — gathers onto one GPU, runs stock cuDF, and warns. Still
  correct, but the frame must fit on that device.

## Things worth knowing

**Never touch a chunk from the wrong thread.** A chunk's memory belongs to one
device; reading it from a thread whose current device differs is an illegal
access, not a slow path. All access goes through `runtime.run(device, ...)`.

**Peer access is deliberately never enabled.** On this machine
`cudaDeviceCanAccessPeer` reports true and `cudaDeviceEnablePeerAccess`
succeeds, yet subsequent device-to-device copies silently transfer *zeros*.
The driver's staged path is both correct and fast (~43 GB/s over PCIe), so we
use that and `DeviceRuntime.validate_peer_copies()` checks real bytes move
before trusting any transfer. If something else in the process enables peer
access, that check fails loudly.

**Eager evaluation means intermediates are full-width.** `a * b + c`
materializes two frame-sized temporaries. At 500 GiB that is 200+ GiB. Use
`map_chunks` to evaluate an expression inside one chunk at a time:

```python
df.map_chunks(lambda c: (c["a"] * c["b"] + c["c"]) > 0.25).sum()
```

**String UDFs work, but only because cuDF's caches are now device-aware.**
cuDF bakes libcudf's character-table device pointers into the generated PTX and
caches the compiled kernel. libcudf's tables are already per-CUDA-context, so
the bug was purely in the Python cache keys; `cudf/utils/device.py` now
supplies the device component of those keys.

**Keep `CUDF_SPILL=0`.** cuDF's spill manager has no notion of which GPU a
buffer belongs to and can restore one onto the wrong device. `init()` warns if
spilling is on.

## Known gaps

* `Index` coverage is thin (15%); most index methods take the fallback.
* No lazy evaluation or expression fusion, so no cross-operation optimization.
* No spilling to host, so the working set plus intermediates must fit in
  aggregate GPU memory.
* `rank`, `pivot`, `pivot_table`, `rolling` and `resample` are fallback-only;
  they need shuffles or boundary exchanges that are not written yet. They are
  deliberately *not* mapped per chunk, which would silently return chunk-local
  answers. (`duplicated`, `factorize`, `melt`, `interpolate` and
  `convert_dtypes` were in this group and are now implemented properly.)
* `.loc` supports `:` or a boolean mask for rows; label-based row lookup would
  need a global index.
* Chunk placement is static round-robin; there is no rebalancing when a filter
  skews chunk sizes. Call `.rechunk()` explicitly.

## Changes outside this package

All three are cuDF bugs that only show up with more than one device in play.

`cudf/utils/device.py` (new) — one place that decides the device component of a
cache key. Single-GPU processes never query the device, so nothing gets slower.

`cudf/utils/scalar.py` — `pa_scalar_to_plc_scalar` cached `plc.Scalar` objects
(which own *device* memory) keyed only on the pyarrow value, so a scalar
allocated on GPU 0 could be handed to a kernel on GPU 3. Now keyed on the
device as well.

`cudf/core/udf/utils.py` — `make_cache_key`, which both UDF caches funnel
through, likewise ignored the device while the cached PTX has device pointers
compiled into it.

`cudf/core/udf/nrt_utils.py` — `nrt_enabled()` mutated a process-global numba
config and restored it in a `finally`. With several threads compiling at once,
whichever finished first switched NRT off underneath the others and their
kernels failed to link (`Unresolved extern function 'NRT_decref'`). Now
reentrant, with a lock and a depth count.
