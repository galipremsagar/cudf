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

**String UDFs are rejected.** cuDF compiles them with raw device pointers baked
into the PTX and caches the kernel globally, so reusing it on another GPU reads
foreign memory. `apply` raises rather than returning a wrong answer. Numeric
UDFs are fine.

**Keep `CUDF_SPILL=0`.** cuDF's spill manager has no notion of which GPU a
buffer belongs to and can restore one onto the wrong device. `init()` warns if
spilling is on.

## Known gaps

* `Index` coverage is thin (13%); most index methods take the fallback.
* No lazy evaluation or expression fusion, so no cross-operation optimization.
* No spilling to host, so the working set plus intermediates must fit in
  aggregate GPU memory.
* `rank`, `duplicated`, `pivot`, `rolling`, `resample` and `melt` are
  fallback-only; they need shuffles or boundary exchanges that are not written
  yet. They are deliberately *not* mapped per chunk, which would silently
  return chunk-local answers.
* Chunk placement is static round-robin; there is no rebalancing when a filter
  skews chunk sizes. Call `.rechunk()` explicitly.

## Change outside this package

`cudf/utils/scalar.py` — `pa_scalar_to_plc_scalar` cached `plc.Scalar` objects
(which own *device* memory) keyed only on the pyarrow value, so a scalar
allocated on GPU 0 could be handed to a kernel on GPU 3. The cache is now keyed
on the current device too, behind a flag so single-GPU processes keep the
existing fast path.
