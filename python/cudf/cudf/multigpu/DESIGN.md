# `cudf.multigpu` — one DataFrame across every GPU

**Status: experimental proof of concept.** Nothing in libcudf is modified.

---

## 1. The problem

cuDF runs on one GPU, so the largest frame you can hold is bounded by one
device. On a machine with 8 × 97 GiB that means a 97 GiB ceiling while 776 GiB
sits idle.

The failure is not a clean error. `cudf.pandas` responds to running out of
device memory by **silently falling back to pandas** — the query still returns
the right answer, on the CPU, having copied the whole frame to host. Measured
here, at TPC-DS SF100 a single GPU keeps only **32 of 99 queries** on the GPU
and takes 19.5 hours; the other 65 run in pandas without saying so.

This package removes the ceiling by partitioning a frame **by rows** across
every device and treating their memory as one pool.

```
          single GPU                        cudf.multigpu
     ┌───────────────────┐         ┌────┐┌────┐┌────┐┌────┐
     │                   │         │ c0 ││ c1 ││ c2 ││ c3 │   rows 0..n/8 …
     │   whole frame     │         └────┘└────┘└────┘└────┘
     │   must fit here   │         ┌────┐┌────┐┌────┐┌────┐
     │      97 GiB       │         │ c4 ││ c5 ││ c6 ││ c7 │
     └───────────────────┘         └────┘└────┘└────┘└────┘
        GPU 0                       GPU0  GPU1  GPU2 …  GPU7
                                          776 GiB total
```

---

## 2. Design in one page

Every operation follows the same shape: **get the rows that must meet onto the
same GPU, then let ordinary single-GPU cuDF do the actual work.**

A `ChunkedDataFrame` is a list of ordinary `cudf.DataFrame` chunks plus the
device each lives on. There is no new column type, no new kernel, no change to
libcudf — the chunks are exactly what cuDF already produces, and every
computation is a normal cuDF call issued on the right device.

```
  ChunkedDataFrame
  ├── _chunks   [cudf.DataFrame, cudf.DataFrame, …]   one per partition
  ├── _devices  [0, 1, 2, …]                          where each chunk lives
  └── _runtime  DeviceRuntime                         one pinned thread per GPU
```

Three primitives carry almost everything:

| primitive | meaning | cost |
|---|---|---|
| `_run_chunks(fn)` | run `fn` on every chunk, on its own device | none — pure parallelism |
| `hash_shuffle(keys)` | move rows so equal keys share a device | all-to-all transfer |
| `_single_gpu_fallback` | gather to one device and run cuDF there | must fit on one GPU |

Operations are classified by which they need:

- **row-wise** (`+`, `astype`, `.str`, filtering) → `_run_chunks`, no movement
- **key-based** (group-by, join, sort, distinct) → shuffle, then local cuDF
- **everything else** → gather onto one GPU, with a warning

---

## 3. Execution model

### 3.1 One pinned thread per device

`DeviceRuntime` owns a single-threaded executor per GPU, each pinned with
`cudaSetDevice` once at creation. Work is submitted to the thread that owns the
target device, so no call ever has to switch devices.

This is only useful because **pylibcudf releases the GIL**, so eight Python
threads genuinely drive eight GPUs at once rather than taking turns.

`submit()` is reentrancy-safe: work submitted *from* a device worker to its own
device runs inline rather than deadlocking on the single-threaded pool. That
matters more than it sounds — see §7.4.

### 3.2 Memory resources

Per device, selected at `init()`:

| resource | behaviour | when |
|---|---|---|
| `pool` (default) | RMM pool, capped at a fraction of device memory | normal |
| `async` | `cudaMallocAsync`, returns memory between queries | long query sequences |
| `managed` | `cudaMallocManaged`, oversubscribes into host RAM | when the pool runs out |

**Managed memory must be used unwrapped.** A pool on top of it grows by
doubling and asks the driver for one enormous contiguous region, which fails as
a *sticky* CUDA error that kills the context. Plain managed allocation
oversubscribes happily — measured at **400 GiB on a 95 GiB device**. It costs
roughly 3× (TPC-H SF500: 108 s → 320 s), so it is the fallback, not the default.

### 3.3 Moving data between devices

This is the part of the system that does not exist in single-GPU cuDF, so it is
worth describing exactly.

#### What a transfer copies

Nothing walks the column structure by hand. A move uses cuDF's own
`device_serialize` protocol, which returns a **host-side header** plus a flat
list of frames — some device buffers, some host bytes:

```
  cudf.DataFrame                 header  {"is-cuda": [1,1,0,1,…], dtypes, …}
    ├── column "a" data     ──►  frame 0  device buffer   3,145,728 B
    ├── column "a" mask     ──►  frame 1  device buffer     393,216 B
    ├── column "b" offsets  ──►  frame 2  device buffer   3,145,732 B
    ├── column "b" chars    ──►  frame 3  device buffer  18,204,113 B
    └── index               ──►  frame 4  device buffer   3,145,728 B
```

The header is ordinary Python objects; the frames are opaque byte ranges. That
means strings, categoricals, decimals, datetimes, lists and structs all move
with exact fidelity, because cuDF describes its own layout and this code never
has to know what a frame means — only how many bytes it is. Nested and
dictionary children appear in the same flat list.

#### The three phases

A move is not "copy the bytes". It is three pieces of work on **two different
device threads**:

```
   source thread (GPU 2)                    destination thread (GPU 5)
   ─────────────────────                    ──────────────────────────
   1. _extract
      device_serialize(obj)   ──► header + [(ptr, nbytes), …]
      cudaDeviceSynchronize()      ← producing kernels may still be in flight
                                   │
                                   ▼
                                        2. _receive
                                           one DeviceBuffer for the whole payload
                                           copy each frame into its slot
                                           cudaStreamSynchronize(0)
                                           device_deserialize(header, views)
                                   │
   3. _release  ◄──────────────────┘
      drop the serialization view
      on the device that owns it
```

Three details each fix a real failure:

- **The source synchronizes before the copy.** The work that produced the frame
  may still be queued on that device's stream; copying from it early reads
  memory that is not written yet.
- **The release runs on the source device**, not wherever the Python happens to
  be. Freeing a buffer from the wrong device returns it to the wrong pool.
- **The destination allocates once.** All device frames are packed back-to-back
  into a single `DeviceBuffer`, 256-byte aligned:

  ```
  base ┌──────────┬─┬──────────┬────────────────────┬──────────┐
       │ frame 0  │▓│ frame 2  │      frame 3       │ frame 4  │
       └──────────┴─┴──────────┴────────────────────┴──────────┘
         ▓ = alignment padding
  ```

  So a transfer costs **one allocation and a batch of copies**, not one
  allocation per buffer. Each frame is then handed back to
  `device_deserialize` as a `_BufferView` — a window onto the base allocation
  exposed through `__cuda_array_interface__`, holding a reference so the base
  outlives it.

  Two edge cases live here. A zero-length frame must present a **null** pointer,
  because cuDF emits such frames for the data buffer of compound (list/struct)
  parent columns and libcudf rejects a compound column whose data pointer is
  non-null. And the keepalive attribute is deliberately *not* called `owner`:
  cuDF's `get_buffer_owner` walks an `owner` chain, and this must look like
  fresh device memory rather than a view into someone else's buffer.

#### The copy itself

```python
if neither pointer is managed:
    cudaMemcpyAsync(dst, src, n, cudaMemcpyDefault, 0)
else:
    cudaMemcpy(host_staging, src, n, DeviceToHost)
    cudaMemcpy(dst, host_staging, n, HostToDevice)
```

`cudaMemcpyDefault` lets the driver work out the direction from the pointers,
which is what makes a device-to-device copy across GPUs a single call.

**Managed allocations are staged through host memory.** A direct
managed-to-managed copy is silently wrong on hardware where P2P is advertised
but not functional — it yields zeros — and a managed pointer is not owned by
any device, so the peer-copy API cannot express the intent either.

> **Peer access is never enabled.** `cudaDeviceEnablePeerAccess` succeeds on
> this machine and then silently returns **zeros** for cross-device reads. The
> driver's staged path is correct and reaches full PCIe bandwidth, so there is
> nothing to gain by risking it. `validate_peer_copies()` writes a known
> pattern, reads it back from another device and refuses to start if it does
> not match — a corruption this quiet is worth paying a startup check to catch.

#### Batching: why an all-to-all is not N² separate moves

`move_batch` takes a whole list of `(obj, src, dst)` and runs each phase
**concurrently across devices**: every source serializes at once, then every
destination receives at once, then every source releases at once. Same-device
moves are recognised and skipped entirely rather than copied.

This is what makes a shuffle a parallel exchange rather than a sequence of
pairwise transfers — during phase 2 all eight PCIe links are busy.

Two specialisations sit on top:

- **`broadcast`** serializes the source **once** and hands the same header and
  pointers to every destination, so replicating to seven GPUs costs one
  serialization and seven receives. This is what a broadcast join uses.
- **`gather_concat`** moves every chunk to one device and concatenates — the
  "collapse to a single GPU" path used by `.compute()` and the fallback. The
  result must of course fit there.

#### Streaming the exchange

A shuffle does not move everything at once. Moving all partitions
simultaneously means the hash partitions, the serialization views of them, the
destination buffers **and** the concatenated result are all live at the same
time — roughly **four times the frame**, which is precisely what made the widest
joins fail at SF300.

So the exchange runs a group of destinations at a time, as wide as the number of
distinct destination devices — normally all of them:

```
  group 1 → GPUs 0..7   move ──► concat ──► release sources
  group 2 → GPUs 0..7   move ──► concat ──► release sources
  group 3 → …
```

Every GPU stays busy within a group, but only one group's worth is in flight,
and each group's source partitions are freed — on their own device — before the
next group allocates. Empty partitions are skipped without a transfer at all.

Whether to split beyond one partition per device is the memory-pressure
decision in §4.5.

---

## 4. The distributed operations

### 4.1 Group-by — pre-aggregate, shuffle, combine

Shuffling raw rows would move the whole frame. Instead each chunk aggregates
locally first, and only the (far smaller) partial results are shuffled:

```
  chunk0   chunk1   chunk2   chunk3        ← local partial aggregation
    │        │        │        │             (sum → sum, count → sum, …)
    └────────┴───┬────┴────────┘
                 │  shuffle partials by key   ← small: one row per key per chunk
    ┌────────┬───┴────┬────────┐
  part0    part1    part2    part3          ← combine, one device per key range
```

Decomposable aggregations (`sum`, `min`, `max`, `count`, `size`, `prod`, `any`,
`all`, and `mean` as sum/count) take this path. Anything else shuffles the raw
rows and aggregates once.

`sort=True` needs a **global** sort of the result: per-chunk sorting cannot
order keys that a shuffle has scattered across chunks.

### 4.2 Join — broadcast or co-partition

```
  broadcast (right side small)        co-partitioned (both large)

  left  ┌──┐┌──┐┌──┐┌──┐              left  ┌──┐┌──┐┌──┐┌──┐
        └┬─┘└┬─┘└┬─┘└┬─┘                    └┬─┘└┬─┘└┬─┘└┬─┘
   right │R  │R  │R  │R    replicated        │ shuffle both on the key
         ▼   ▼   ▼   ▼                       ▼   ▼   ▼   ▼
        local join per chunk                local join per partition
```

The choice is by cost, not by absolute size: replication costs
`right × ndevices`; shuffling costs roughly a copy of *both* sides plus
serialization buffers. Broadcast wins whenever it is cheaper — otherwise a
mid-sized dimension table drags a huge fact table through a full shuffle.

### 4.3 Sort — sample, range-partition, sort locally

Splitters are sampled from every chunk, rows are bucketed by key range, buckets
are shuffled so each device owns a contiguous slice of key space, and each
sorts locally. Concatenating the chunks in order is then globally sorted.

Two subtleties, both learned from wrong answers (§7.2):

- `na_position` must be applied when **bucketing**, not only within a bucket —
  buckets are concatenated in order, so a row's bucket decides where it lands.
- A null anywhere in a multi-column key makes the lexicographic `searchsorted`
  unreliable, so partitioning falls back to the leading key alone.

### 4.4 Predicate pushdown

Analytical queries are written join-first-filter-after and left to an optimizer
to reorder. Executing that literally is what breaks at scale: at TPC-DS SF300
one query materialised 81.5 GiB before its filter ran.

So `merge` returns a frame holding a **`JoinPlan`** rather than executing.
Selecting a column yields an `Expr` — a deferred expression that remembers
which *input* its columns came from. Using an `Expr` as a mask rewrites the
plan to filter that input instead of the output.

```
  df.merge(other).query(...)        →  plan: JOIN(left, right)
                                       mask origin = left
                                    →  plan: JOIN(filter(left), right)
```

The critical property is that an `Expr` can be evaluated against **any** frame.
When a predicate cannot be pushed — it spans both inputs, or would filter the
null-extended side of an outer join — it is evaluated against the materialized
join instead, and the answer is identical. Laziness only changes *when* work
happens. This alone took TPC-DS SF300 from 19/22 to 22/22.

### 4.5 Shuffle sizing

Splitting a shuffle into more partitions than devices lowers the peak (only one
group is in flight) but the groups run in sequence, so it costs wall time.
It is triggered by **memory pressure**, not absolute size:

```python
pressured = (nbytes > capacity * 0.25) or (nbytes > free * 0.5)
```

Capacity — a fixed property of the machine — has to be the primary signal. A
pool starts small and grows, so early in a query *free* memory looks abundant
regardless of what the query is about to need, and the frames that most need
splitting are exactly the ones a free-memory test clears. Fixing this took
TPC-DS SF300 from 112 s to 42 s.

---

## 5. The `cudf.pandas` integration

`cudf.pandas` proxies every pandas object over a *fast* implementation (cuDF)
with pandas as the slow fallback. This package swaps the fast implementation
for the chunked frame, so unmodified pandas code runs across every GPU.

```
        user code:  import pandas as pd
                            │
                    ┌───────▼────────┐
                    │  proxy object  │
                    └───┬────────┬───┘
                fast ───┘        └─── slow
          ChunkedDataFrame          pandas.DataFrame
        (8 GPUs, this package)      (host, the fallback)
```

Four properties of the proxy machinery drove the design:

1. **Attributes resolve on the fast *class*, not the instance.** Hence a
   metaclass hook that synthesizes a dispatcher for any cuDF name on demand.
   Without it every unimplemented name resolves to `_Unusable` and silently
   takes the pandas path.
2. **`_is_final_type` is an exact type test.** A subclass escapes the proxy
   entirely, which is why the deferred `JoinPlan` lives on a field of
   `ChunkedDataFrame` rather than in a subclass.
3. **Intermediates need registering too** — group-by objects, and the
   `.str`/`.dt`/`.cat` accessors and `.loc`/`.iloc` indexers. Unregistered, the
   proxy hands back a bare object and everything downstream escapes. Each
   namespace needs its **own class**: they key the map by fast type, so sharing
   one class makes them collide.
4. **Proxy `__module__` comes from walking the stack.** Wrapping the factories
   shifts that stack, so every proxy type gets attributed to this package —
   where pickle cannot find it.

### 5.1 Detecting the silent fallback

A fallback returns the **right answer** — it just computes it on the host. It
is therefore invisible if you only check results, and it was: 18 of 22 TPC-H
queries initially "passed" while never touching the GPUs.

Two detectors, and you need both:

- `CUDF_PANDAS_FAIL_ON_FALLBACK=1` covers only the *function-call* path.
  **Attribute access falls back through the proxy's `__getattr__`, which never
  consults it.**
- Host RSS growth per query. Any fallback copies the frame to host, so a
  multi-GiB jump gives it away whichever path took it.

---

## 6. Mutation and copy-on-write

An in-place write has to land on the object the caller holds. Every distributed
operation builds a *new* frame, so without care the write is silently lost.

```
   df.sort_values(inplace=True)
        │
        ├─ builds a NEW sorted frame  ──► discarded
        └─ caller still holds the old one, unchanged, and got None back
```

Three mechanisms make mutation behave:

- **`_adopt(other)`** — take over another frame's chunks in place. Applied to
  every method whose pandas signature accepts `inplace`.
- **`_absorb(obj, device)`** — put a mutated single-GPU object back into the
  chunks. Used by `.at`/`.iat`/`.loc` scalar assignment, which cannot be
  per-chunk, and by the single-GPU fallback when `inplace=True`.
- **Copy-on-write chunks** — chunk callbacks take a shallow copy before
  mutating. cuDF builds `reset_index(drop=True)`'s accessor over the *same*
  backing dict as its source, and `concat`/`loc[:]` hand back the same chunk
  objects, so without this a write to a derived frame wrote through to its
  source. A shallow copy is a fresh accessor over the same device buffers — no
  data moves.

A lazy join needed the same treatment in time rather than space: `JoinPlan`
snapshots its inputs, because pandas' merge is eager and its result cannot
change when an input is written afterwards.

---

## 7. Bugs worth knowing about

The interesting output of this project is not the speedup — it is the class of
defect that only appears when data is spread across devices, and the discovery
that **benchmarks cannot find most of them.** 121 validated TPC-H/TPC-DS
queries missed bugs a unit test found in minutes.

### 7.1 Device-blind caches

A process-wide `@lru_cache` returning **device memory** is valid only on the
device that filled it. Two in stock cuDF:

- `pa_scalar_to_plc_scalar` — a converted scalar. `df * 2` on GPU 3 faulted
  with an illegal access after GPU 0 populated the cache.
- `_get_tz_data` — timezone transition tables, keyed on zone name only.

Both are now keyed on the current device, behind a flag so single-GPU
processes pay nothing (`cudf/utils/device.py`).

### 7.2 Wrong answers with no error

| bug | symptom |
|---|---|
| `df.iloc[0, 0]` dropped the column selector | returned the **whole row** |
| decimal sums accumulated in the column's own type | `DECIMAL(7,2)` caps at 21,474,836.47; the true sum was 526,011,321,410.48 — it wrapped **negative** |
| `na_position` ignored when bucketing | null group sorted last, `head(100)` cut it |
| wrong-length assignment broadcast per chunk | `df['a'] = arange(512)` on 4096 rows → `0..511` repeated 8× |
| Series assignment aligned only within a chunk | shifted index corrupted 17% of rows; reversed index gave all-NaN |

None of these raised. The decimal overflow is the sharpest lesson: nothing fell
back, nothing errored, `--strict` was blind — only comparing against an
independent engine (DuckDB) caught it.

### 7.3 cuDF/pandas semantic differences

- **Boolean OR is not SQL three-valued logic**: `NULL | True` yields `NULL`,
  not `True`, so a row qualifying on one predicate is dropped when an unrelated
  one is NULL. cuDF differs from pandas *and* from SQL here.
- **`groupby.sum()` of an all-NULL group** is `nan` in cuDF and `0` in pandas.
  SQL agrees with cuDF.
- **NULL join keys**: pandas and cuDF both match NULL to NULL; SQL never does.
  Invisible at TPC-DS SF1 because its foreign keys have no nulls — which is why
  SF1 is not a correctness test.

### 7.4 Deadlocks

Single-threaded per-device executors deadlock if work re-enters its own worker.
Two instances: a lazily-resolved accessor evaluated inside a worker, and
grouping by an external Series. Both hang rather than fail — the worse outcome.
The fixes are a reentrant `submit()` and declining the unsupported case.

> **Raising `NotImplementedError` is not the safe default.** `cudf.pandas`
> answers it by falling back, and for an *in-place* operation the fallback
> mutates a host copy that is then discarded — so the write vanishes silently.
> Declining is only safe for operations that return a value.

---

## 8. Results

All runs are `--strict`: a query that silently ran on the CPU is a failure.

### TPC-H (22 queries)

| scale | 1 GPU | 8 GPUs |
|---|---|---|
| SF1 | **2.6 s**, 22/22 on GPU | 11 s, 22/22 |
| SF100 | 19 min, 17/22 on GPU | **25 s**, 22/22 |
| SF300 | 44 min, 8/22 on GPU | **31 s**, 22/22 |
| SF500 | *not run* | 5 min, 22/22 (managed) |
| SF1000 | *not run* | 15 min, 22/22 (managed) |

### TPC-DS (99 queries)

| scale | data | 1 GPU | 8 GPUs |
|---|---|---|---|
| SF1 | 266 MB | 11 min, 96/99 on GPU | **2 min**, 99/99 |
| SF10 | 2.6 GB | 1.8 h, 50/99 on GPU | **2 min**, 99/99 |
| SF100 | 25 GB | **19.5 h**, 32/99 on GPU | **3 min**, 99/99 |
| SF300 | 74 GB | *not run* | 4 min, 99/99 |
| SF500 | 116 GB | *not run* | 4 min, 98/99 |
| SF1000 | **243 GB** | *not run* | 6 min, 98/99 |

Times are like-for-like — each pair counts only the queries **both**
configurations completed, so neither is credited for work it skipped.

**The number that matters is not the speedup, it is `96 → 50 → 32`.** As data
grows 1× → 10× → 100×, a single GPU keeps less and less of the suite on the
GPU. At SF100, 65 of 99 queries run in pandas without telling you.

At SF1 the chunked layer is a **4× tax** — partitioning overhead dominates when
the data fits comfortably on one device. This is worth having past the point
where things stop fitting, and not before.

### Correctness

- TPC-DS is validated against **DuckDB's answers to the official SQL**, not
  against a previous run of this code: 99/99 at SF1, SF10 and SF100.
- 8-GPU answers were diffed against a **single-CPU** run over 121 queries: all
  equivalent, worst relative difference 1.9 × 10⁻¹³. The one genuine
  disagreement was a query where multi-GPU is *more* correct than pandas.
- Above SF300 there are no references — DuckDB exhausts 1.7 TiB of spill space
  building them — so those runs measure speed, not correctness.

---

## 9. What is here

```
_runtime.py     device pools, one pinned worker thread per GPU
_transfer.py    device↔device movement via cuDF's serialization protocol
_frame.py       ChunkedDataFrame / Series / Index, dispatch, indexers, mutation
_shuffle.py     hash and range repartitioning
_ops.py         group-by, join, sort, distinct
_lazy.py        deferred joins and portable expressions (predicate pushdown)
_io.py          readers that land data directly on the owning GPU
_creation.py    constructors
_scan.py        cumulative ops (prefix carried across chunks)
_stats.py       statistics reduced to per-chunk sums
_reshape.py     operations that are subtly wrong if done per chunk
pandas_compat.py  the cudf.pandas backend
```

Benchmarks and tooling: `tpch_pdsh.py`, `tpcds_run.py`, `tpcds_gen.py`,
`tpcds_reference.py`, `tpcds_queries/` (99 queries, no pandas implementation
existed anywhere), `compare_results.py`, `export_results.py`, plotting.

**Tests: 373 functions, 490 cases after parametrization.** 73 unit tests plus a
300-function mutation battery covering column assignment, element/slice/mask
assignment, `inplace` methods, aliasing and write-back, and dtype/attribute
mutation — every case checked against pandas on frames large enough to actually
be partitioned.

---

## 10. Known gaps

| gap | why |
|---|---|
| `__cuda_array_interface__` | one interface cannot describe memory on 8 devices — not fixable |
| `to_numpy()` writeable flag | lost when cudf.pandas re-wraps; **stock cudf.pandas has the same gap** |
| `ChunkedIndex` subtype erasure | one class collapses cuDF's Index hierarchy, so `DatetimeIndex` methods (`tz_localize`, `as_unit`) are unreachable; needs per-dtype subclasses |
| index-aligned assignment | needs a distributed reindex; currently refuses rather than returning NaN |
| `cudf.read_parquet` returns a chunked frame | installing the backend perturbs the `cudf.*` namespace for direct callers |
| TPC-DS q23 at SF500 | libcudf caps a column at 2³¹−1 rows; no allocator fixes it, the layer must split the intermediate |
| single-GPU baselines above SF100 | 19.5 h at SF100 already; SF300 would be days |

---

## 11. Quick start

```python
import cudf.multigpu as mgpu

mgpu.init()                                  # one worker thread per GPU
df = mgpu.read_parquet("data/*.parquet")     # row groups land on all GPUs
df.groupby("key").agg({"value": "sum"})      # pre-aggregate, shuffle, combine
```

Accelerated pandas, backed by every GPU:

```python
import cudf.multigpu.pandas_compat as mgpandas
mgpandas.install()          # must precede `import pandas`

import pandas as pd         # unmodified pandas code, now multi-GPU
```

Benchmarks:

```
python -m cudf.multigpu.tpch_pdsh --path .../sf100c --scale 100 --strict
python -m cudf.multigpu.tpcds_run --path .../sf100  --scale 100 --strict
python -m cudf.multigpu.coverage --list
```

Keep `--strict` on while evaluating coverage. A fallback is silent, returns the
right answer, and copies the entire frame to host — on a dataset that only fits
in aggregate GPU memory that is fatal rather than slow.
