# Shopping Sessions

The scorer image builds and runs the Rust pipeline in `src/`. It reads mapped CSV shards, selects exact event winners, resolves linked IDs, groups and attributes sessions, and writes both daily Parquet tables. The [challenge brief](problem.txt) and [rules](rules.md) define the input and output contract.

```sh
make -C starter-duckdb build
make -C starter-duckdb fixtures
```

The fixture checker needs Python and Polars. See the [local runner guide](starter-duckdb/README.md) for the official dev check and Docker settings. The earlier C++ and DuckDB implementations are available in the starter for comparison.

For a native Linux build, use Rust 1.89 or newer and run `cargo build --release --locked` from the repository root. Set `DATA_DIR`, `OUTPUT_DIR` and `NCPU` to run the executable directly. The production amd64 build targets x86-64-v3 and selects a supported SIMD CSV scanner at runtime.

The current generator uses keyed, variable-width identifiers. The default
pipeline stages events in a single scan. Short ASCII event IDs fit in an
exact packed key. Longer IDs use a hash and a location, with byte comparison
resolving collisions. The common event record is 24 bytes. Purchase details
and outlying ingest timestamps go in per-worker side buffers. Click IDs stay
in the mapped input until attribution needs a lexical tie-break. Set
`J2_DEDUP=filter` to compare the two-pass candidate filter. The older ranked
implementation is available with `J2_DEDUP=rank` for historical benchmarks.

The scorer image defaults to buffered reads (`J2_INPUT=pread`). Workers
reuse their input buffers and retain the original bytes of longer event IDs
for deduplication. Compare against mapped input by building with
`SHOPPING_INPUT=mmap` or passing `-e J2_INPUT=mmap` to the container.
Direct native runs use mapped input unless `J2_INPUT=pread` is supplied.

Compare event scanners on the same Xeon using otherwise identical images.
The scanner selection applies to events, including layout sampling.
The scorer image selects `tokens` for events and clicks. It visits
separators and quotes in byte order using a SIMD lookup on each input
block. Native automatic selection uses it on arm64.

```sh
make -C starter-duckdb build SHOPPING_EVENT_SCANNER=avx2 IMAGE=shopping-avx2
make -C starter-duckdb build SHOPPING_EVENT_SCANNER=avx512 IMAGE=shopping-avx512
make -C starter-duckdb build SHOPPING_EVENT_SCANNER=tokens IMAGE=shopping-tokens
```

Stage timings are written to stderr, including validation, duplicate
arbitration, session aggregation and Parquet output. Set
`SHOPPING_PROFILE_SCAN=1` to log summed event and click worker durations.
Those timings include time spent waiting on input and memory.

Test `SHOPPING_FEATURES=split-cuts` for specialized delimiter extraction.
Test `SHOPPING_FEATURES=short-clicks` for the amd64 64-bit decoder for
click rows shorter than 64 bytes.
Build the `avx512-intrinsics` experiment with `RUST_VERSION=1.89.0` and
`SHOPPING_EVENT_SCANNER=avx512`. Use that same Rust version for its assembly
control. The scorer uses the defaults in the submitted Dockerfile.
