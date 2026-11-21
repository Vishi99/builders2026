# Shopping Sessions

The scorer image builds and runs the Rust pipeline in `src/`. It reads mapped CSV shards, selects exact event winners, resolves linked IDs, groups and attributes sessions, and writes both daily Parquet tables. The [challenge brief](problem.txt) and [rules](rules.md) define the input and output contract.

```sh
make -C starter-duckdb build
make -C starter-duckdb fixtures
```

The fixture checker needs Python and Polars. See the [local runner guide](starter-duckdb/README.md) for the official dev check and Docker settings. The earlier C++ and DuckDB implementations are available in the starter for comparison.

For a native Linux build, use Rust 1.85 or newer and run `cargo build --release --locked` from the repository root. Set `DATA_DIR`, `OUTPUT_DIR` and `NCPU` to run the executable directly. The production amd64 build targets x86-64-v3 and selects a supported SIMD CSV scanner at runtime.

Compare event scanners on the same Xeon using otherwise identical images.
The scanner selection applies to events, including layout sampling.

```sh
make -C starter-duckdb build SHOPPING_EVENT_SCANNER=avx2 IMAGE=shopping-avx2
make -C starter-duckdb build SHOPPING_EVENT_SCANNER=avx512 IMAGE=shopping-avx512
```

`SHOPPING_PROFILE_SCAN=1` reports the summed worker time spent on event and click chunks in the ranked path. For native runs, set it in the environment. For Docker runs, pass it with `-e SHOPPING_PROFILE_SCAN=1`.

Test `SHOPPING_FEATURES=split-cuts` for specialized delimiter extraction or
`SHOPPING_FEATURES=shared-worker-budget` for bounded link and scan workers.
Build the `avx512-intrinsics` experiment with `RUST_VERSION=1.89.0` and
`SHOPPING_EVENT_SCANNER=avx512`. Use that same Rust version for its assembly
control. The scorer uses the defaults in the submitted Dockerfile.
