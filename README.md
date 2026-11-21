# Shopping Sessions

The scorer image builds and runs the Rust pipeline in `src/`. It reads mapped CSV shards, selects exact event winners, resolves linked IDs, groups and attributes sessions, and writes both daily Parquet tables. The [challenge brief](problem.txt) and [rules](rules.md) define the input and output contract.

```sh
make -C starter-duckdb build
make -C starter-duckdb fixtures
```

The fixture checker needs Python and Polars. See the [local runner guide](starter-duckdb/README.md) for the official dev check and Docker settings. The earlier C++ and DuckDB implementations are available in the starter for comparison.

For a native Linux build, use Rust 1.85 or newer and run `cargo build --release --locked` from the repository root. Set `DATA_DIR`, `OUTPUT_DIR` and `NCPU` to run the executable directly. The production amd64 build targets x86-64-v3 and selects a supported SIMD CSV scanner at runtime.
