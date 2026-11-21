# Shopping Sessions pipeline

The root Dockerfile builds the Rust executable in [src](../src). Both output tables are written directly to Parquet. Consult the [authoritative rules](../rules.md) for the exact input and output contract.

## Test with Docker

Install Docker with buildx and Python 3.10+ with Polars. From this directory:

```sh
python3 -m pip install polars
make fixtures
```

Download `generator.zip` and `dev-answer.zip` from the track page. Build the generator from its included instructions, put `generate` on PATH, and extract the expected CSVs to `dev-answer/`. Then run:

```sh
make dev
make run-dev
make check-dev
```

`make run-dev CPUS=4 MEM=8g` uses lower local resource limits. For an Arm Linux image, use `PLATFORM=linux/arm64`. The scorer uses the default linux/amd64 build. The amd64 target is x86-64-v3 by default. The loader picks its SIMD parser according to detected CPU features.

To run another generated input through the image, select a fresh output directory:

```sh
make build IMAGE=shopping-sessions:joseph2
OUTPUT=$(mktemp -d)
IMAGE=shopping-sessions:joseph2 CPUS=8 MEM=24g scripts/docker_run.sh /path/to/input "$OUTPUT"
python3 scripts/compare.py "$OUTPUT" /path/to/reference
```

## Native Linux tests

```sh
cd ..
cargo build --locked --release
cd starter-duckdb
NCPU=4 python3 scripts/run_fixtures.py fixtures -- ../target/release/shopping-sessions
```

The original DuckDB pipeline, C++ sources, and PyArrow writer are kept as cross-checks. The fixture runner and reference comparison check values, logical types, nulls, and multiplicities independent of row order.

## Scorer interface

The container runs the executable with no arguments. It reads from `DATA_DIR` (default `/data`), writes both table directories under `OUTPUT_DIR` (default `/output`), and accepts `NCPU` for worker count. Runtime is measured from container start to exit, including both Parquet writes. Build time is outside runtime.

For a submission from a non-default branch, enter the full commit SHA in the organizer's form.
