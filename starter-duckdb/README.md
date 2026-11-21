# Shopping Sessions pipeline

The scorer image runs a standalone C++ executable. It resolves linked IDs, scans CSV shards in parallel, builds sessions and campaign attribution, and writes the two daily Parquet tables. The original DuckDB implementation and the PyArrow writer are available for comparison.

If this starter and `rules.md` ever disagree, `rules.md` wins. Ask the organizers if `rules.md` leaves you unsure.

## What to do first

- Read `rules.md` for the full rules.
- Start with `pipeline/native_main.cpp` for the scorer entrypoint and `pipeline/native_pipeline.cpp` for the processing stages.
- Build the image and test against all 36 edge-case fixtures and the 1M-event dev set before submitting.

## Run and test locally

You need Docker with buildx, and Python 3.10+ with `polars` for the test scripts. Install it with `pip install polars`.

From this directory, build the image and run all 36 fixtures:

```sh
make build        # uses the repository root as its Docker build context
make fixtures     # runs the image on all 36 fixtures with scorer flags, diffs each against expected/
```

Build, run, and check the 1M-event dev set. `make dev` needs `generate` on your PATH: build it from the track page's generator.zip (Rust 1.85 or newer; its README has the two commands).

```sh
make dev                                # generate --seed 1 --events 1000000 --clicks 250000 --output dev
make run-dev                            # runs your image on dev/ with scorer flags and times it
make check-dev                          # exact diff against dev-answer/, the track page's dev-answer.zip unzipped here
```

If your laptop cannot spare 8 cores and 24 GB, run with lower limits using `make run-dev CPUS=4 MEM=8g`. Your local times will not match the scorer anyway; only the board's times count.

- **x86 Linux and Windows machines.** The default amd64 build targets `x86-64-v4` (AVX-512). Use `NATIVE_AMD64_ARCH=x86-64-v3` on a machine that supports AVX2 but lacks AVX-512.
- **Apple-silicon Macs.** Emulating amd64 is slow. For fast local loops, build a native image with `make build run-dev PLATFORM=linux/arm64`. The Dockerfile pins no platform, so it builds either way. Check the amd64 image before you submit.

To compare the AVX2 and AVX-512 builds on the same AVX-512-capable machine, build both images from the same source:

```sh
make build NATIVE_AMD64_ARCH=x86-64-v3 IMAGE=shopping-avx2
make build NATIVE_AMD64_ARCH=x86-64-v4 IMAGE=shopping-avx512
```

The scorer builds from the submitted revision and uses that revision's Dockerfile default.

The default AVX-512 event scanner reuses delimiter masks across rows.
To compare the two scanners on the same Xeon, build from the same source with:

```sh
make build NATIVE_ROLLING_AVX512=1 IMAGE=shopping-rolling
make build NATIVE_ROLLING_AVX512=0 IMAGE=shopping-per-row
```

For a scorer comparison, submit two revisions that differ only in the
`NATIVE_ROLLING_AVX512` default in the root Dockerfile.

To iterate without Docker on Linux with a C++20 compiler:

```sh
python3 -m pip install -r requirements.txt polars
make native
NCPU=4 python3 scripts/run_fixtures.py fixtures -- python3 pipeline/native_runner.py
```

`pipeline/pipeline.py` uses the native library when it has been built in place and runs the DuckDB reference otherwise.

## Submitting and scoring

Submit your public GitHub repository on the Buildathon track page. Enter a commit SHA to score a specific revision, or leave it empty to score the newest commit on the default branch.

The image's `CMD` runs with no arguments. Exit code 0 means "done, check my output". Build time is never timed; only the run is timed. The track page lists the current practice limits and how the final is scored.

## Container limits

Scorers are Google Cloud n2 Intel Xeon machines, Cascade Lake or newer, with AVX-512 available. Your container gets:

| Resource | Setting |
| --- | --- |
| `/data` | The dataset, read-only |
| `/output` | Empty, writable. Write `/output/user_daily/` and `/output/campaign_daily/` as Parquet |
| `/scratch` | Empty, writable, fast local disk for spills and temp files |
| CPU / RAM | 8 vCPUs, hyperthreads of an Intel Xeon, 24 GB, no swap |
| Network | None |
| Env vars | `DATA_DIR=/data`, `OUTPUT_DIR=/output`, `SCRATCH_DIR=/scratch`, `HOME=/scratch`, `NCPU=8`, `MEM_GB=24` |
| Filesystem | Root is read-only apart from `/tmp`, 64 MB in memory. The process runs as uid 1000, not root |

## Common pitfalls

- **CSV parsing.** The native scanner splits at physical LF boundaries and checks the field count and quote placement. It retains CR, NUL and spaces as field bytes.
- **Memory.** Native workers read input shards into reusable buffers and keep compact event and click records in memory. The pipeline releases each event partition after deduplication or sessionization.
- **Build dependencies.** The image builds the executable with LZ4 and nlohmann JSON. The executable needs no network at run time.
- **`\d` is not `[0-9]`.** In several regex engines, including Rust's used by Polars, `\d` matches any Unicode digit. The rules mean ASCII digits only.
- **Extra files in output folders.** `_SUCCESS` and other non-Parquet files in the output folders are ignored.

## Self-checks

- Summed over all rows, `sessions` and `revenue_cents` are equal in both tables.
- `day` must be a Parquet `DATE`, not a string or timestamp.
- Row order and the number of Parquet files do not matter. Exact column names do, and there must be no extra columns.

## Files in this starter

- `pipeline/native_input.hpp` parses CSV, timestamps and prices.
- `pipeline/native_events.hpp` deduplicates events, builds sessions and summarizes user days.
- `pipeline/native_attribution.hpp` credits sessions and counts assists.
- `pipeline/pipeline.py`, `pipeline/events.sql` and `pipeline/attribution.sql` contain the DuckDB reference.
- The [repository Dockerfile](../Dockerfile) builds the `linux/amd64` scorer image.
- `fixtures/` holds the 36 edge cases, each an `input/` folder and the `expected/` tables. In this monorepo they live in `../../fixtures`; copy them in when publishing the starter as its own repo.
- `scripts/` runs your image the way the scorer does and diffs the output.
