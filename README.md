# Shopping Sessions

The scorer image builds and runs the Rust pipeline in `src/` (no dependencies). The [challenge brief](problem.txt) and [rules](rules.md) define the input and output contract.

How it works:

1. **Parse.** Inputs are memory-mapped and split into newline-aligned chunks. An AVX2 scanner classifies 64-byte blocks (commas, quotes, newlines) and validates each row's quoting with a prefix-XOR mask. IDs of up to 10 alphanumeric bytes pack exactly into order-preserving 64-bit keys; any other ID (such as the 32-character hex IDs) gets a 63-bit hash key, and every spelling is registered so a later phase checks that no key is shared by two spellings. If one is, or any other input breaks a fast-path assumption, the run switches to the exact single-threaded pipeline in `generic.rs`.
2. **Dedup.** Event copies are radix-partitioned by event key. Each partition keeps the copy with the smallest `ingest_ts`, and winners go to user partitions.
3. **Links.** Union-find over dense node IDs gives each component's byte-wise smallest ID.
4. **Route and sessions.** Users map to persons, and events fold into (person, 30-minute bucket) cells. Sorted cells form sessions, followed by credit, assists and daily totals.
5. **Write.** Each worker writes its own `user_daily` Parquet part; one worker writes `campaign_daily`.

Build with Rust 1.85 or newer: `RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+pclmulqdq" cargo build --release --locked`. Set `DATA_DIR`, `OUTPUT_DIR` and `NCPU` to run it directly; `SS_TIMING=1` prints phase timings and `SS_GENERIC=1` forces the generic pipeline.

The [local runner guide](starter-duckdb/README.md) covers the fixtures and the dev check.
