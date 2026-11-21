#!/usr/bin/env python3
"""Load the challenge's CSV dialect, then run the DuckDB transformation."""
import json
import mmap
import os
import sys
from itertools import islice
from pathlib import Path
from time import perf_counter

import duckdb
import pyarrow as pa

BATCH_ROWS = 65_536
EVENT_COLUMNS = ("event_id", "user_id", "ts", "ingest_ts", "event_type",
                 "product_id", "price_cents")
CLICK_COLUMNS = ("click_id", "user_id", "campaign_id", "ts")


def csv_rows(paths, width):
    for path in paths:
        with path.open("rb") as source:
            next(source, None)  # Skip just the first line of each shard.
            for line in source:
                # Iteration on a binary stream splits only on LF. CR belongs to the field.
                fields = line.removesuffix(b"\n").split(b",")
                if len(fields) != width:
                    continue
                row = []
                for field in fields:
                    if b'"' in field:
                        if len(field) < 2 or field[:1] != b'"' or field[-1:] != b'"':
                            break
                        field = field[1:-1]
                        if b'"' in field:
                            break
                    row.append(field or None)
                else:
                    yield tuple(row)


def load_rows(db, table, columns, rows, *, append=False):
    """Load bounded Arrow batches without materializing the dataset in Python."""
    schema = pa.schema([(name, pa.string()) for name in columns])
    rows = iter(rows)

    def batches():
        while batch := list(islice(rows, BATCH_ROWS)):
            yield pa.RecordBatch.from_arrays(
                [pa.array(values, type=pa.string()) for values in zip(*batch)],
                schema=schema,
            )

    # DuckDB consumes one stream, avoiding an INSERT and registration per batch.
    with pa.RecordBatchReader.from_batches(schema, batches()) as reader:
        db.register("input_batch", reader)
        try:
            destination = f"INSERT INTO {table}" if append else f"CREATE TEMP TABLE {table} AS"
            db.execute(f"{destination} SELECT * FROM input_batch")
        finally:
            db.unregister("input_batch")


def load_logs(db, table, columns, paths):
    """Scan ordinary shards in DuckDB and retain exact parsing for exceptions."""
    native_paths, exact_paths = [], []
    for path in paths:
        filename = str(path)
        # DuckDB expands glob patterns even when a path is passed in a list.
        if any(char in filename for char in "*?[]"):
            exact_paths.append(path)
            continue
        with path.open("rb") as source:
            if os.fstat(source.fileno()).st_size == 0:
                continue
            with mmap.mmap(source.fileno(), 0, access=mmap.ACCESS_READ) as mapped:
                # The native scanner treats CR as a line ending and uses NUL
                # internally when the delimiter is empty.
                if mapped.find(b"\r") >= 0 or mapped.find(b"\x00") >= 0:
                    exact_paths.append(path)
                else:
                    native_paths.append(filename)

    if native_paths:
        # Scan one raw field per physical line. With the CSV quote state machine
        # disabled, an unclosed quote cannot consume the following line.
        # The regex checks the exact field count and whole-field quote syntax.
        field = r'(?:[^",]*|"[^",]*")'
        pattern = ",".join([field] * len(columns))
        projection = ", ".join(
            f"""nullif(CASE WHEN starts_with(fields[{index}], '"')
                            THEN fields[{index}][2:-2]
                            ELSE fields[{index}] END, '') AS {column}"""
            for index, column in enumerate(columns, 1)
        )
        try:
            db.execute(rf"""
                CREATE TEMP TABLE {table} AS
                SELECT {projection}
                FROM (
                    SELECT string_split(line, ',') AS fields
                    FROM read_csv(
                        ?, columns={{'line': 'VARCHAR'}}, auto_detect=false,
                        delim='', quote='', escape='', new_line='\n',
                        header=false, skip=1, hive_partitioning=false
                    )
                    WHERE regexp_full_match(line, ?)
                )
                WHERE fields[1] NOT IN ('', '""')
            """, [native_paths, pattern])
        except duckdb.InvalidInputException:
            # A valid raw line may exceed the native scanner's line limit.
            # The failed CREATE TABLE rolls back, so replay all native shards.
            exact_paths.extend(Path(path) for path in native_paths)
            native_paths.clear()

    if exact_paths or not native_paths:
        rows = (row for row in csv_rows(exact_paths, len(columns)) if row[0] is not None)
        load_rows(db, table, columns, rows, append=bool(native_paths))


def person_mapping(edges):
    # Union by size and compress paths. The internal root can be any ID.
    parent = {}
    size = {}
    minimum = {}

    def root(user):
        if user not in parent:
            parent[user] = user
            size[user] = 1
            minimum[user] = user
        while user != parent[user]:
            parent[user] = parent[parent[user]]
            user = parent[user]
        return user

    for left, right in edges:
        if left is None or right is None:
            continue
        a, b = root(left), root(right)
        if a == b:
            continue
        if size[a] < size[b]:
            a, b = b, a
        parent[b] = a
        size[a] += size.pop(b)
        # The keys are bytes, so this also handles non-ASCII IDs.
        minimum[a] = min(minimum[a], minimum.pop(b))

    for user in parent:
        person = minimum[root(user)]
        if user != person:
            yield user, person


def report_stage(name, started):
    # Keep timing in the scorer's stderr logs, including completed stages if a later one fails.
    finished = perf_counter()
    print(f"{name}: {finished - started:.3f}s", file=sys.stderr, flush=True)
    return finished


def main():
    started = perf_counter()
    data = Path(os.environ.get("DATA_DIR", "/data"))
    output = Path(os.environ.get("OUTPUT_DIR", "/output"))
    scratch = Path(os.environ.get("SCRATCH_DIR", "/scratch"))
    threads = int(os.environ.get("NCPU", os.cpu_count() or 1))
    memory_gb = int(os.environ.get("MEM_GB", "8"))
    manifest = json.loads((data / "manifest.json").read_text())

    scratch.mkdir(parents=True, exist_ok=True)
    with duckdb.connect() as db:
        db.execute("SET threads = ?", [threads])
        # Leave headroom for Python, Arrow batches, and DuckDB's unmanaged buffers.
        db.execute("SET memory_limit = ?", [f"{max(1, int(memory_gb * 0.8))}GB"])
        db.execute("SET temp_directory = ?", [str(scratch / "duckdb_tmp")])
        db.execute("SET preserve_insertion_order = false")
        db.execute("""
            CREATE TEMP TABLE dataset_window AS
            SELECT epoch_ms(strptime(?, '%Y-%m-%dT%H:%M:%SZ')) AS start_ms,
                   epoch_ms(strptime(?, '%Y-%m-%dT%H:%M:%SZ')) AS end_ms
        """, [manifest["window_start"], manifest["window_end"]])

        load_rows(db, "person_ids", ("user_id", "person"),
                  person_mapping(csv_rows((data / "links").glob("*.csv"), 2)))
        load_rows(db, "products", ("product_id", "category"),
                  csv_rows([data / "products.csv"], 2))
        load_rows(db, "campaigns", ("campaign_id", "channel"),
                  csv_rows([data / "campaigns.csv"], 2))
        stage = report_stage("Setup, links, and dimensions", started)
        load_logs(db, "raw_events", EVENT_COLUMNS, (data / "events").glob("*.csv"))
        stage = report_stage("Load events", stage)
        db.execute(Path(__file__).with_name("events.sql").read_text())
        stage = report_stage("Clean events and build sessions", stage)

        load_logs(db, "raw_clicks", CLICK_COLUMNS, (data / "clicks").glob("*.csv"))
        stage = report_stage("Load clicks", stage)
        db.execute(Path(__file__).with_name("attribution.sql").read_text())
        stage = report_stage("Clean clicks and attribute sessions", stage)

        for table in ("user_daily", "campaign_daily"):
            folder = output / table
            folder.mkdir(parents=True, exist_ok=True)
            staging = folder / ".part-0.parquet"
            db.execute(f"COPY {table} TO ? (FORMAT parquet)", [str(staging)])
            staging.replace(folder / "part-0.parquet")
        report_stage("Write Parquet", stage)
    report_stage("Total pipeline", started)


if __name__ == "__main__":
    main()
