#!/usr/bin/env python3
"""Load the challenge's CSV dialect, then run the DuckDB transformation."""
import json
import os
from itertools import islice
from pathlib import Path

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


def load_rows(db, table, columns, rows):
    """Load bounded Arrow batches without materializing the dataset in Python."""
    definitions = ", ".join(f"{name} VARCHAR" for name in columns)
    db.execute(f"CREATE TEMP TABLE {table} ({definitions})")
    rows = iter(rows)
    while batch := list(islice(rows, BATCH_ROWS)):
        arrow_batch = pa.table(
            [pa.array(values, type=pa.string()) for values in zip(*batch)],
            names=list(columns),
        )
        db.register("input_batch", arrow_batch)
        try:
            db.execute(f"INSERT INTO {table} SELECT * FROM input_batch")
        finally:
            db.unregister("input_batch")


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


def main():
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
        load_rows(db, "raw_events", EVENT_COLUMNS,
                  (row for row in csv_rows((data / "events").glob("*.csv"), len(EVENT_COLUMNS))
                   if row[0] is not None))
        db.execute(Path(__file__).with_name("events.sql").read_text())

        load_rows(db, "raw_clicks", CLICK_COLUMNS,
                  (row for row in csv_rows((data / "clicks").glob("*.csv"), len(CLICK_COLUMNS))
                   if row[0] is not None))
        db.execute(Path(__file__).with_name("attribution.sql").read_text())

        for table in ("user_daily", "campaign_daily"):
            folder = output / table
            folder.mkdir(parents=True, exist_ok=True)
            staging = folder / ".part-0.parquet"
            db.execute(f"COPY {table} TO ? (FORMAT parquet)", [str(staging)])
            staging.replace(folder / "part-0.parquet")


if __name__ == "__main__":
    main()
