"""Write the native pipeline's column buffers to the required Parquet tables."""
import ctypes as ct
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter

import pyarrow as pa
import pyarrow.parquet as pq


class Buffer(ct.Structure):
    _fields_ = [("address", ct.c_void_p), ("size", ct.c_int64)]

    def arrow(self, owner, *, optional=False):
        if self.size == 0:
            return None if optional else pa.py_buffer(b"")
        return pa.foreign_buffer(self.address, self.size, base=owner)


class Column(ct.Structure):
    _fields_ = [("validity", Buffer), ("offsets", Buffer), ("values", Buffer)]

    def arrow(self, kind, count, owner):
        buffers = [self.validity.arrow(owner, optional=True)]
        if pa.types.is_large_string(kind):
            buffers.append(self.offsets.arrow(owner))
        buffers.append(self.values.arrow(owner))
        return pa.Array.from_buffers(kind, count, buffers)


class Table(ct.Structure):
    _fields_ = [("rows", ct.c_int64), ("columns", Column * 7)]

    def arrow(self, schema, owner):
        arrays = [column.arrow(field.type, self.rows, owner)
                  for column, field in zip(self.columns, schema)]
        return pa.Table.from_arrays(arrays, schema=schema)


class Output(ct.Structure):
    _fields_ = [("users", Table), ("campaigns", Table)]


USER_SCHEMA = pa.schema([
    ("user_id", pa.large_string()), ("day", pa.date32()),
    ("sessions", pa.uint32()), ("events", pa.uint32()),
    ("revenue_cents", pa.int64()), ("top_category", pa.large_string()),
])
CAMPAIGN_SCHEMA = pa.schema([
    ("campaign_id", pa.large_string()), ("channel", pa.large_string()), ("day", pa.date32()),
    ("sessions", pa.int64()), ("purchasing_sessions", pa.int64()),
    ("revenue_cents", pa.int64()), ("assisted_sessions", pa.int64()),
])


@contextmanager
def native_result(library, data, manifest, threads):
    native = ct.CDLL(str(library))
    native.shopping_run.argtypes = [ct.c_char_p, ct.c_char_p, ct.c_char_p, ct.c_uint,
                                   ct.POINTER(Output), ct.POINTER(ct.c_char), ct.c_size_t]
    native.shopping_run.restype = ct.c_void_p
    native.shopping_free.argtypes = [ct.c_void_p]
    native.shopping_free.restype = None

    start, end = (manifest[key].encode("ascii") for key in ("window_start", "window_end"))
    output = Output()
    error = ct.create_string_buffer(1024)
    owner = native.shopping_run(os.fsencode(data), start, end, threads,
                                ct.byref(output), error, len(error))
    if owner is None:
        raise RuntimeError(error.value.decode(errors="replace"))
    try:
        yield output
    finally:
        native.shopping_free(owner)


def write_table(table, path, threads=1):
    path.mkdir(parents=True, exist_ok=True)
    parts = max(1, min(threads, table.num_rows // 262_144))
    length = (table.num_rows + parts - 1) // parts
    names = ["part-0.parquet", *(f"part-native-{i}.parquet" for i in range(1, parts))]
    with TemporaryDirectory(prefix=".parquet-", dir=path) as staging:
        staging = Path(staging)

        def write(index):
            pq.write_table(table.slice(index * length, length), staging / names[index],
                           compression="snappy", row_group_size=122_880)

        if parts == 1:
            write(0)
        else:
            with ThreadPoolExecutor(max_workers=parts) as pool:
                list(pool.map(write, range(parts)))

        for name in names:
            (staging / name).replace(path / name)

    for existing in path.glob("part-native-*.parquet"):
        suffix = existing.stem.removeprefix("part-native-")
        if suffix.isdecimal() and int(suffix) >= parts:
            existing.unlink()


def main(library):
    started = perf_counter()
    data = Path(os.environ.get("DATA_DIR", "/data"))
    destination = Path(os.environ.get("OUTPUT_DIR", "/output"))
    threads = int(os.environ.get("NCPU", os.cpu_count() or 1))
    if threads < 1:
        raise ValueError("NCPU must be positive")
    manifest = json.loads((data / "manifest.json").read_text())

    # Each synchronous write finishes before the native buffers are freed.
    with native_result(library, data, manifest, threads) as output:
        write_table(output.users.arrow(USER_SCHEMA, output), destination / "user_daily", threads)
        write_table(output.campaigns.arrow(CAMPAIGN_SCHEMA, output), destination / "campaign_daily")

    print(f"Total native pipeline: {perf_counter() - started:.3f}s", file=sys.stderr)


if __name__ == "__main__":
    main(Path(__file__).with_name("shopping_native.so"))
