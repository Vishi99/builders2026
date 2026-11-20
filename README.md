# Shopping Sessions

A DuckDB pipeline that turns shopping events, ad clicks, and linked user IDs into two daily Parquet tables. See the [challenge brief](problem.txt) and [rules](rules.md) for the scorer contract.

From the repository root, build the scorer image or run the bundled fixtures. The fixture checker requires Python with Polars.

```sh
make -C starter-duckdb build
make -C starter-duckdb fixtures
```

On Apple silicon, use `PLATFORM=linux/arm64 CPUS=4 MEM=8g` for local Docker runs. The scorer uses the default `linux/amd64` image. The [runner guide](starter-duckdb/README.md) covers native tests and the public dev dataset.
