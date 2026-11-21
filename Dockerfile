# Shopping Sessions: native processing and Parquet output.
# The scorers are linux/amd64 (Intel Xeon). They build this file natively, so
# no FROM line pins a platform: `make build` builds for linux/amd64 anywhere.
FROM gcc:14-bookworm AS native-build
ARG TARGETARCH
ARG NATIVE_AMD64_ARCH=x86-64-v4
WORKDIR /src
COPY starter-duckdb/pipeline/*.cpp starter-duckdb/pipeline/*.hpp ./
RUN set -eu; \
    set --; \
    if [ "$TARGETARCH" = amd64 ]; then set -- "-march=$NATIVE_AMD64_ARCH"; fi; \
    g++ -std=c++20 -O3 -DNDEBUG -flto -fPIC -shared -pthread "$@" \
        -static-libstdc++ -static-libgcc native_pipeline.cpp -o /shopping_native.so

FROM python:3.12-slim-bookworm

# The scorer runs the container as uid 1000; give that uid a name and a home.
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app

COPY starter-duckdb/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY starter-duckdb/pipeline/ /app/pipeline/
COPY --from=native-build /shopping_native.so /app/pipeline/shopping_native.so
COPY starter-duckdb/run.sh /app/run.sh
RUN chmod +x /app/run.sh

# At run time the root filesystem is read-only, the user is 1000:1000, there is
# no network, /data is read-only, and /output and /scratch are the only writable
# places. run.sh points every temp and cache directory at /scratch.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
CMD ["/app/run.sh"]
