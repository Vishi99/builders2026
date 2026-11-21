# Shopping Sessions: native processing and Parquet output.
# The scorers are linux/amd64 (Intel Xeon). They build this file natively, so
# no FROM line pins a platform: `make build` builds for linux/amd64 anywhere.
FROM gcc:14-bookworm AS native-build
ARG TARGETARCH
ARG NATIVE_AMD64_ARCH=x86-64-v4
# Set to 1 to reuse delimiter masks across event rows in the AVX-512 scanner.
ARG NATIVE_ROLLING_AVX512=0
WORKDIR /src
RUN apt-get update && apt-get install -y --no-install-recommends liblz4-dev nlohmann-json3-dev \
    && rm -rf /var/lib/apt/lists/*
COPY starter-duckdb/pipeline/*.cpp starter-duckdb/pipeline/*.hpp ./
RUN set -eu; \
    set --; \
    if [ "$TARGETARCH" = amd64 ]; then set -- "-march=$NATIVE_AMD64_ARCH"; fi; \
    if [ "$NATIVE_ROLLING_AVX512" = 1 ]; then set -- "$@" -DSHOPPING_ROLLING_AVX512; fi; \
    g++ -std=c++20 -O3 -DNDEBUG -flto -pthread "$@" \
        -static-libstdc++ -static-libgcc native_pipeline.cpp native_main.cpp \
        -Wl,-Bstatic -llz4 -Wl,-Bdynamic -o /shopping-sessions

FROM debian:bookworm-slim
COPY --from=native-build /shopping-sessions /usr/local/bin/shopping-sessions
# The scorer mounts /data read-only and supplies a writable /output.
USER 1000:1000
CMD ["/usr/local/bin/shopping-sessions"]
