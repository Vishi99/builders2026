FROM rust:1.89.0-slim-bookworm AS build
ARG TARGETARCH
ARG NATIVE_AMD64_ARCH=x86-64-v3
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
RUN set -eu; \
    if [ "$TARGETARCH" = amd64 ]; then export RUSTFLAGS="-C target-cpu=$NATIVE_AMD64_ARCH"; fi; \
    cargo build --locked --release

FROM debian:bookworm-slim
COPY --from=build /build/target/release/shopping-sessions /usr/local/bin/shopping-sessions
ENV J2_BATCH_SCAN=1 J2_BATCH_RANK=avx512
USER 1000:1000
CMD ["/usr/local/bin/shopping-sessions"]
