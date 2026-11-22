ARG RUST_VERSION=1.89.0
FROM rust:${RUST_VERSION}-slim-bookworm AS build
ARG TARGETARCH
ARG NATIVE_AMD64_ARCH=x86-64-v3
ARG SHOPPING_FEATURES=
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
RUN set -eu; \
    if [ "$TARGETARCH" = amd64 ]; then export RUSTFLAGS="-C target-cpu=$NATIVE_AMD64_ARCH"; fi; \
    cargo build --locked --release --features "$SHOPPING_FEATURES"

FROM debian:bookworm-slim
ARG SHOPPING_EVENT_SCANNER=auto
ENV SHOPPING_EVENT_SCANNER=$SHOPPING_EVENT_SCANNER
COPY --from=build /build/target/release/shopping-sessions /usr/local/bin/shopping-sessions
USER 1000:1000
CMD ["/usr/local/bin/shopping-sessions"]
