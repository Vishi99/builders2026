FROM rust:1.89.0-slim-bookworm AS build
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
RUN RUSTFLAGS="-C target-cpu=skylake -C target-feature=+pclmulqdq" cargo build --locked --release

FROM debian:bookworm-slim
COPY --from=build /build/target/release/shopping-sessions /usr/local/bin/shopping-sessions
# Large malloc blocks on transparent huge pages: far fewer page faults.
ENV GLIBC_TUNABLES=glibc.malloc.hugetlb=1
USER 1000:1000
CMD ["/usr/local/bin/shopping-sessions"]
