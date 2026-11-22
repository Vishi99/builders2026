FROM rust:1.89.0-slim-bookworm AS build
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
RUN RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+pclmulqdq" cargo build --locked --release

FROM debian:bookworm-slim
COPY --from=build /build/target/release/shopping-sessions /usr/local/bin/shopping-sessions
USER 1000:1000
CMD ["/usr/local/bin/shopping-sessions"]
