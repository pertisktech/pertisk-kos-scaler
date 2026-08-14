# syntax=docker/dockerfile:1.7

FROM rust:1-alpine AS rust-builder

WORKDIR /app

RUN apk add --no-cache musl-dev openssl-dev openssl-libs-static pkgconf

COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs && cargo build --release && rm -rf src

COPY src ./src
RUN touch src/main.rs && cargo build --release

FROM alpine:3.20

RUN apk add --no-cache ca-certificates \
    && addgroup -S appgroup \
    && adduser -S appuser -G appgroup \
    && mkdir -p /var/lib/kos-scaler \
    && chown appuser:appgroup /var/lib/kos-scaler

COPY --from=rust-builder /app/target/release/pertisk-kos-scaler /usr/local/bin/kos-scaler

USER appuser
ENV RUST_LOG=info
ENV KOS_SCALER_STATE_DIR=/var/lib/kos-scaler
ENTRYPOINT ["/usr/local/bin/kos-scaler"]
