FROM rust:1.77.2-bookworm AS builder

WORKDIR /src/brioche-registry

COPY Cargo.toml Cargo.lock ./
COPY src src
COPY migrations migrations
COPY .sqlx .sqlx
RUN mkdir -p /app/bin
RUN cargo install --locked --path . --root /app

FROM debian:bookworm-slim

COPY --from=flyio/litefs:0.5 /usr/local/bin/litefs /usr/local/bin/litefs
COPY --from=builder /app/bin/brioche-registry /usr/local/bin/brioche-registry

COPY docker/entrypoint.sh /entrypoint.sh
COPY docker/etc/litefs /etc/litefs

RUN apt-get update && \
    apt-get install -y bash curl fuse3 sqlite3 ca-certificates && \
    rm -rf /var/lib/apt/lists/*

ENTRYPOINT /entrypoint.sh
