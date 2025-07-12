FROM rust:1.88.0-bookworm AS builder

WORKDIR /src/brioche-registry

COPY Cargo.toml Cargo.lock ./
COPY src src
COPY migrations migrations
COPY .sqlx .sqlx
RUN cargo install --locked --path . --root /app

FROM debian:bookworm-slim

COPY --from=builder /app/bin/brioche-registry /usr/local/bin/brioche-registry

ENV DEBIAN_FRONTEND=O
RUN set -eux; \
    apt-get update; \
    apt-get install -y bash curl fuse3 sqlite3 ca-certificates; \
    apt-get clean; \
    rm -rf /var/lib/apt/lists/*

CMD [ "/usr/local/bin/brioche-registry", "serve" ]
