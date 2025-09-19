FROM rust:1.90.0-bookworm AS builder

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
    apt-get install -y bash=5.2.15-2+b8 curl=7.88.1-10+deb12u12 fuse3=3.14.0-4 sqlite3=3.40.1-2+deb12u1 ca-certificates=20230311; \
    apt-get clean; \
    rm -rf /var/lib/apt/lists/*

CMD [ "/usr/local/bin/brioche-registry", "serve" ]
