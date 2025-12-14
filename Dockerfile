FROM docker.io/library/rust:1.92.0-trixie AS builder

WORKDIR /src/brioche-registry

COPY Cargo.toml Cargo.lock ./
COPY src src
COPY migrations migrations
COPY .sqlx .sqlx
RUN cargo install --locked --path . --root /app

FROM docker.io/library/debian:trixie-slim

COPY --from=builder /app/bin/brioche-registry /usr/local/bin/brioche-registry

ENV DEBIAN_FRONTEND=O
RUN set -eux; \
    apt-get update; \
    apt-get install -y bash=5.2.37-2+b5 curl=8.14.1-2+deb13u2 fuse3=3.17.2-3 sqlite3=3.46.1-7 ca-certificates=20250419; \
    apt-get clean; \
    rm -rf /var/lib/apt/lists/*

CMD [ "/usr/local/bin/brioche-registry", "serve" ]
