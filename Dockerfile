FROM docker.io/library/rust:1.100.0-slim-trixie AS builder

RUN set -eux; \
    apt-get update; \
    DEBIAN_FRONTEND=noninteractive apt-get install -y curl=8.14.1-2+deb13u7 ca-certificates=20250419+deb13u1; \
    apt-get clean; \
    rm -rf /var/lib/apt/lists/*

WORKDIR /src/brioche-registry

COPY Cargo.toml Cargo.lock ./
COPY src src
COPY migrations migrations
COPY .sqlx .sqlx
RUN cargo install --locked --path . --root /app

FROM docker.io/library/debian:13.8-slim

COPY --from=builder /app/bin/brioche-registry /usr/local/bin/brioche-registry

RUN set -eux; \
    apt-get update; \
    DEBIAN_FRONTEND=noninteractive apt-get install -y bash=5.2.37-2+b10 curl=8.14.1-2+deb13u7 fuse3=3.17.2-3 sqlite3=3.46.1-7+deb13u2 ca-certificates=20250419+deb13u1; \
    apt-get clean; \
    rm -rf /var/lib/apt/lists/*

CMD [ "/usr/local/bin/brioche-registry", "serve" ]
