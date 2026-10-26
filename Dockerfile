FROM rust:1-bookworm AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /data

COPY --from=builder /app/target/release/messenger /usr/local/bin/messenger

ENV RUST_LOG=info
ENV DATABASE_URL=sqlite:///data/messages.db
VOLUME ["/data"]
EXPOSE 3000

CMD ["/usr/local/bin/messenger"]
