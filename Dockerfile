# Stage 1: Build the optimized Rust binary
FROM rust:latest AS builder

ARG APP_NAME=simple-maps-server
ENV APP_NAME=${APP_NAME}

WORKDIR /app

RUN apt-get update -y \
    && apt-get install -y --no-install-recommends musl-tools \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency manifests first
COPY Cargo.toml Cargo.lock ./

# Create a placeholder source file so Cargo can resolve and compile dependencies
RUN mkdir src \
    && printf 'fn main() {}\n' > src/main.rs \
    && rustup target add x86_64-unknown-linux-musl \
    && cargo build --release --target x86_64-unknown-linux-musl

# Copy the real source code
COPY src ./src

# Build the actual application
RUN touch src/main.rs \
    && cargo build --release --target x86_64-unknown-linux-musl \
    && strip target/x86_64-unknown-linux-musl/release/$APP_NAME

RUN file target/x86_64-unknown-linux-musl/release/$APP_NAME \
    && readelf -l target/x86_64-unknown-linux-musl/release/$APP_NAME

# Stage 2: Minimal runtime image
FROM alpine:latest

LABEL org.opencontainers.image.source=https://github.com/abstractumbra/simplemaps-server
LABEL org.opencontainers.image.description="Server for the SimpleMaps plugin by Caraxi"

ARG APP_NAME=simple-maps-server
ENV APP_NAME=${APP_NAME}
ENV RUST_LOG=info

WORKDIR /app
COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/$APP_NAME .

USER 1000:1000

EXPOSE 44333

CMD ["/app/simple-maps-server"]
