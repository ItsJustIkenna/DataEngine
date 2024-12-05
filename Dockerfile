# Use the official Rust image
FROM rust:alpine

# Install necessary build dependencies
RUN apk update && apk add --no-cache pkgconfig openssl-dev build-base

ENV PKG_CONFIG_PATH=/usr/lib/aarch64-linux-gnu/pkgconfig
ENV OPENSSL_DIR=/usr
ENV OPENSSL_LIB_DIR=$OPENSSL_DIR/lib
ENV OPENSSL_INCLUDE_DIR=$OPENSSL_DIR/include

# Set the working directory to DataEngine
WORKDIR /usr/src/app/DataEngine

# Copy the DataEngine files
COPY . .

# Copy the DatabaseEngine directory
COPY DatabaseEngine .

# Build the project
RUN cargo build

# Run tests
RUN cargo test

# Run the main application binary
RUN cargo run