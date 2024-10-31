# Use the official Rust image
FROM rust:latest

# Set the working directory
WORKDIR /usr/src/dataengine

# Copy the Cargo files separately for dependency caching
COPY Cargo.toml Cargo.lock ./
COPY hostbuilder/Cargo.toml hostbuilder/

# Fetch dependencies
RUN cargo fetch

# Copy only necessary files
COPY hostbuilder ./hostbuilder
COPY program ./program

# Build the main module
RUN cargo build --release --manifest-path=program/Cargo.toml

# Expose the port your service runs on
EXPOSE 12345

# Run the main application binary
CMD ["./target/release/program"]