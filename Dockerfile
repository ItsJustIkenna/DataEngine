# Use the official Rust image
FROM rust:latest

# Set the working directory
WORKDIR /usr/src/app

# Copy the Cargo files separately for dependency caching
COPY Cargo.toml Cargo.lock ./

# Fetch dependencies
RUN cargo fetch

# Copy the rest of the project files
COPY . .

# Build the main module
RUN cargo build --release --manifest-path=program/Cargo.toml

# Expose the port your service runs on
EXPOSE 12345

# Run the main application binary
CMD ["./target/release/program"]
