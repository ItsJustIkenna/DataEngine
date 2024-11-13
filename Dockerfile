# Use the official Rust image
FROM rust:alpine

# Set the working directory to DataEngine
WORKDIR /usr/src/app/DataEngine

# Copy the DataEngine files
COPY . .

# Copy the databaseschema directory
COPY DatabaseEngine .
# Build the project
RUN cargo build --release

# Run the main application binary
CMD ["./target/release/program"]
