# Use the official Rust image
FROM rust:latest

# Set the working directory to DataEngine
WORKDIR /usr/src/app/DataEngine

# Copy the DataEngine files
COPY . .

# Copy the DatabaseEngine files from the checked-out path
COPY DatabaseEngine /usr/src/app/DataEngine/DatabaseEngine
# Build the project
RUN cargo build --release

# Run the main application binary
CMD ["./target/release/program"]
