# Use the official Rust image
FROM rust:latest

# Set the working directory
WORKDIR /usr/src/app

# Clone the DatabaseEngine repository
RUN git clone https://github.com/ItsJustIkenna/DatabaseEngine.git

# Copy the DataEngine files
COPY . .

# Set the working directory to DataEngine
WORKDIR /usr/src/app/DataEngine

# Build the project
RUN cargo build --release

# Run the main application binary
CMD ["./target/release/program"]
