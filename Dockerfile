# --- STAGE 1: Build Stage ---
FROM ubuntu:24.04 AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    g++ \
    cmake \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy only the source code for building
COPY . .

# Compile the binary
# We use -o bin/minisql to place it in a specific folder
RUN mkdir -p bin && g++ -std=c++17 -O2 -pthread \
    $(find src -name "*.cpp") \
    -o bin/minisql \
    -I src \
    -I src/storage \
    -I src/index \
    -I src/parser \
    -I src/optimizer \
    -I src/optimiser \
    -I src/executor \
    -I src/utility

# --- STAGE 2: Runtime Stage ---
FROM ubuntu:24.04

# Set up the internal directory structure you wanted
WORKDIR /app
RUN mkdir -p bin data

# Copy only the compiled binary from the builder stage
COPY --from=builder /build/bin/minisql ./bin/minisql

# (Optional) Set permissions to ensure the binary is executable
RUN chmod +x ./bin/minisql

# Set the working directory to /app/bin so relative paths work
WORKDIR /app/bin

# Start the application
CMD ["./minisql"]